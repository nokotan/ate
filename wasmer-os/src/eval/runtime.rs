use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use bytes::Bytes;
use derivative::Derivative;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use wasmer::{Module, Store};
use wasmer::vm::VMMemory;
use wasmer_bus::abi::SerializationFormat;
use wasmer_bus_process::api::Spawn;
use wasmer_wasi::{
    WasiRuntimeImplementation,
    PluggableRuntimeImplementation,
    UnsupportedVirtualBus,
    UnsupportedVirtualNetworking,
    WasiError,
    WasiThreadId,
    WasiThreadError, WasiBusProcessId,
};
use wasmer_vnet::VirtualNetworking;
use wasmer_vbus::{VirtualBus, BusError, SpawnOptions, VirtualBusListener, BusCallEvent, VirtualBusSpawner, SpawnOptionsConfig, BusSpawnedProcess, VirtualBusProcess, VirtualBusScope, VirtualBusInvokable, BusDataFormat, VirtualBusInvocation, FileDescriptor, BusInvocationEvent};

use crate::api::{System, AsyncResult};
use crate::api::abi::{SystemAbiExt, SpawnType};
use crate::bus::{ProcessExecFactory, WasmCallerContext, EvalContextTaker, ProcessExecInvokable, LaunchContext, LaunchEnvironment, StandardBus, LaunchResult, WasmCheckpoint};
use crate::common::MAX_MPSC;
use crate::err;
use crate::fd::{Fd, WeakFd};
use crate::pipe::ReceiverMode;

use super::{EvalContext, RuntimeBusListener, RuntimeBusFeeder, EvalResult, EvalStatus, exec_process};

#[derive(Debug, Clone)]
pub struct WasiRuntime
{
    pluggable: Arc<PluggableRuntimeImplementation>,
    forced_exit: Arc<AtomicU32>,
    process_factory: ProcessExecFactory,
    ctx: WasmCallerContext,
    feeder: RuntimeBusFeeder,
    listener: RuntimeBusListener
}

impl WasiRuntime
{
    pub fn new(
        forced_exit: &Arc<AtomicU32>,
        process_factory: ProcessExecFactory,
        ctx: WasmCallerContext
    ) -> Self {
        let (tx, rx) = mpsc::channel(MAX_MPSC);
        let pluggable = PluggableRuntimeImplementation::default();
        Self {
            pluggable: Arc::new(pluggable),
            forced_exit: forced_exit.clone(),
            process_factory,
            ctx,
            feeder: RuntimeBusFeeder {
                system: Default::default(),
                listener: tx,
            },
            listener: RuntimeBusListener {
                rx: Arc::new(Mutex::new(rx)),
            }
        }
    }
}

impl WasiRuntime
{
    pub fn take_context(&self) -> Option<EvalContext> {
        self.process_factory.take_context()
    }

    pub fn prepare_take_context(&self) -> EvalContextTaker {
        EvalContextTaker::new(&self.process_factory)
    }

    pub fn to_take_context(self: Arc<Self>) -> EvalContextTaker {
        EvalContextTaker::new(&self.process_factory)
    }

    pub fn feeder(&self) -> RuntimeBusFeeder {
        self.feeder.clone()
    }
}

impl WasiRuntimeImplementation
for WasiRuntime
{
    fn bus<'a>(&'a self) -> &'a (dyn VirtualBus) {
        self
    }
    
    fn networking<'a>(&'a self) -> &'a (dyn VirtualNetworking) {
        self.pluggable.networking.deref()
    }
    
    fn thread_generate_id(&self) -> WasiThreadId {
        self.pluggable.thread_id_seed.fetch_add(1, Ordering::Relaxed).into()
    }

    fn thread_spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), WasiThreadError> {
        let system = System::default();
        
        system.task_wasm(Box::new(move |_, _, _| {
                task();
                Box::pin(async move { })
            }),
            Store::default(),
            None,
            SpawnType::Create)
    }

    #[cfg(not(target_family = "wasm"))]
    fn thread_parallelism(&self) -> Result<usize, WasiThreadError> {
        if let Ok(cnt) = std::thread::available_parallelism() {
            Ok(usize::from(cnt))
        } else {
            Err(WasiThreadError::Unsupported)
        }
    }
    
    #[cfg(target_family = "wasm")]
    fn thread_parallelism(&self) -> Result<usize, WasiThreadError> {
        return Ok(0)
    }
    
    fn yield_now(&self, _id: WasiThreadId) -> Result<(), WasiError> {
        let forced_exit = self.forced_exit.load(Ordering::Acquire);
        if forced_exit != 0 {
            return Err(WasiError::Exit(forced_exit));
        }
        std::thread::yield_now();
        Ok(())
    }
}

impl VirtualBus
for WasiRuntime
{
    fn new_spawn(&self) -> SpawnOptions {
        let spawner = RuntimeProcessSpawner {
            process_factory: self.process_factory.clone(),
        };
        SpawnOptions::new(Box::new(spawner))
    }

    fn listen(&self) -> Result<Box<dyn VirtualBusListener + Sync>, BusError> {
        Ok(Box::new(self.listener.clone()))
    }
}

pub(crate) struct RuntimeProcessSpawner
{
    pub(crate) process_factory: ProcessExecFactory,
}

struct RuntimeProcessSpawned
{
    pub result: LaunchResult<EvalResult>,
    pub runtime: mpsc::Receiver<Arc<WasiRuntime>>,
}

impl RuntimeProcessSpawner
{
    pub fn spawn(&mut self, name: &str, config: &SpawnOptionsConfig) -> Result<LaunchResult<EvalResult>, BusError>
    {
        let spawned = self.spawn_internal(name, config)?;
        Ok(spawned.result)
    }
    
    fn spawn_internal(&mut self, name: &str, config: &SpawnOptionsConfig) -> Result<RuntimeProcessSpawned, BusError>
    {
        let conv_stdio_mode = |mode| {
            use wasmer_vfs::StdioMode as S1;
            use wasmer_bus_process::prelude::StdioMode as S2;
            match mode {
                S1::Inherit => S2::Inherit,
                S1::Log => S2::Log,
                S1::Null => S2::Null,
                S1::Piped => S2::Piped,
            }
        };

        let request = wasmer_bus_process::api::PoolSpawnRequest {
            spawn: Spawn {
                path: name.to_string(),
                args: config.args().clone(),
                chroot: config.chroot(),
                working_dir: Some(config.working_dir().to_string()),
                stdin_mode: conv_stdio_mode(config.stdin_mode()),
                stdout_mode: conv_stdio_mode(config.stdout_mode()),
                stderr_mode: conv_stdio_mode(config.stderr_mode()),
                pre_open: config.preopen().clone(),
            }
        };

        let (runtime_tx, runtime_rx) = mpsc::channel(1);

        let env = self.process_factory.launch_env();
        let result = self
            .process_factory
            .launch_ext(request, &env, None, None, None, true,
            move |ctx: LaunchContext| {
                let runtime_tx = runtime_tx.clone();
                Box::pin(async move {
                    let stdio = ctx.eval.stdio.clone();
                    let env = ctx.eval.env.clone().into_exported();

                    let mut show_result = false;
                    let redirect = Vec::new();

                    let (process, eval_rx, runtime, checkpoint2) = exec_process(
                        ctx.eval,
                        &ctx.path,
                        &ctx.args,
                        &env,
                        &mut show_result,
                        stdio,
                        &redirect,
                    )
                    .await?;

                    let _ = runtime_tx.send(runtime).await;

                    eval_rx
                        .await
                        .ok_or(err::ERR_ENOEXEC)
                        .map(|(ctx, ret)|
                            EvalResult {
                                ctx,
                                status: EvalStatus::Executed {
                                    code: ret,
                                    show_result: false,
                                }
                            }
                        )
                })
            });

        Ok(
            RuntimeProcessSpawned {
                result,
                runtime: runtime_rx,
            }
        )
    }
} 

impl VirtualBusSpawner
for RuntimeProcessSpawner
{
    fn spawn(&mut self, name: &str, config: &SpawnOptionsConfig) -> Result<BusSpawnedProcess, BusError>
    {
        if name == "os" {
            let env = self.process_factory.launch_env();
            return Ok(BusSpawnedProcess {
                inst: Box::new(
                    StandardBus::new(self.process_factory.clone())
                )
            });
        }

        let spawned = RuntimeProcessSpawner::spawn_internal(self, name, config)?;

        let process = RuntimeSpawnedProcess {
            exit_code: None,
            finish: spawned.result.finish,
            checkpoint2: spawned.result.checkpoint2,
            runtime: Arc::new(
                DelayedRuntime {
                    rx: Mutex::new(spawned.runtime),
                    val: RwLock::new(None)
                }
            )
        };

        Ok(
            BusSpawnedProcess {
                inst: Box::new(process)
            }
        )
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct DelayedRuntime
{
    #[derivative(Debug = "ignore")]
    rx: Mutex<mpsc::Receiver<Arc<WasiRuntime>>>,
    #[derivative(Debug = "ignore")]
    val: RwLock<Option<Result<Arc<WasiRuntime>, BusError>>>,
}

impl DelayedRuntime
{
    fn poll_runtime(&self, cx: &mut Context<'_>) -> Poll<Result<Arc<WasiRuntime>, BusError>>
    {
        // Fast path
        {
            let guard = self.val.read().unwrap();
            if let Some(runtime) = guard.deref() {
                return Poll::Ready(runtime.clone());
            }
        }

        // Enter a write lock on the runtime (and check again as it might have changed)
        let mut guard = self.val.write().unwrap();
        if let Some(runtime) = guard.deref() {
            return Poll::Ready(runtime.clone());
        }

        // Slow path (wait for the runtime to be returned by the sub process after it starts
        let mut runtime_rx = self.rx.lock().unwrap();
        match runtime_rx.poll_recv(cx) {
            Poll::Ready(runtime) => {
                match runtime {
                    Some(runtime) => {
                        guard.replace(Ok(runtime.clone()));
                        Poll::Ready(Ok(runtime))
                    },
                    None => {
                        guard.replace(Err(BusError::Aborted));
                        Poll::Ready(Err(BusError::Aborted))
                    }
                }
            },
            Poll::Pending => Poll::Pending
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct RuntimeSpawnedProcess
{
    exit_code: Option<u32>,
    #[derivative(Debug = "ignore")]
    finish: AsyncResult<Result<EvalResult, u32>>,
    checkpoint2: Arc<WasmCheckpoint>,
    runtime: Arc<DelayedRuntime>,
}

impl VirtualBusProcess
for RuntimeSpawnedProcess
{
    fn exit_code(&self) -> Option<u32>
    {
        self.exit_code.clone()
    }

    fn stdin_fd(&self) -> Option<FileDescriptor> {
        None
    }

    fn stdout_fd(&self) -> Option<FileDescriptor> {
        None
    }

    fn stderr_fd(&self) -> Option<FileDescriptor> {
        None
    }
}

impl VirtualBusScope
for RuntimeSpawnedProcess
{
    fn poll_finished(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>
    {
        if self.exit_code.is_some() {
            return Poll::Ready(())
        }
        match self.finish.rx.poll_recv(cx) {
            Poll::Ready(Some(eval)) => {
                let code = eval
                    .map(|a| {
                        match a.status {
                            EvalStatus::Executed { code, .. } => code,
                            _ => err::ERR_ENOEXEC
                        }
                    })
                    .unwrap_or_else(|err| err);
                self.exit_code.replace(code);
                Poll::Ready(())
            },
            Poll::Ready(None) => Poll::Ready(()),
            Poll::Pending => Poll::Pending
        }
    }
}

impl VirtualBusInvokable
for RuntimeSpawnedProcess
{
    fn invoke(
        &self,
        topic: String,
        format: BusDataFormat,
        buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>>
    {
        Ok(Box::new(
            DelayedInvocation {
                topic,
                format,
                buf: Some(buf.to_vec()),
                runtime: self.runtime.clone(),
                feeder: None
            }
        ))
    }
}

#[derive(Debug)]
struct DelayedInvocation
{
    topic: String,
    format: BusDataFormat,
    buf: Option<Vec<u8>>,
    runtime: Arc<DelayedRuntime>,
    feeder: Option<Pin<Box<dyn VirtualBusInvocation>>>,
}

impl VirtualBusInvokable
for DelayedInvocation
{
    fn invoke(
        &self,
        _topic: String,
        _format: BusDataFormat,
        _buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>>
    {
        Err(BusError::AlreadyConsumed)
    }
}

impl VirtualBusScope
for DelayedInvocation {
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl VirtualBusInvocation
for DelayedInvocation
{
    fn poll_event(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BusInvocationEvent> {
        match self.feeder {
            Some(_) => {},
            None => {
                let runtime = match self.runtime.poll_runtime(cx) {
                    Poll::Ready(Ok(runtime)) => runtime,
                    Poll::Ready(Err(err)) => { return Poll::Pending; },
                    Poll::Pending => { return Poll::Pending; }
                };
        
                let buf = match self.buf.take() {
                    Some(a) => a,
                    None => {
                        return Poll::Pending;
                    }
                };
        
                let mut feeder = Box::pin(runtime.feeder().call_raw(self.topic.clone(), self.format, buf));
               
                self.feeder = Some(feeder);
            }
        };

        match self.feeder {
            Some(ref mut feed) => feed.as_mut().poll_event(cx),
            None => Poll::Pending
        }
    }
}
