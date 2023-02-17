#![allow(dead_code)]
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;
#[allow(unused_imports, dead_code)]
use tracing::{debug, error, info, trace, warn};
use wasmer_bus::abi::BusError;
use wasmer_bus::abi::SerializationFormat;
use wasmer_bus_process::api;
use wasmer_bus_process::prelude::*;
use wasmer_vbus::BusDataFormat;
use wasmer_vbus::BusInvocationEvent;
use wasmer_vbus::{BusError as VirtualBusError};
use wasmer_vbus::VirtualBusInvocation;
use wasmer_vbus::VirtualBusInvokable;
use wasmer_vbus::VirtualBusScope;

use crate::api::AsyncResult;
use crate::api::System;
use crate::eval::EvalContext;
use crate::eval::EvalResult;
use crate::eval::Process;
use crate::eval::RuntimeCallOutsideTask;
use crate::eval::WasiRuntime;
use crate::fd::FdMsg;

use super::*;

#[derive(Clone)]
pub struct SubProcessMultiplexer {
    processes: Arc<Mutex<HashMap<String, Weak<SubProcess>>>>,
}

impl SubProcessMultiplexer {
    pub fn new() -> SubProcessMultiplexer {
        SubProcessMultiplexer {
            processes: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

pub struct SubProcessFactoryInner {
    process_factory: ProcessExecFactory,
    multiplexer: SubProcessMultiplexer,
}

#[derive(Clone)]
pub struct SubProcessFactory {
    inner: Arc<SubProcessFactoryInner>,
    ctx: Arc<Mutex<Option<EvalContext>>>,
}

impl SubProcessFactory {
    pub fn new(process_factory: ProcessExecFactory, multiplexer: SubProcessMultiplexer) -> SubProcessFactory {
        SubProcessFactory {
            ctx: process_factory.ctx.clone(),
            inner: Arc::new(SubProcessFactoryInner {
                process_factory,
                multiplexer,
            }),
        }
    }
    pub async fn get_or_create(
        &self,
        wapm: &str,
        env: &LaunchEnvironment,
        stdout_mode: StdioMode,
        stderr_mode: StdioMode,
    ) -> Result<Arc<SubProcess>, BusError> {
        let wapm = wapm.to_string();
        let key = format!("{}-{}-{}", wapm, stdout_mode, stderr_mode);

        // Check for any existing process of this name thats already running
        {
            let processes = self.inner.multiplexer.processes.lock().unwrap();
            if let Some(process) = processes
                .get(&key)
                .iter()
                .filter_map(|a| a.upgrade())
                .next()
            {
                return Ok(process);
            }
        }

        // None was found so go ahead and start a new process
        let spawn = api::PoolSpawnRequest {
            spawn: api::Spawn {
                path: wapm.clone(),
                args: vec![wapm.to_string(), "bus".to_string()],
                chroot: false,
                working_dir: None,
                stdin_mode: StdioMode::Null,
                stdout_mode: stdout_mode,
                stderr_mode: stderr_mode,
                pre_open: Vec::new(),
            },
        };
        let (process, finish, runtime, checkpoint2) = self
            .inner
            .process_factory
            .create(spawn, &env)
            .await?;

        // Add it to the list of sub processes and return it
        let ctx = self.ctx.clone();
        let process = Arc::new(SubProcess::new(
            wapm.as_str(),
            process,
            finish,
            checkpoint2,
            runtime,
            ctx,
        ));
        {
            let mut processes = self.inner.multiplexer.processes.lock().unwrap();
            processes.insert(key, Arc::downgrade(&process));
        }
        Ok(process)
    }
}

pub struct SubProcessInner {
    pub wapm: String,
}

pub struct SubProcess {
    pub system: System,
    pub process: Process,
    pub finish: Arc<Mutex<AsyncResult<(EvalContext, u32)>>>,
    pub checkpoint2: Arc<WasmCheckpoint>,
    pub inner: Arc<SubProcessInner>,
    pub runtime: Arc<WasiRuntime>,
    pub ctx: Arc<Mutex<Option<EvalContext>>>,
}

impl SubProcess {
    pub fn new(
        wapm: &str,
        process: Process,
        finish: AsyncResult<(EvalContext, u32)>,
        checkpoint2: Arc<WasmCheckpoint>,
        runtime: Arc<WasiRuntime>,
        ctx: Arc<Mutex<Option<EvalContext>>>,
    ) -> SubProcess {
        SubProcess {
            system: System::default(),
            process,
            finish: Arc::new(Mutex::new(finish)),
            checkpoint2,
            inner: Arc::new(SubProcessInner {
                wapm: wapm.to_string(),
            }),
            runtime,
            ctx,
        }
    }

    pub fn create(
        self: &Arc<Self>,
        topic: String,
        format: BusDataFormat,
        request: Vec<u8>,
        ctx: WasmCallerContext,
    ) -> Result<(Box<dyn Processable>, Option<Box<dyn Session>>), BusError> {
        let feeder = self.runtime.feeder();
        let handle = feeder.call_raw(topic, format, request);
        let sub_process = self.clone();

        let session = SubProcessSession::new(
            self.runtime.clone(),
            handle.clone_task(),
            sub_process,
            ctx
        );
        Ok((Box::new(handle), Some(Box::new(session))))
    }
}

pub struct SubProcessSession {
    pub runtime: Arc<WasiRuntime>,
    pub task: RuntimeCallOutsideTask,
    pub sub_process: Arc<SubProcess>,
    pub ctx: WasmCallerContext,
}

impl SubProcessSession {
    pub fn new(
        runtime: Arc<WasiRuntime>,
        task: RuntimeCallOutsideTask,
        sub_process: Arc<SubProcess>,
        ctx: WasmCallerContext,
    ) -> SubProcessSession {
        SubProcessSession {
            runtime,
            task,
            sub_process,
            ctx,
        }
    }
}

impl Session for SubProcessSession {
    fn call(&mut self, topic: String, format: BusDataFormat, request: &[u8]) -> Result<(Box<dyn Processable + 'static>, Option<Box<dyn Session + 'static>>), BusError> {
        let invoker =
            self.task
                .call_raw(topic, format, request);
        Ok((Box::new(invoker), None))
    }
}

pub fn process_spawn(
    factory: ProcessExecFactory,
    request: api::PoolSpawnRequest,
) -> Box<dyn VirtualBusInvocation + Sync> {
    let mut cmd = request.spawn.path.clone();
    for arg in request.spawn.args.iter() {
        cmd.push_str(" ");
        cmd.push_str(arg.as_str());
    }
    let dst = Arc::clone(&factory.ctx);
    let env = factory.launch_env();
    let result = factory
        .launch_ext(request, &env, None, None, None, true,
            move |ctx: LaunchContext| {
                let mut eval_rx = crate::eval::eval(cmd, ctx.eval);
                Box::pin(async move {
                    Ok(eval_rx.recv().await)
                })
            }
        );
    
    Box::new(SubProcessHandler {
        dst,
        result: Mutex::new(result)
    })
}

#[derive(Debug)]
pub struct SubProcessHandler {
    
    dst: Arc<Mutex<Option<EvalContext>>>,
    result: Mutex<LaunchResult<Option<EvalResult>>>
}

impl VirtualBusInvocation
for SubProcessHandler
{
    fn poll_event(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BusInvocationEvent> {
        let mut result = self.result.lock().unwrap();
        if let Some(stdout) = &mut result.stdout {
            let mut stdout = Pin::new(stdout);
            while let Poll::Ready(msg) = stdout.poll_recv(cx) {
                if let Some(FdMsg::Data { data, flag }) = msg {
                    if flag.is_stdin() {
                        let data = api::PoolSpawnStdoutCallback(data);
                        return Poll::Ready(BusInvocationEvent::Callback {
                            topic: type_name_hash::<api::PoolSpawnStdoutCallback>().to_string(),
                            format: BusDataFormat::Bincode,
                            data: match SerializationFormat::Bincode.serialize(data) {
                                Ok(d) => d,
                                Err(err) => {
                                    // return Poll::Ready(BusInvocationEvent::Fault { fault: conv_error_back(err) });
                                    return Poll::Ready(conv_fault_to_callback(conv_error_back(err)));
                                }
                            }
                        });
                    }
                } else {
                    break;
                }
            }
        }
        if let Some(stderr) = &mut result.stderr {
            let mut stderr = Pin::new(stderr);
            while let Poll::Ready(msg) = stderr.poll_recv(cx) {
                if let Some(FdMsg::Data { data, flag }) = msg {
                    if flag.is_stderr() {
                        let data = api::PoolSpawnStderrCallback(data);
                        return Poll::Ready(BusInvocationEvent::Callback {
                            topic: type_name_hash::<api::PoolSpawnStderrCallback>().to_string(),
                            format: BusDataFormat::Bincode,
                            data: match SerializationFormat::Bincode.serialize(data) {
                                Ok(d) => d,
                                Err(err) => {
                                    // return Poll::Ready(BusInvocationEvent::Fault { fault: conv_error_back(err) });
                                    return Poll::Ready(conv_fault_to_callback(conv_error_back(err)));
                                }
                            }
                        });
                    }
                } else {
                    break;
                }
            }
        }
        let finish = Pin::new(&mut result.finish);
        if let Poll::Ready(finish) = finish.poll(cx) {
            let code = match finish {
                Some(Ok(Some(result))) => {
                    let code = result.raw();
                    let mut guard = self.dst.lock().unwrap();
                    if let Some(dst) = guard.as_mut() {
                        dst.env = result.ctx.env;
                        dst.root = result.ctx.root;
                        dst.working_dir = result.ctx.working_dir;
                    }
                    code
                },
                Some(Ok(None)) => {
                    // return Poll::Ready(BusInvocationEvent::Fault { fault: BusError::Aborted });    
                    return Poll::Ready(conv_fault_to_callback(VirtualBusError::Aborted));
                }
                Some(Err(err)) => {
                    err
                },
                None => {
                    // return Poll::Ready(BusInvocationEvent::Fault { fault: BusError::Aborted }); 
                    return Poll::Ready(conv_fault_to_callback(VirtualBusError::Aborted)); 
                }
            };
            let data = api::PoolSpawnExitCallback(code as i32);
            return Poll::Ready(BusInvocationEvent::Callback {
                topic: type_name_hash::<api::PoolSpawnExitCallback>().to_string(),
                format: BusDataFormat::Bincode,
                data: match SerializationFormat::Bincode.serialize(data) {
                    Ok(d) => d,
                    Err(err) => {
                        // return Poll::Ready(BusInvocationEvent::Fault { fault: conv_error_back(err) });
                        return Poll::Ready(conv_fault_to_callback(conv_error_back(err)));
                    }
                }
            });
        }
        Poll::Pending
    }
}

impl VirtualBusScope
for SubProcessHandler {
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl VirtualBusInvokable
for SubProcessHandler
{
    fn invoke(
        &self,
        topic: String,
        format: BusDataFormat,
        buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>>
    {
        let mut result = self.result.lock().unwrap();
        if topic == type_name_hash::<api::ProcessStdinRequest>().to_string() {
            let data = match decode_request::<api::ProcessStdinRequest>(
                format,
                buf,
            ) {
                Ok(a) => a.data,
                Err(err) => {
                    return Err(conv_error_back(err));
                }
            };
            let data_len = data.len();
            
            if let Some(stdin) = &result.stdin {
                match wasmer_bus::task::block_on(stdin.send(FdMsg::Data { data, flag: crate::fd::FdFlag::Stdin(false) })) {
                    Ok(_) => {
                        encode_instant_response(BusDataFormat::Bincode, &data_len)
                    }
                    Err(err) => {
                        debug!("failed to send data to stdin of process - {}", err);
                        Err(wasmer_vbus::BusError::InternalError)
                    }
                }                
            } else {
                Err(wasmer_vbus::BusError::BadHandle)
            }
        } else if topic == type_name_hash::<api::ProcessCloseStdinRequest>().to_string() {
            result.stdin.take();
            encode_instant_response(BusDataFormat::Bincode, &())
        } else if topic == type_name_hash::<api::ProcessFlushRequest>().to_string() {
            encode_instant_response(BusDataFormat::Bincode, &())
        } else if topic == type_name_hash::<api::ProcessIdRequest>().to_string() {
            let id = 0u32;
            encode_instant_response(BusDataFormat::Bincode, &id)
        } else {
            debug!("websocket invalid topic (hash={})", topic);
            Err(wasmer_vbus::BusError::InvalidTopic)
        }
    }
}
