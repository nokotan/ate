use async_trait::async_trait;
#[cfg(feature = "embedded_files")]
use include_dir::{include_dir, Dir};
use wasmer_os::wasmer::{Module, Store};
use wasmer_os::wasmer::vm::{VMMemory, VMSharedMemory};
use wasmer_os::wasmer_wasi::WasiThreadError;
use std::cell::RefCell;
use std::convert::TryFrom;
use std::future::Future;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use wasmer_os::api::abi::*;
use wasmer_os::api::AsyncResult;
use wasmer_os::api::SerializationFormat;
use wasmer_os::api::ThreadLocal;
use wasmer_os::api::WebSocketAbi;
use wasmer_os::api::WebGlAbi;
use wasmer_os::err;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::watch;
#[allow(unused_imports, dead_code)]
use tracing::{debug, error, info, trace, warn};

use crate::ws::SysWebSocket;

#[cfg(feature = "embedded_files")]
static PUBLIC_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/public");

thread_local!(static THREAD_LOCAL: Rc<RefCell<ThreadLocal>> = Rc::new(RefCell::new(ThreadLocal::default())));

#[derive(Debug, Clone)]
pub struct SysSystem {
    exit_tx: Arc<watch::Sender<bool>>,
    runtime: Arc<Runtime>,
    stdio_lock: Arc<Mutex<()>>,
    native_files_path: Option<PathBuf>,
}

impl SysSystem {
    pub fn new(native_files_path: Option<String>, exit: watch::Sender<bool>) -> SysSystem {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let native_files_path = native_files_path
            .map(PathBuf::from);

        SysSystem {
            exit_tx: Arc::new(exit),
            runtime: Arc::new(runtime),
            stdio_lock: Arc::new(Mutex::new(())),
            native_files_path,
        }
    }
    pub fn new_with_runtime(native_files_path: Option<String>, exit: watch::Sender<bool>, runtime: Arc<Runtime>) -> SysSystem {
        let native_files_path = native_files_path
            .map(PathBuf::from);

        SysSystem {
            exit_tx: Arc::new(exit),
            runtime,
            stdio_lock: Arc::new(Mutex::new(())),
            native_files_path,
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(async move {
            future.await
        })
    }
}

#[async_trait]
impl SystemAbi for SysSystem {
    /// Starts an asynchronous task that will run on a shared worker pool
    /// This task must not block the execution or it could cause a deadlock
    fn task_shared(
        &self,
        task: Box<
            dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + 'static,
        >,
    ) {
        self.runtime.spawn(async move {
            let fut = task();
            fut.await
        });
    }

    /// Starts an asynchronous task will will run on a dedicated thread
    /// pulled from the worker pool that has a stateful thread local variable
    /// It is ok for this task to block execution and any async futures within its scope
    fn task_wasm(
        &self,
        task: Box<dyn FnOnce(Store, Module, Option<VMMemory>) -> Pin<Box<dyn Future<Output = ()> + 'static>> + Send + 'static>,
        store: Store,
        module: Module,
        memory_spawn: SpawnType,
    ) -> Result<(), WasiThreadError> {
        use tracing::error;

        let memory: Option<VMMemory> = match memory_spawn {
            SpawnType::CreateWithType(mem) => {
                Some(
                    VMSharedMemory::new(&mem.ty, &mem.style)
                        .map_err(|err| {
                            error!("failed to create memory - {}", err);
                        })
                        .unwrap()
                        .into()
                )
            },
            SpawnType::NewThread(mem) => mem,
            SpawnType::Create => None,
        };
        
        let rt = self.runtime.clone();
        self.runtime.spawn_blocking(move || {
            // Invoke the callback
            let fut = task(store, module, memory);
            rt.block_on(fut)
        });
        Ok(())
    }

    /// Starts an synchronous task will will run on a dedicated thread
    /// pulled from the worker pool. It is ok for this task to block execution
    /// and any async futures within its scope
    fn task_dedicated(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) {
        self.runtime.spawn_blocking(move || {
            task();
        });
    }

    /// Starts an asynchronous task will will run on a dedicated thread
    /// pulled from the worker pool. It is ok for this task to block execution
    /// and any async futures within its scope
    fn task_dedicated_async(
        &self,
        task: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + 'static>> + Send + 'static>,
    ) {
        let rt = self.runtime.clone();
        self.runtime.spawn_blocking(move || {
            let fut = task();
            rt.block_on(fut)
        });
    }

    /// Starts an asynchronous task on the current thread. This is useful for
    /// launching background work with variables that are not Send.
    fn task_local(&self, task: Pin<Box<dyn Future<Output = ()> + 'static>>) {
        tokio::task::spawn_local(async move {
            task.await;
        });
    }

    /// Puts the current thread to sleep for a fixed number of milliseconds
    fn sleep(&self, ms: u128) -> AsyncResult<()> {
        let (tx_done, rx_done) = mpsc::channel(1);
        self.task_shared(Box::new(move || {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(ms as u64)).await;
                let _ = tx_done.send(()).await;
            })
        }));
        AsyncResult::new(SerializationFormat::Json, rx_done)
    }

    /// Fetches a data file from the local context of the process
    #[allow(unused)]
    fn fetch_file(&self, path: &str) -> AsyncResult<Result<Vec<u8>, u32>> {
        let mut path = path.to_string();
        if path.starts_with("/") {
            path = path[1..].to_string();
        };

        let native_files_path = self.native_files_path.clone();
        let (tx_done, rx_done) = mpsc::channel(1);
        self.task_dedicated_async(Box::new(move || {
            Box::pin(async move {
                #[cfg(not(feature = "embedded_files"))]
                let mut ret = Err(err::ERR_ENOENT);
                #[cfg(feature = "embedded_files")]
                let mut ret = PUBLIC_DIR
                    .get_file(path.as_str())
                    .map_or(Err(err::ERR_ENOENT), |file| Ok(file.contents().to_vec()));

                if ret.is_err() {
                    if let Some(native_files) = native_files_path.as_ref() {
                        if path.contains("..") || path.contains("~") || path.contains("//") {
                            warn!("relative paths are a security risk - {}", path);
                            ret = Err(err::ERR_EACCES);
                        } else {
                            let mut path = path.as_str();
                            while path.starts_with("/") {
                                path = &path[1..];
                            }
                            let path = native_files.join(path);
            
                            // Attempt to open the file
                            ret = match std::fs::File::open(path.clone()) {
                                Ok(mut file) => {
                                    let mut data = Vec::new();
                                    file
                                        .read_to_end(&mut data)
                                        .map_err(|err| {
                                            debug!("failed to read local file ({}) - {}", path.to_string_lossy(), err);
                                            err::ERR_EIO
                                        })
                                        .map(|_| data)
                                },
                                Err(err) => {
                                    debug!("failed to open local file ({}) - {}", path.to_string_lossy(), err);
                                    Err(err::ERR_EIO)
                                }
                            };
                        }
                    }
                }

                let _ = tx_done.send(ret).await;
            })
        }));
        AsyncResult::new(SerializationFormat::Bincode, rx_done)
    }

    /// Performs a HTTP or HTTPS request to a destination URL
    fn reqwest(
        &self,
        url: &str,
        method: &str,
        _options: ReqwestOptions,
        headers: Vec<(String, String)>,
        data: Option<Vec<u8>>,
    ) -> AsyncResult<Result<ReqwestResponse, u32>> {
        let method = method.to_string();
        let url = url.to_string();

        let (tx_done, rx_done) = mpsc::channel(1);
        self.task_shared(Box::new(move || {
            Box::pin(async move {
                let ret = move || async move {
                    let method = reqwest::Method::try_from(method.as_str()).map_err(|err| {
                        debug!("failed to convert method ({}) - {}", method, err);
                        err::ERR_EIO
                    })?;

                    let client = reqwest::ClientBuilder::default().build().map_err(|err| {
                        debug!("failed to build reqwest client - {}", err);
                        err::ERR_EIO
                    })?;

                    let mut builder = client.request(method, url.as_str());
                    for (header, val) in headers {
                        if let Ok(header) =
                            reqwest::header::HeaderName::from_bytes(header.as_bytes())
                        {
                            builder = builder.header(header, val);
                        } else {
                            debug!("failed to parse header - {}", header);
                        }
                    }

                    if let Some(data) = data {
                        builder = builder.body(reqwest::Body::from(data));
                    }

                    let request = builder.build().map_err(|err| {
                        debug!("failed to convert request (url={}) - {}", url.as_str(), err);
                        err::ERR_EIO
                    })?;

                    let response = client.execute(request).await.map_err(|err| {
                        debug!("failed to execute reqest - {}", err);
                        err::ERR_EIO
                    })?;

                    let status = response.status().as_u16();
                    let status_text = response.status().as_str().to_string();
                    let data = response.bytes().await.map_err(|err| {
                        debug!("failed to read response bytes - {}", err);
                        err::ERR_EIO
                    })?;
                    let data = data.to_vec();

                    Ok(ReqwestResponse {
                        pos: 0usize,
                        ok: true,
                        status,
                        status_text,
                        redirected: false,
                        data: Some(data),
                        headers: Vec::new(),
                    })
                };
                let ret = ret().await;
                let _ = tx_done.send(ret).await;
            })
        }));
        AsyncResult::new(SerializationFormat::Bincode, rx_done)
    }

    async fn web_socket(&self, url: &str) -> Result<Box<dyn WebSocketAbi>, String> {
        return Ok(Box::new(SysWebSocket::new(url).await?));
    }

    // WebGL is not supported here
    async fn webgl(&self) -> Option<Box<dyn WebGlAbi>> {
        None
    }
}

#[async_trait]
impl ConsoleAbi for SysSystem {
    async fn stdout(&self, data: Vec<u8>) {
        use raw_tty::GuardMode;
        let _guard = self.stdio_lock.lock().unwrap();
        if let Ok(mut stdout) = io::stdout().guard_mode() {
            stdout.write_all(&data[..]).unwrap();
            stdout.flush().unwrap();
        }
    }

    async fn stderr(&self, data: Vec<u8>) {
        use raw_tty::GuardMode;
        let _guard = self.stdio_lock.lock().unwrap();
        if let Ok(mut stderr) = io::stderr().guard_mode() {
            stderr.write_all(&data[..]).unwrap();
            stderr.flush().unwrap();
        }
    }

    async fn flush(&self) {
        use raw_tty::GuardMode;
        let _guard = self.stdio_lock.lock().unwrap();
        if let Ok(mut stdout) = io::stdout().guard_mode() {
            stdout.flush().unwrap();
        }
        if let Ok(mut stderr) = io::stderr().guard_mode() {
            stderr.flush().unwrap();
        }
    }

    /// Writes output to the log
    async fn log(&self, text: String) {
        use raw_tty::GuardMode;
        let _guard = self.stdio_lock.lock().unwrap();
        if let Ok(mut stderr) = io::stderr().guard_mode() {
            write!(&mut *stderr, "{}\r\n", text).unwrap();
            stderr.flush().unwrap();
        }
    }

    /// Gets the number of columns and rows in the terminal
    async fn console_rect(&self) -> ConsoleRect {
        if let Some((w, h)) = term_size::dimensions() {
            ConsoleRect {
                cols: w as u32,
                rows: h as u32,
            }
        } else {
            ConsoleRect { cols: 80, rows: 25 }
        }
    }

    /// Clears the terminal
    async fn cls(&self) {
        print!("{}[2J", 27 as char);
    }

    async fn exit(&self) {
        let _ = self.exit_tx.send(true);
    }
}
