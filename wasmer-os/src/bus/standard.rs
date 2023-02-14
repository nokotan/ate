use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
#[allow(unused_imports, dead_code)]
use tracing::{debug, error, info, trace, warn};
use wasmer_vbus::BusDataFormat;
use wasmer_vbus::BusError;
use wasmer_vbus::Result;
use wasmer_vbus::VirtualBusInvokable;
use wasmer_vbus::VirtualBusInvocation;
use wasmer_vbus::VirtualBusProcess;
use wasmer_vbus::VirtualBusScope;

use crate::api::System;
use crate::fd::*;
use crate::fs::TtyFile;
use crate::stdio::*;
use crate::stdout::*;

use super::*;

#[derive(Debug, Clone)]
pub struct StandardBus {
    system: System,
    process_factory: ProcessExecFactory,
}

impl StandardBus {
    pub fn new(process_factory: ProcessExecFactory) -> StandardBus {
        StandardBus {
            system: Default::default(),
            process_factory,
        }
    }

    pub fn stdio(&self, env: &LaunchEnvironment) -> Stdio {
        self.process_factory.stdio(env)
    }

    #[allow(dead_code)]
    pub fn stdin(&self, env: &LaunchEnvironment) -> Fd {
        self.process_factory.stdin(env)
    }

    pub fn stdout(&self, env: &LaunchEnvironment) -> Stdout {
        self.process_factory.stdout(env)
    }

    pub fn stderr(&self, env: &LaunchEnvironment) -> Fd {
        self.process_factory.stderr(env)
    }
}

impl VirtualBusProcess
for StandardBus
{
    fn exit_code(&self) -> Option<u32> {
        None
    }
}

impl VirtualBusScope
for StandardBus
{
    fn poll_finished(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl VirtualBusInvokable
for StandardBus
{
    fn invoke(
        &self,
        topic_hash: u128,
        format: BusDataFormat,
        buf: Vec<u8>,
    ) -> Result<Box<dyn VirtualBusInvocation + Sync>> {
        let format = conv_format(format);
        match topic_hash {
            h if h == type_name_hash::<wasmer_bus_ws::api::SocketBuilderConnectRequest>() =>
            {
                let request = match format.deserialize(buf) {
                    Ok(a) => a,
                    Err(err) => {
                        return Err(conv_error_back(err))
                    }
                };
                Ok(
                    Box::new(ws::web_socket(request))
                )
            }
            h if h == type_name_hash::<wasmer_bus_time::api::TimeSleepRequest>() => {
                let request: wasmer_bus_time::api::TimeSleepRequest = match format.deserialize(buf) {
                    Ok(a) => a,
                    Err(err) => {
                        return Err(conv_error_back(err))
                    }
                };
                Ok(time::sleep(self.system, request.duration_ms))
            }
            h if h == type_name_hash::<wasmer_bus_reqwest::api::ReqwestMakeRequest>() => {
                let request: wasmer_bus_reqwest::api::ReqwestMakeRequest = match format.deserialize(buf) {
                    Ok(a) => a,
                    Err(err) => {
                        return Err(conv_error_back(err))
                    }
                };
                reqwest::reqwest(self.system, request)
            }
            h if h == type_name_hash::<wasmer_bus_tty::api::TtyStdinRequest>() => {
                let env = self.process_factory.launch_env();
                let stdio = self.stdio(&env);
                let tty = TtyFile::new(&stdio);
                Ok(tty::stdin(tty))
            }
            h if h == type_name_hash::<wasmer_bus_tty::api::TtyStdoutRequest>() => {
                let env = self.process_factory.launch_env();
                let stdout = self.stdout(&env);
                Ok(tty::stdout(self.system, stdout.fd()))
            }
            h if h == type_name_hash::<wasmer_bus_tty::api::TtyStderrRequest>() => {
                let env = self.process_factory.launch_env();
                let stderr = self.stderr(&env);
                Ok(tty::stderr(self.system, stderr))
            }
            h if h == type_name_hash::<wasmer_bus_tty::api::TtyRectRequest>() => {
                let env = self.process_factory.launch_env();
                Ok(tty::rect(self.system, &env.abi))
            }
            h if h == type_name_hash::<wasmer_bus_process::api::PoolSpawnRequest>() => {
                let request = match format.deserialize(buf) {
                    Ok(a) => a,
                    Err(err) => {
                        return Err(conv_error_back(err))
                    }
                };
                let factory = self.process_factory.clone();
                Ok(sub_process::process_spawn(factory, request))
            }
            /*
            h if h == type_name_hash::<wasmer_bus_webgl::api::WebGlContextRequest>() => {
                let request = format.deserialize(buf)?;
                webgl::webgl2(self.system, request)
            }
            */
            _ => {
                error!("the os function ({}) is not supported", topic_hash);
                Err(BusError::Unsupported)
            },
        }
    }
}
