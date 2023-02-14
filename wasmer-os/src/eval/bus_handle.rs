use std::{task::{Poll, Context}, pin::Pin, collections::HashMap, ops::DerefMut, marker::PhantomData, time::Duration};

use async_trait::async_trait;
use derivative::Derivative;
use serde::*;
use tokio::sync::mpsc;
use wasmer_bus::{abi::SerializationFormat, prelude::BusError};
use wasmer_vbus::{BusError as VirtualBusError, BusDataFormat, VirtualBusInvocation, BusInvocationEvent, VirtualBusScope, VirtualBusInvokable};
use crate::{bus::{conv_format, Processable, InvokeResult}, api::abi::SystemAbiExt};

#[allow(unused_imports, dead_code)]
use tracing::{debug, error, info, trace, warn};

use crate::{api::System, bus::{type_name_hash}, common::MAX_MPSC};

use super::{RuntimeCallStateChange, RuntimeNewCall};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RuntimeCallOutsideHandle
{
    pub(crate) system: System,
    pub(crate) task: RuntimeCallOutsideTask,
    pub(crate) rx: mpsc::Receiver<RuntimeCallStateChange>,
    #[derivative(Debug = "ignore")]
    pub(crate) callbacks: HashMap<String, Box<dyn FnMut(SerializationFormat, Vec<u8>) + Send + Sync + 'static>>,
}

#[derive(Debug, Clone)]
pub struct RuntimeCallOutsideTask
{
    pub(crate) system: System,
    pub(crate) tx: mpsc::Sender<RuntimeNewCall>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RuntimeCallResult {
    handle: RuntimeCallOutsideHandle,
    format: SerializationFormat,
    #[derivative(Debug = "ignore")]
    value: Vec<u8>,
}

#[must_use = "You must consume the result - either you continue using the handle(), evaluate the value() or discard() it."]
impl RuntimeCallResult {
    pub fn handle(self) -> RuntimeCallOutsideHandle {
        self.handle
    }

    pub fn value<T>(self) -> Result<T, BusError>
    where T: serde::de::DeserializeOwned {
        self.format.deserialize(self.value)
    }

    pub fn value_raw(self) -> (BusDataFormat, Vec<u8>) {
        let format = crate::bus::conv_format_back(self.format);
        (format, self.value)
    }

    pub fn handle_and_value<T>(self) -> Result<(RuntimeCallOutsideHandle, T), BusError>    
    where T: serde::de::DeserializeOwned {
        let handle = self.handle;
        let value = self.format.deserialize(self.value)?;
        Ok((handle, value))
    }

    pub fn discard(self) {
    }
}

impl RuntimeCallOutsideHandle
{
    pub fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RuntimeCallStateChange>
    {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(msg),
            Poll::Ready(None) => Poll::Ready(RuntimeCallStateChange::Fault { fault: BusError::Aborted }),
            Poll::Pending => Poll::Pending
        }            
    }

    pub fn callback<F, T>(&mut self, mut f: F) -> &mut Self
    where F: FnMut(T),
          F: Send + Sync + 'static,
          T: de::DeserializeOwned
    {
        let hash = type_name_hash::<T>();
        self.callbacks.insert(hash.to_string(), Box::new(move |format, data| {
            let data = format.deserialize(data);
            match data {
                Ok(data) => {
                    f(data);
                },
                Err(err) => {
                    debug!("error while processed callback - {}", err);
                }
            }
        }));
        self
    }

    fn process_msg(&mut self, msg: RuntimeCallStateChange) -> Result<Option<(SerializationFormat, Vec<u8>)>, BusError> {
        match msg {
            RuntimeCallStateChange::Callback { topic, format, buf } => {
                if let Some(callback) = self.callbacks.get_mut(&topic) {
                    callback(format, buf);
                }
                Ok(None)
            },
            RuntimeCallStateChange::Reply { format, buf } => {
                Ok(Some(
                    (format, buf)
                ))
            },
            RuntimeCallStateChange::Fault { fault } => {
                Err(fault)
            }
        }
    }

    pub async fn join(mut self) -> Result<RuntimeCallResult, BusError> {
        while let Some(msg) = self.rx.recv().await {
            if let Some((format, value)) = self.process_msg(msg)? {
                return Ok(RuntimeCallResult {
                    handle: self,
                    format,
                    value,
                });
            }
        }
        Err(BusError::Aborted)
    }

    pub fn block_on(mut self) -> Result<RuntimeCallResult, BusError> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => {
                    if let Some((format, value)) = self.process_msg(msg)? {
                        return Ok(RuntimeCallResult {
                            handle: self,
                            format,
                            value,
                        });
                    }
                },
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(BusError::Aborted);
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    pub fn clone_task(&self) -> RuntimeCallOutsideTask {
        self.task.clone()
    }
}

#[async_trait]
impl Processable
for RuntimeCallOutsideHandle
{
    async fn process(&mut self) -> Result<InvokeResult, BusError> {
        while let Some(msg) = self.rx.recv().await {
            if let Some((format, data)) = self.process_msg(msg)? {
                return Ok(
                    InvokeResult::Response(format, data)
                );
            }
        }
        Err(BusError::Aborted)
    }
}

impl RuntimeCallOutsideHandle
{
    pub fn call<T>(&self, format: SerializationFormat, data: T) -> Result<RuntimeCallOutsideHandle, BusError>
    where T: ser::Serialize {
        self.task.call(format, data)
    }

    pub fn call_raw(&self, topic: String, format: BusDataFormat, data: &[u8]) -> RuntimeCallOutsideHandle {
        self.task.call_raw(topic, format, data)
    }
}

impl RuntimeCallOutsideTask
{
    pub fn call<T>(&self, format: SerializationFormat, data: T) -> Result<RuntimeCallOutsideHandle, BusError>
    where T: ser::Serialize {
        let topic = type_name_hash::<T>();
        let data = format.serialize(data)?;
        Ok(self.call_raw(topic.to_string(), crate::bus::conv_format_back(format), &data))
    }

    pub fn call_raw(&self, topic: String, format: BusDataFormat, data: &[u8]) -> RuntimeCallOutsideHandle {
        let (tx1, rx1) = mpsc::channel(MAX_MPSC);
        let (tx2, rx2) = mpsc::channel(MAX_MPSC);
        self.system.fire_and_forget(&self.tx, RuntimeNewCall {
            topic,
            format,
            data: data.to_vec(),
            tx: tx1,
            rx: rx2,
        });
        RuntimeCallOutsideHandle {
            system: self.system.clone(),
            rx: rx1,
            task: RuntimeCallOutsideTask {
                system: self.system.clone(),
                tx: tx2,
            },
            callbacks: Default::default(),
        }
    }
}

impl VirtualBusInvokable
for RuntimeCallOutsideHandle
{
    fn invoke(
        &self,
        topic: String,
        format: BusDataFormat,
        buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>> {
        self.task.invoke(topic, format, buf)
    }
}

impl VirtualBusInvokable
for RuntimeCallOutsideTask
{
    fn invoke(
        &self,
        topic: String,
        format: BusDataFormat,
        buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>> {
        Ok(Box::new(
            self.call_raw(topic, format, buf)
        ))
    }
}

impl VirtualBusScope
for RuntimeCallOutsideHandle
{
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl VirtualBusInvocation
for RuntimeCallOutsideHandle
{
    fn poll_event(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BusInvocationEvent> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(RuntimeCallStateChange::Callback { topic, format, buf })) => {
                let format = crate::bus::conv_format_back(format);
                Poll::Ready(BusInvocationEvent::Callback { topic, format, data: buf })
            },
            Poll::Ready(Some(RuntimeCallStateChange::Reply { format, buf })) => {
                let format = crate::bus::conv_format_back(format);
                Poll::Ready(BusInvocationEvent::Response { format, data: buf })
            },
            Poll::Ready(Some(RuntimeCallStateChange::Fault { fault })) => {
                let fault = crate::bus::conv_error_back(fault);
                // Poll::Ready(BusInvocationEvent::Fault { fault })
                Poll::Pending
            },
            Poll::Ready(None) => {
                // Poll::Ready(BusInvocationEvent::Fault { fault: BusError::Aborted })
                Poll::Pending
            },
            Poll::Pending => Poll::Pending
        }
    }
}
