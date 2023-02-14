use std::{sync::{Arc, Mutex}, pin::Pin, task::{Context, Poll}};

use tokio::sync::mpsc;
use wasmer_bus::abi::SerializationFormat;
use wasmer_vbus::{BusError, VirtualBusListener, BusCallEvent, VirtualBusCalled, VirtualBusScope, VirtualBusListener, BusDataFormat};

use crate::api::{System, abi::SystemAbiExt};

use super::{RuntimeCallStateChange, RuntimeNewCall};

#[derive(Clone, Debug)]
pub struct RuntimeBusListener
{
    pub(crate) rx: Arc<Mutex<mpsc::Receiver<RuntimeNewCall>>>
}

impl VirtualBusListener
for RuntimeBusListener
{
    fn poll_call(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<BusCallEvent> {
        let mut guard = self.rx.lock().unwrap();
        match guard.poll_recv(cx) {
            Poll::Ready(Some(call)) => {
                let handle = RuntimeCallInsideHandle {
                    system: Default::default(),
                    tx: call.tx,
                    rx: call.rx,
                };
                Poll::Ready(BusCallEvent {
                    topic: call.topic,
                    format: call.format,
                    data: call.data,
                    called: Box::new(handle),
                })
            }
            _ => Poll::Pending,
        }
    }
}

#[derive(Debug)]
struct RuntimeCallInsideHandle
{
    system: System,
    rx: mpsc::Receiver<RuntimeNewCall>,
    tx: mpsc::Sender<RuntimeCallStateChange>,
}

impl VirtualBusScope
for RuntimeCallInsideHandle {
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl VirtualBusListener
for RuntimeCallInsideHandle {
    fn poll_call(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BusCallEvent> {
        Poll::Pending
    }
}

impl VirtualBusCalled
for RuntimeCallInsideHandle
{
    fn callback(&self, topic: String, format: BusDataFormat, buf: Vec<u8>) {
        let format = crate::bus::conv_format(format);
        self.system.fire_and_forget(&self.tx, RuntimeCallStateChange::Callback {
            topic,
            format,
            buf,
        });
    }

    fn reply(&self, format: BusDataFormat, buf: Vec<u8>) {
        let format = crate::bus::conv_format(format);
        self.system.fire_and_forget(&self.tx, RuntimeCallStateChange::Reply {
            format,
            buf,
        });
    }

    fn fault(self: Box<Self>, fault: BusError) {
        let fault = crate::bus::conv_error(fault);
        self.system.fire_and_forget(&self.tx, RuntimeCallStateChange::Fault {
            fault
        });
    }
}
