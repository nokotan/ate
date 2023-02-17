use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use derivative::Derivative;
use tokio::sync::mpsc;
use wasmer_bus::abi::SerializationFormat;
use wasmer_vbus::BusDataFormat;
use wasmer_vbus::BusInvocationEvent;
use wasmer_vbus::BusError;
use wasmer_vbus::VirtualBusInvocation;
use wasmer_vbus::VirtualBusInvokable;
use wasmer_vbus::VirtualBusScope;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AsyncResult<T> {
    #[derivative(Debug = "ignore")]
    pub rx: mpsc::Receiver<T>,
    pub(crate) format: SerializationFormat,
}

impl<T> AsyncResult<T> {
    pub fn new(format: SerializationFormat, rx: mpsc::Receiver<T>) -> Self {
        Self { rx, format }
    }

    pub fn block_on(mut self) -> Option<T> {
        self.rx.blocking_recv()
    }
}

impl<T> Future for AsyncResult<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.rx.poll_recv(cx)
    }
}

impl<T> VirtualBusInvocation
for AsyncResult<T>
where T: Send + 'static,
      T: serde::ser::Serialize
{
    fn poll_event(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BusInvocationEvent> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                Poll::Ready(BusInvocationEvent::Response {
                    format: crate::bus::conv_format_back(self.format),
                    data: match self.format.serialize(data) {
                        Ok(d) => d,
                        Err(err) => {
                            // return Poll::Ready(BusInvocationEvent::Fault { fault: crate::bus::conv_error_back(err) });
                            return Poll::Ready(crate::bus::conv_fault_to_callback(crate::bus::conv_error_back(err)));
                        }
                    }
                })
            },
            Poll::Ready(None) => {
                // Poll::Ready(BusInvocationEvent::Fault { fault: BusError::Aborted })
                Poll::Ready(crate::bus::conv_fault_to_callback(BusError::Aborted))
            },
            Poll::Pending => Poll::Pending
        }
    }
}

impl<T> VirtualBusScope
for AsyncResult<T>
where T: Send + 'static,
      T: serde::ser::Serialize
{
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl<T> VirtualBusInvokable
for AsyncResult<T>
where T: Send + 'static
{
    fn invoke(
        &self,
        _topic: String,
        _format: BusDataFormat,
        _buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>> {
        wasmer_vbus::Result::Err(BusError::InvalidTopic)
    }
}
