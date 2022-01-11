use tokio::sync::mpsc;
use wasm_bus::abi::SerializationFormat;
use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::pin::Pin;

pub struct AsyncResult<T> {
    pub(crate) rx: mpsc::Receiver<T>,
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

impl<T> Future
for AsyncResult<T>
{
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        self.rx.poll_recv(cx)
    }
}