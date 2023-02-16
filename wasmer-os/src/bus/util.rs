use serde::*;
use std::pin::Pin;
use core::task::{Context, Poll};
#[allow(unused_imports, dead_code)]
use tracing::{debug, error, info, trace, warn};
use wasmer_bus::abi::BusError;
use wasmer_bus::abi::SerializationFormat;
use wasmer_vbus::BusDataFormat;
use wasmer_vbus::BusInvocationEvent;
use wasmer_vbus::VirtualBusInvokable;
use wasmer_vbus::VirtualBusScope;
use wasmer_vbus::VirtualBusInvocation;
use wasmer_vbus::{ BusError as VirtualBusError };

pub fn conv_error(fault: VirtualBusError) -> BusError {
    use VirtualBusError::*;
    match fault {
        Serialization => BusError::SerializationFailed,
        Deserialization => BusError::DeserializationFailed,
        InvalidWapm => BusError::InvalidWapm,
        FetchFailed => BusError::FetchFailed,
        CompileError => BusError::CompileError,
        InvalidABI => BusError::IncorrectAbi,
        Aborted => BusError::Aborted,
        BadHandle => BusError::InvalidHandle,
        InvalidTopic => BusError::InvalidTopic,
        BadCallback => BusError::MissingCallbacks,
        Unsupported => BusError::Unsupported,
        BadRequest => BusError::BadRequest,
        AccessDenied => BusError::AccessDenied,
        InternalError => BusError::InternalFailure,
        MemoryAllocationFailed => BusError::MemoryAllocationFailed,
        InvokeFailed => BusError::BusInvocationFailed,
        AlreadyConsumed => BusError::AlreadyConsumed,
        MemoryAccessViolation => BusError::MemoryAccessViolation,
        UnknownError => BusError::Unknown,
    }
}

pub fn conv_error_back(fault: BusError) -> VirtualBusError {
    use VirtualBusError::*;
    match fault {
        BusError::SerializationFailed => Serialization,
        BusError::DeserializationFailed => Deserialization,
        BusError::InvalidWapm => InvalidWapm,
        BusError::FetchFailed => FetchFailed,
        BusError::CompileError => CompileError,
        BusError::IncorrectAbi => InvalidABI,
        BusError::Aborted => Aborted,
        BusError::InvalidHandle => BadHandle,
        BusError::InvalidTopic => InvalidTopic,
        BusError::MissingCallbacks => BadCallback,
        BusError::Unsupported => Unsupported,
        BusError::BadRequest => BadRequest,
        BusError::AccessDenied => AccessDenied,
        BusError::InternalFailure => InternalError,
        BusError::MemoryAllocationFailed => MemoryAllocationFailed,
        BusError::BusInvocationFailed => InvokeFailed,
        BusError::AlreadyConsumed => AlreadyConsumed,
        BusError::MemoryAccessViolation => MemoryAccessViolation,
        BusError::Unknown => UnknownError,
        BusError::Success => UnknownError,
    }
}

pub fn conv_format(format: BusDataFormat) -> SerializationFormat {
    use BusDataFormat::*;
    match format {
        Raw => SerializationFormat::Raw,
        Bincode => SerializationFormat::Bincode,
        MessagePack => SerializationFormat::MessagePack,
        Json => SerializationFormat::Json,
        Yaml => SerializationFormat::Yaml,
        Xml => SerializationFormat::Xml
    }
}

pub fn conv_format_back(format: SerializationFormat) -> BusDataFormat {
    use BusDataFormat::*;
    match format {
        SerializationFormat::Raw => Raw,
        SerializationFormat::Bincode => Bincode,
        SerializationFormat::MessagePack => MessagePack,
        SerializationFormat::Json => Json,
        SerializationFormat::Yaml => Yaml,
        SerializationFormat::Xml => Xml
    }
}

pub fn decode_request<T>(format: BusDataFormat, request: &[u8]) -> Result<T, BusError>
where
    T: de::DeserializeOwned,
{
    let format = conv_format(format);
    format.deserialize(request.to_vec())
}

pub fn encode_response<T>(format: BusDataFormat, response: &T) -> Result<Vec<u8>, BusError>
where
    T: Serialize,
{
    let format = conv_format(format);
    format.serialize(response)
}

pub fn encode_instant_response<T>(format: BusDataFormat, response: &T) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>>
where
    T: Serialize,
{
    match encode_response(format, response) {
        Ok(data) => {
            Ok(Box::new(InstantInvocation::new(
            BusInvocationEvent::Response { format, data }
            )))
        },
        Err(err) => {
            Err(conv_error_back(err))
        }
    }
}

pub fn encode_instant_fault(err: BusError) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>> {

}

#[derive(Debug)]
struct InstantInvocation {
    val: Option<BusInvocationEvent>
}

impl InstantInvocation {
    fn new(val: BusInvocationEvent) -> Self {
        Self {
            val: Some(val)
        }
    }
}

impl VirtualBusInvokable
for InstantInvocation
{
    fn invoke(
        &self,
        _topic: String,
        _format: BusDataFormat,
        _buf: &[u8],
    ) -> wasmer_vbus::Result<Box<dyn VirtualBusInvocation + Sync>> {
        Ok(Box::new(
            InstantInvocation {
                val: None
            }
        ))
    }
}

impl VirtualBusScope 
for InstantInvocation
{
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.val {
            Some(_) => Poll::Pending,
            None => Poll::Ready(())
        }
    }
}

impl VirtualBusInvocation
for InstantInvocation
{
    fn poll_event(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<BusInvocationEvent> {
        match self.val.take() {
            Some(val) => {
                Poll::Ready(val)
            },
            None => {
                Poll::Pending
            }
        }
    }
}
