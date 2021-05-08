use tokio::io::BufStream;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use std::mem::size_of;
use async_trait::async_trait;
use tokio::io::Result;
use std::io::SeekFrom;

use super::LogLookup;
use super::archive::*;
use super::magic::*;

use crate::spec::LogApi;
use crate::event::*;
use crate::error::*;

#[derive(Debug)]
pub(crate) struct LogAppender
{
    path: String,
    file: File,
    stream: BufStream<File>,
    offset: u64,
    header: Vec<u8>,
    pub(crate) index: u32,
}

impl LogAppender
{
    pub async fn new(path_log: String, truncate: bool, index: u32) -> Result<(LogAppender, LogArchive)>
    {
        // Compute the log file name
        let log_back_path = format!("{}.{}", path_log.clone(), index);
        let log_back = match truncate {
            true => OpenOptions::new().read(true).write(true).truncate(true).create(true).open(log_back_path.clone()).await?,
               _ => OpenOptions::new().read(true).write(true).create(true).open(log_back_path.clone()).await?,
        };

        // Build the appender
        let mut appender = LogAppender {
            path: log_back_path.clone(),
            stream: BufStream::new(log_back.try_clone().await.unwrap()),
            file: log_back,
            offset: 0,
            index,
            header: Vec::new(),
        };
        
        // If it does not have a magic then add one - otherwise read it and check the value
        let version: RedoHeader = match RedoHeader::read(&mut appender).await? {
            Some(a) => a,
            None => {
                let magic = RedoHeader::new(RedoMagic::V1);
                let _ = magic.write(&mut appender).await?;
                appender.sync().await?;
                magic
            }
        };

        // Update the header as reading it from the log file may have updated it
        appender.header = Vec::from(version.inner().clone());
        
        // Create the archive
        let archive = LogArchive::new(path_log, index).await?;

        // Return the result
        Ok(
            (appender, archive)
        )
    }

    pub(super) async fn clone(&mut self) -> Result<LogAppender>
    {
        // We have to flush the stream in-case there is outstanding IO that is not yet written to the backing disk
        self.stream.flush().await?;

        // Copy the file handles
        Ok(
            LogAppender {
                path: self.path.clone(),
                file: self.file.try_clone().await?,
                stream: BufStream::new(self.file.try_clone().await?),
                offset: self.offset,
                index: self.index,
                header: self.header.clone(),
            }
        )
    }

    pub(super) async fn write(&mut self, evt: &EventData, header: &EventHeaderRaw) -> std::result::Result<LogLookup, SerializationError>
    {
        let log_header = crate::LOG_VERSION.write(
            self, 
            &header.meta_bytes[..], 
            match &evt.data_bytes {
                Some(d) => Some(&d[..]),
                None => None
            },
            evt.format
        ).await?;

        // Record the lookup map
        let lookup = LogLookup {
            index: self.index,
            offset: log_header.offset
        };

        // Return the log pointer
        Ok(lookup)
    }

    pub(crate) fn path(&self) -> &String
    {
        &self.path
    }

    pub(crate) fn header(&self) -> &[u8] {
        &self.header[..]
    }

    pub(super) async fn flush(&mut self) -> Result<()>
    {
        self.stream.flush().await?;
        Ok(())
    }

    pub(super) async fn sync(&mut self) -> Result<()>
    {
        self.flush().await?;
        self.file.sync_all().await?;
        Ok(())
    }
}

#[async_trait]
impl LogApi
for LogAppender
{
    fn offset(&self) -> u64 {
        self.offset
    }

    async fn len(&self) -> Result<u64> {
        Ok(self.file.metadata().await?.len())
    }

    async fn seek(&mut self, off: u64) -> Result<()> {
        self.stream.flush().await?;
        self.file.seek(SeekFrom::Start(off)).await?;
        self.offset = off;
        self.stream = BufStream::new(self.file.try_clone().await?);
        Ok(())
    }
    
    async fn read_u8(&mut self) -> Result<u8> {
        let ret = self.stream.read_u8().await?;
        self.offset = self.offset + size_of::<u8>() as u64;
        Ok(ret)
    }

    async fn read_u16(&mut self) -> Result<u16> {
        let ret = self.stream.read_u16().await?;
        self.offset = self.offset + size_of::<u16>() as u64;
        Ok(ret)
    }

    async fn read_u32(&mut self) -> Result<u32> {
        let ret = self.stream.read_u32().await?;
        self.offset = self.offset + size_of::<u32>() as u64;
        Ok(ret)
    }

    async fn read_u64(&mut self) -> Result<u64> {
        let ret = self.stream.read_u64().await?;
        self.offset = self.offset + size_of::<u64>() as u64;
        Ok(ret)
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let amt = self.stream.read_exact(&mut buf[..]).await?;
        self.offset = self.offset + amt as u64;
        Ok(())
    }

    async fn write_u8(&mut self, val: u8) -> Result<()> {
        self.stream.write_u8(val).await?;
        self.offset = self.offset + size_of::<u8>() as u64;
        Ok(())
    }

    async fn write_u16(&mut self, val: u16) -> Result<()> {
        self.stream.write_u16(val).await?;
        self.offset = self.offset + size_of::<u16>() as u64;
        Ok(())
    }

    async fn write_u32(&mut self, val: u32) -> Result<()> {
        self.stream.write_u32(val).await?;
        self.offset = self.offset + size_of::<u32>() as u64;
        Ok(())
    }

    async fn write_u64(&mut self, val: u64) -> Result<()> {
        self.stream.write_u64(val).await?;
        self.offset = self.offset + size_of::<u64>() as u64;
        Ok(())
    }

    async fn write_exact(&mut self, buf: &[u8]) -> Result<()> {
        self.stream.write_all(&buf[..]).await?;
        self.offset = self.offset + buf.len() as u64;
        Ok(())
    }
}

impl Drop
for LogAppender
{
    fn drop(&mut self) {
        let exec = async_executor::LocalExecutor::default();
        let _ = futures::executor::block_on(exec.run(self.stream.shutdown()));
    }
}