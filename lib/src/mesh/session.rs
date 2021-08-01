use async_trait::async_trait;
use tracing::{info, warn, debug, error, trace};
use parking_lot::Mutex as StdMutex;
use std::net::SocketAddr;
use std::{sync::Arc, sync::Weak};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use fxhash::FxHashMap;
use parking_lot::RwLock as StdRwLock;
use std::ops::Rem;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tokio::select;

use super::recoverable_session_pipe::*;
use super::lock_request::*;
use super::core::*;
use crate::{anti_replay::AntiReplayPlugin, comms::*};
use crate::trust::*;
use crate::chain::*;
use crate::error::*;
use crate::conf::*;
use crate::transaction::*;
use super::msg::*;
use crate::pipe::*;
use crate::header::*;
use crate::spec::*;
use crate::loader::*;
use crate::crypto::*;
use crate::meta::*;
use crate::session::*;
use crate::time::*;
use crate::engine::*;

#[cfg(feature="enable_dns")]
type MeshConnectAddr = SocketAddr;
#[cfg(not(feature="enable_dns"))]
type MeshConnectAddr = crate::conf::MeshAddress;

pub struct MeshSession
{
    pub(super) addr: MeshAddress,
    pub(super) key: ChainKey,
    pub(super) sync_tolerance: Duration,
    pub(super) chain: Weak<Chain>,
    pub(super) commit: Arc<StdMutex<FxHashMap<u64, mpsc::Sender<Result<u64, CommitError>>>>>,
    pub(super) lock_requests: Arc<StdMutex<FxHashMap<PrimaryKey, LockRequest>>>,
    pub(super) inbound_conversation: Arc<ConversationSession>,
    pub(super) outbound_conversation: Arc<ConversationSession>,
}

impl MeshSession
{
    pub(super) async fn connect
    (
        builder: ChainBuilder,
        cfg_mesh: &ConfMesh,
        chain_key: &ChainKey,
        addr: MeshAddress,
        hello_path: String,
        loader_local: impl Loader + 'static,
        loader_remote: impl Loader + 'static
    )
    -> Result<Arc<Chain>, ChainCreationError>
    {
        debug!("new: chain_key={}", chain_key.to_string());

        #[cfg(feature = "enable_super_verbose")]
        {
            let bt = backtrace::Backtrace::new();
            trace!("{:?}", bt);
        }

        let temporal = builder.temporal;

        // While we load the data on disk we run in centralized mode
        // as otherwise there could be errors loading the redo log
        let mut builder = builder.clone();
        builder = builder.integrity(IntegrityMode::Centralized);

        // Open the chain and make a sample of the last items so that we can
        // speed up the synchronization by skipping already loaded items
        let mut chain = {
            let chain_key = chain_key.clone();
            
            // Generate a better key name
            let mut key_name = chain_key.name.clone();
            if key_name.starts_with("/") {
                key_name = key_name[1..].to_string();
            }
            let chain_key = ChainKey::new(format!("{}", key_name).to_string());

            // Generate the chain object
            let mut chain = Chain::new_ext(builder.clone(), chain_key, Some(Box::new(loader_local)), true).await?;
            chain.remote_addr = Some(addr.clone());
            chain
        };

        // While we are running offline we run in full distributed mode until
        // we are reconnect as otherwise if the server is in distributed mode
        // it will immediately reject everything
        chain.single().await.set_integrity(IntegrityMode::Distributed);

        // Create a session pipe
        let chain_store = Arc::new(StdMutex::new(None));
        let session = RecoverableSessionPipe {
            cfg_mesh: cfg_mesh.clone(),
            next: NullPipe::new(),
            active: RwLock::new(None),
            mode: builder.cfg_ate.recovery_mode,
            addr,
            hello_path,
            key: chain_key.clone(),
            builder,
            chain: Arc::clone(&chain_store),
            loader_remote: StdMutex::new(Some(Box::new(loader_remote))),
        };
        
        // Add the pipe to the chain and cement it
        chain.proxy(Box::new(session));
        let chain = Arc::new(chain);

        // Set a reference to the chain and trigger it to connect!
        chain_store.lock().replace(Arc::downgrade(&chain));
        let on_disconnect = chain.pipe.connect().await?;

        // Launch an automatic reconnect thread
        if temporal == false {
            TaskEngine::spawn(RecoverableSessionPipe::auto_reconnect(Arc::downgrade(&chain), on_disconnect));
        }

        // Ok we are good!
        Ok(chain)
    }

    pub(super) async fn inbox_events(self: &Arc<MeshSession>, evts: Vec<MessageEvent>, loader: &mut Option<Box<dyn Loader>>) -> Result<(), CommsError> {
        trace!("inbox: events cnt={}", evts.len());

        match self.chain.upgrade() {
            Some(chain) =>
            {
                // Convert the events but we do this differently depending on on if we are
                // in a loading phase or a running phase
                let feed_me = MessageEvent::convert_from(evts.into_iter());
                let feed_me = match loader.as_mut() {
                    Some(l) =>
                    {
                        // Feeding the events into the loader lets proactive feedback to be given back to
                        // the user such as progress bars
                        l.feed_events(&feed_me);

                        // When we are running then we proactively remove any duplicates to reduce noise
                        // or the likelihood of errors
                        feed_me.into_iter()
                            .filter(|e| {
                                l.relevance_check(e) == false
                            })
                            .collect::<Vec<_>>()
                    },
                    None => feed_me
                };
            
                // We only feed the transactions into the local chain otherwise this will
                // reflect events back into the chain-of-trust running on the server
                chain.pipe.feed(Transaction {
                    scope: TransactionScope::Local,
                    transmit: false,
                    events: feed_me,
                    conversation: Some(Arc::clone(&self.inbound_conversation)),
                }).await?;
            },
            None => { }
        };

        Ok(())
    }

    pub(super) async fn inbox_confirmed(self: &Arc<MeshSession>, id: u64) -> Result<(), CommsError> {
        trace!("inbox: commit_confirmed id={}", id);

        let r = {
            let mut lock = self.commit.lock();
            lock.remove(&id)
        };
        if let Some(result) = r {
            result.send(Ok(id)).await?;
        } else {
            debug!("orphaned confirmation!");
        }
        Ok(())
    }

    pub(super) async fn inbox_commit_error(self: &Arc<MeshSession>, id: u64, err: String) -> Result<(), CommsError> {
        trace!("inbox: commit_error id={}, err={}", id, err);

        let r= {
            let mut lock = self.commit.lock();
            lock.remove(&id)
        };
        if let Some(result) = r {
            result.send(Err(CommitError::RootError(err))).await?;
        }
        Ok(())
    }

    pub(super) fn inbox_lock_result(self: &Arc<MeshSession>, key: PrimaryKey, is_locked: bool) -> Result<(), CommsError> {
        trace!("inbox: lock_result key={} is_locked={}", key.to_string(), is_locked);

        let mut remove = false;
        let mut guard = self.lock_requests.lock();
        if let Some(result) = guard.get_mut(&key) {
            if result.entropy(is_locked) == true {
                remove = true;
            }
        }
        if remove == true { guard.remove(&key); }
        Ok(())
    }

    pub(super) async fn record_delayed_upload(chain: &Arc<Chain>, pivot: ChainTimestamp) -> Result<(), CommsError>
    {
        let mut guard = chain.inside_async.write().await;
        let from = guard.range_keys(pivot..).next();
        if let Some(from) = from
        {
            if let Some(a) = guard.chain.timeline.pointers.get_delayed_upload(from) {
                trace!("delayed_upload exists: {}..{}", a.from, a.to);
                return Ok(());
            }

            let to = guard.range_keys(from..).next_back();
            if let Some(to) = to {
                trace!("delayed_upload new: {}..{}", from, to);
                guard.feed_meta_data(&chain.inside_sync, Metadata {
                    core: vec![CoreMetadata::DelayedUpload(MetaDelayedUpload {
                        complete: false,
                        from: from.clone(),
                        to: to.clone()
                    })]
                }).await?;
            } else {
                trace!("delayed_upload: {}..error", from);
            }
        } else {
            trace!("delayed_upload: error..error");
        }

        Ok(())
    }

    pub(super) async fn complete_delayed_upload(chain: &Arc<Chain>, from: ChainTimestamp, to: ChainTimestamp) -> Result<(), CommsError>
    {
        trace!("delayed_upload complete: {}..{}", from, to);
        let mut guard = chain.inside_async.write().await;
        let _ = guard.feed_meta_data(&chain.inside_sync, Metadata {
            core: vec![CoreMetadata::DelayedUpload(MetaDelayedUpload {
                complete: true,
                from,
                to
            })]
        }).await?;
        Ok(())
    }

    pub(super) async fn inbox_start_of_history(self: &Arc<MeshSession>, size: usize, _from: Option<ChainTimestamp>, to: Option<ChainTimestamp>, loader: &mut Option<Box<dyn Loader>>, root_keys: Vec<PublicSignKey>, integrity: IntegrityMode) -> Result<(), CommsError>
    {
        // Declare variables
        let size = size;

        if let Some(chain) = self.chain.upgrade()
        {
            #[cfg(feature = "enable_verbose")]
            trace!("start_of_history: chain_key={}", chain.key());

            {
                // Setup the chain based on the properties given to us
                let mut lock = chain.inside_sync.write();
                lock.set_integrity_mode(integrity);
                for plugin in lock.plugins.iter_mut() {
                    plugin.set_root_keys(&root_keys);
                }
            }

            // If we are synchronizing from an earlier point in the tree then
            // add all the events into a redo log that will be shippped
            if let Some(to) = to {
                let next = {
                    let multi = chain.multi().await;
                    let guard = multi.inside_async.read().await;
                    let mut iter = guard
                        .range_keys(to..);
                    iter.next();
                    iter.next()
                };
                if let Some(next) = next {
                    MeshSession::record_delayed_upload(&chain, next).await?;
                }
            }
        }
        
        // Tell the loader that we will be starting the load process of the history
        if let Some(loader) = loader {
            loader.start_of_history(size).await;
        }

        Ok(())
    }

    pub(super) async fn inbox_end_of_history(self: &Arc<MeshSession>, _pck: PacketWithContext<Message, ()>, loader: &mut Option<Box<dyn Loader>>) -> Result<(), CommsError> {
        trace!("inbox: end_of_history");

        // The end of the history means that the chain can now be actively used, its likely that
        // a loader is waiting for this important event which will then release some caller who
        // wanted to use the data but is waiting for it to load first.
        if let Some(mut loader) = loader.take() {
            loader.end_of_history().await;
        }
        Ok(())
    }

    pub(super) async fn inbox_secure_with(self: &Arc<MeshSession>, mut session: crate::session::AteSession) -> Result<(), CommsError> {
        if let Some(chain) = self.chain.upgrade() {
            trace!("received 'secure_with' secrets");
            chain.inside_sync.write().default_session.user.properties.append(&mut session.user.properties);
        }
        Ok(())
    }

    pub(super) async fn inbox_packet(
        self: &Arc<MeshSession>,
        loader: &mut Option<Box<dyn Loader>>,
        pck: PacketWithContext<Message, ()>,
    ) -> Result<(), CommsError>
    {
        #[cfg(feature = "enable_super_verbose")]
        trace!("inbox: packet size={}", pck.data.bytes.len());

        match pck.packet.msg {
            Message::StartOfHistory { size, from, to, root_keys, integrity }
                => {
                    Self::inbox_start_of_history(self, size, from, to, loader, root_keys, integrity).await?;
                },
            Message::Events { commit: _, evts }
                => {
                    Self::inbox_events(self, evts, loader).await?;
                },
            Message::Confirmed(id)
                => {
                    Self::inbox_confirmed(self, id).await?;
                },
            Message::CommitError { id, err }
                => {
                    Self::inbox_commit_error(self, id, err).await?;
                },
            Message::LockResult { key, is_locked }
                => {
                    Self::inbox_lock_result(self, key, is_locked)?;
                },
            Message::EndOfHistory
                => {
                    Self::inbox_end_of_history(self, pck, loader).await?;
                },
            Message::SecuredWith(session)
                => {
                    Self::inbox_secure_with(self, session).await?;
                },
            Message::FatalTerminate(fatal)
                => {
                    if let Some(mut loader) = loader.take() {
                        loader.failed(ChainCreationError::ServerRejected(fatal.clone())).await;
                    }
                    warn!("mesh-session-err: {}", fatal);
                    return Err(CommsError::Disconnected);
                },
            _ => { }
        };

        Ok(())
    }

    pub(super) async fn cancel_commits(&self)
    {
        let mut senders = Vec::new();
        {
            let mut guard = self.commit.lock();
            for (_, sender) in guard.drain() {
                senders.push(sender);
            }
        }

        for sender in senders.into_iter() {
            if let Err(err) = sender.send(Err(CommitError::Aborted)).await {
                warn!("mesh-session-cancel-err: {}", err.to_string());
            }
        }
    }

    pub(super) fn cancel_locks(&self)
    {
        let mut guard = self.lock_requests.lock();
        for (_, sender) in guard.drain() {
            sender.cancel();
        }
    }

    pub(super) fn cancel_sniffers(&self)
    {
        if let Some(guard) = self.chain.upgrade() {
            let mut lock = guard.inside_sync.write();
            lock.sniffers.clear();
        }
    }
}

pub(crate) struct MeshSessionProcessor
{
    pub(crate) addr: MeshAddress,
    pub(crate) loader: Option<Box<dyn Loader>>,
    pub(crate) session: Weak<MeshSession>,
    pub(crate) status_tx: mpsc::Sender<ConnectionStatusChange>,
}

#[async_trait]
impl InboxProcessor<Message, ()>
for MeshSessionProcessor
{
    async fn process(&mut self, pck: PacketWithContext<Message, ()>) -> Result<(), CommsError>
    {
        let session = match Weak::upgrade(&self.session) {
            Some(a) => a,
            None => {
                trace!("inbox-server-exit: reference dropped scope");
                return Err(CommsError::Disconnected);
            }
        };

        MeshSession::inbox_packet(&session, &mut self.loader, pck).await?;
        Ok(())        
    }

    async fn shutdown(&mut self, _sock_addr: MeshConnectAddr)
    {
        info!("disconnected: {}:{}", self.addr.host, self.addr.port);
        if let Some(session) = self.session.upgrade() {
            session.cancel_commits().await;
            session.cancel_sniffers();
            session.cancel_locks();
        }
        
        // We should only get here if the inbound connection is shutdown or fails
        let _ = self.status_tx.send(ConnectionStatusChange::Disconnected).await;
    }
}

impl Drop
for MeshSession
{
    fn drop(&mut self)
    {
        trace!("drop {}", self.key.to_string());
        self.cancel_locks();
        self.cancel_sniffers();
    }
}