#![allow(unused_imports)]
use log::{error, info, debug};

use super::crypto::*;
use super::signature::*;
use super::error::*;
use super::sink::*;
use super::meta::*;
use super::validator::*;
use super::compact::*;
use super::lint::*;
use super::session::*;
use super::transform::*;
use super::plugin::*;
use super::event::*;
use super::header::*;
use super::transaction::*;
use super::trust::*;
use bytes::Bytes;
use fxhash::FxHashMap;
use fxhash::FxHashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TreeAuthorityPlugin
{
    root: WriteOption,
    root_keys: FxHashMap<Hash, PublicSignKey>,
    auth: FxHashMap<PrimaryKey, MetaAuthorization>,
    parents: FxHashMap<PrimaryKey, MetaParent>,
    signature_plugin: SignaturePlugin,
    integrity: IntegrityMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ComputePhase
{
    BeforeStore,
    AfterStore,
}

impl TreeAuthorityPlugin
{
    pub fn new() -> TreeAuthorityPlugin {
        TreeAuthorityPlugin {
            root: WriteOption::Everyone,
            root_keys: FxHashMap::default(),
            signature_plugin: SignaturePlugin::new(),
            auth: FxHashMap::default(),
            parents: FxHashMap::default(),
            integrity: IntegrityMode::Distributed,
        }
    }

    #[allow(dead_code)]
    pub fn add_root_public_key(&mut self, key: &PublicSignKey)
    {
        self.root_keys.insert(key.hash(), key.clone());
        self.root = WriteOption::Any(self.root_keys.keys().map(|k| k.clone()).collect::<Vec<_>>());
    }

    fn compute_auth(&self, meta: &Metadata, trans_meta: &TransactionMetadata, phase: ComputePhase) -> Result<MetaAuthorization, TrustError>
    {
        // If its not got a key then it just inherits the permissions of the root
        let key = match meta.get_data_key() {
            Some(a) => a,
            None => {
                return Ok(
                    MetaAuthorization {
                        read: ReadOption::Everyone(None),
                        write: self.root.clone(),
                    }
                );
            }
        };

        // Get the authorization of this node itself (if its post phase)
        let mut auth = match phase {
            ComputePhase::BeforeStore => None,
            ComputePhase::AfterStore => meta.get_authorization(),
        };

        // In the scenarios that this is before the record is saved or
        // if no authorization is attached to the record then we fall
        // back to whatever is the value in the existing chain of trust
        if auth.is_none() {
            auth = trans_meta.auth.get(&key);
            if auth.is_none() {
                auth = self.auth.get(&key);
            }
        }

        // Fall back on inheriting from the parent if there is no
        // record yet set for this data object
        let (mut read, mut write) = match auth {
            Some(a) => (a.read.clone(), a.write.clone()),
            None => (ReadOption::Inherit, WriteOption::Inherit),
        };

        // Resolve any inheritance through recursive queries
        let mut parent = meta.get_parent();
        while (read == ReadOption::Inherit || write == WriteOption::Inherit)
               && parent.is_some()
        {
            {
                let parent = match parent {
                    Some(a) => a.vec.parent_id,
                    None => unreachable!(),
                };

                // Get the authorization for this parent (if there is one)
                let mut parent_auth = trans_meta.auth.get(&parent);
                if parent_auth.is_none() {
                    parent_auth = self.auth.get(&parent);
                }
                let parent_auth = match parent_auth {
                    Some(a) => a,
                    None => {
                        return Err(TrustError::MissingParent(parent));
                    }
                };

                // Resolve the read inheritance
                if read == ReadOption::Inherit {
                    read = parent_auth.read.clone();
                }
                // Resolve the write inheritance
                if write == WriteOption::Inherit {
                    write = parent_auth.write.clone();
                }
            }

            // Walk up the tree until we have a resolved inheritance or there are no more parents
            parent = match parent {
                Some(a) => {
                    let mut r = trans_meta.parents.get(&a.vec.parent_id);
                    if r.is_none() {
                        r = self.parents.get(&a.vec.parent_id);
                    }
                    match r {
                        Some(a) => Some(a),
                        None => { break; }
                    }
                },
                None => unreachable!(),
            }
        }

        // If we are at the top of the walk and its still inherit then we inherit the
        // permissions of a root node
        if read == ReadOption::Inherit {
            read = ReadOption::Everyone(None);
        }
        if write == WriteOption::Inherit {
            write = self.root.clone();
        }
        let auth = MetaAuthorization {
            read,
            write,
        };

        // Return the result
        Ok(auth)
    }

    fn generate_encrypt_key(&self, auth: &ReadOption, session: &Session) -> Result<Option<(InitializationVector, EncryptKey)>, TransformError>
    {
        match auth {
            ReadOption::Inherit => {
                Err(TransformError::UnspecifiedReadability)
            },
            ReadOption::Everyone(_key) => {
                Ok(None)
            },
            ReadOption::Specific(key_hash, derived) => {
                for key in session.read_keys() {
                    if key.hash() == *key_hash {
                        return Ok(Some((
                            InitializationVector::generate(),
                            derived.transmute(key)?
                        )));
                    }
                }
                for key in session.private_read_keys() {
                    if key.hash() == *key_hash {
                        return Ok(Some((
                            InitializationVector::generate(),
                            derived.transmute_private(key)?
                        )));
                    }
                }
                Err(TransformError::MissingReadKey(key_hash.clone()))
            }
        }
    }

    fn get_encrypt_key(&self, meta: &Metadata, confidentiality: &MetaConfidentiality, iv: Option<&InitializationVector>, session: &Session) -> Result<Option<EncryptKey>, TransformError>
    {
        let trans_meta = TransactionMetadata::default();
        let auth_store;
        let auth = match &confidentiality._cache {
            Some(a) => a,
            None => {
                auth_store = self.compute_auth(meta, &trans_meta, ComputePhase::AfterStore)?;
                &auth_store.read
            }
        };

        match auth {
            ReadOption::Inherit => {
                Err(TransformError::UnspecifiedReadability)
            },
            ReadOption::Everyone(key) => {
                if let Some(_iv) = iv {
                    if let Some(key) = key {
                        return Ok(Some(key.clone()));
                    }
                }
                Ok(None)
            },
            ReadOption::Specific(key_hash, derived) => {
                for key in session.read_keys() {
                    if key.hash() == *key_hash {
                        let inner = derived.transmute(key)?;
                        if inner.short_hash() == confidentiality.hash {
                            return Ok(Some(inner));
                        }
                    }
                }
                for key in session.private_read_keys() {
                    if key.hash() == *key_hash {
                        let inner = derived.transmute_private(key)?;
                        if inner.short_hash() == confidentiality.hash {
                            return Ok(Some(inner));
                        }
                    }
                }
                Err(TransformError::MissingReadKey(key_hash.clone()))
            }
        }
    }
}

impl EventSink
for TreeAuthorityPlugin
{
    fn feed(&mut self, header: &EventHeader, conversation: Option<&Arc<ConversationSession>>) -> Result<(), SinkError>
    {
        
        if let Some(key) = header.meta.get_tombstone() {
            self.auth.remove(&key);
            self.parents.remove(&key);
        }
        else if let Some(key) = header.meta.get_data_key() {
            self.auth.insert(key, match header.meta.get_authorization() {
                Some(a) => a.clone(),
                None => {
                    MetaAuthorization {
                        read: ReadOption::Inherit,
                        write: WriteOption::Inherit
                    }
                }
            });

            if let Some(parent) = header.meta.get_parent() {
                self.parents.insert(key, parent.clone());
            }
        }

        self.signature_plugin.feed(header, conversation)?;
        Ok(())
    }

    fn reset(&mut self) {
        self.auth.clear();
        self.parents.clear();
        self.signature_plugin.reset();
    }
}

impl EventValidator
for TreeAuthorityPlugin
{
    fn clone_validator(&self) -> Box<dyn EventValidator> {
        Box::new(self.clone())
    }

    fn validate(&self, header: &EventHeader, conversation: Option<&Arc<ConversationSession>>) -> Result<ValidationResult, ValidationError>
    {
        // We need to check all the signatures are valid
        self.signature_plugin.validate(header, conversation)?;

        // If it does not need a signature then accept it
        if header.meta.needs_signature() == false && header.raw.data_hash.is_none() {
            return Ok(ValidationResult::Allow);
        }

        // If it has data then we need to check it - otherwise we ignore it
        let hash = match header.raw.data_hash {
            Some(a) => DoubleHash::from_hashes(&header.raw.meta_hash, &a).hash(),
            None => header.raw.meta_hash.clone()
        };

        // It might be the case that everyone is allowed to write freely
        let dummy_trans_meta = TransactionMetadata::default();
        let auth = self.compute_auth(&header.meta, &dummy_trans_meta, ComputePhase::BeforeStore)?;
        
        // Of course if everyone can write here then its allowed
        if auth.write == WriteOption::Everyone {
            return Ok(ValidationResult::Allow);
        }
        
        // Make sure that it has a signature
        let verified_signatures = match self.signature_plugin.get_verified_signatures(&hash) {
            Some(a) => a,
            None =>
            {
                // If integrity is centrally managed and we have seen this public key before in this
                // particular conversation then we can trust the rest of the integrity of the chain
                if self.integrity == IntegrityMode::Centralized {
                    if let Some(conversation) = conversation {
                        if conversation.other_end_is_server {
                            return Ok(ValidationResult::Allow)
                        }

                        let lock = conversation.signatures.read();
                        let already = match &auth.write {
                            WriteOption::Specific(hash) => lock.contains(hash),
                            WriteOption::Any(hashes) => hashes.iter().any(|h| lock.contains(h)),
                            _ => false
                        };
                        if already {
                            return Ok(ValidationResult::Allow)
                        }
                    }
                    debug!("rejected event as it has no signatures (centralized)");
                } else {
                    debug!("rejected event as it has no signatures (distributed)");
                }
                
                // Otherwise fail
                return Err(ValidationError::NoSignatures);
            },
        };
        
        // Compute the auth tree and if a signature exists for any of the auths then its allowed
        let auth_write = auth.write.vals();
        for hash in verified_signatures.iter() {
            if auth_write.contains(hash) {
                return Ok(ValidationResult::Allow);
            }
        }

        // If we get this far then any data events must be denied
        // as all the other possible routes for it to be accepted into the tree have failed
        debug!("rejected event as it is detached from the tree");
        Err(ValidationError::Detached)
    }

    fn set_integrity_mode(&mut self, mode: IntegrityMode) {
        self.integrity = mode;
        self.signature_plugin.set_integrity_mode(mode);
    }
}

impl EventMetadataLinter
for TreeAuthorityPlugin
{
    fn clone_linter(&self) -> Box<dyn EventMetadataLinter> {
        Box::new(self.clone())
    }

    fn metadata_lint_many<'a>(&self, headers: &Vec<LintData<'a>>, session: &Session, conversation: Option<&Arc<ConversationSession>>) -> Result<Vec<CoreMetadata>, LintError>
    {
        let mut ret = Vec::new();

        let mut other = self.signature_plugin.metadata_lint_many(headers, session, conversation)?;
        ret.append(&mut other);

        Ok(ret)
    }

    fn metadata_lint_event(&self, meta: &Metadata, session: &Session, trans_meta: &TransactionMetadata) -> Result<Vec<CoreMetadata>, LintError>
    {
        let mut ret = Vec::new();
        let mut sign_with = Vec::new();

        // Signatures a done using the authorizations before its attached
        let auth = self.compute_auth(meta, trans_meta, ComputePhase::BeforeStore)?;
        match auth.write {
            WriteOption::Specific(_) | WriteOption::Any(_) =>
            {
                for write_hash in auth.write.vals().iter()
                {
                    // Add any signing keys that we have
                    sign_with.append(
                        &mut session.write_keys()
                            .filter(|p| p.hash() == *write_hash)
                            .map(|p| p.hash())
                            .collect::<Vec<_>>()
                    );
                }

                if meta.needs_signature() && sign_with.len() <= 0
                {
                    // This record has no authorization
                    return match meta.get_data_key() {
                        Some(key) => Err(LintError::Trust(TrustError::NoAuthorizationWrite(key, auth.write))),
                        None => Err(LintError::Trust(TrustError::NoAuthorizationOrphan))
                    };
                }

                // Add the signing key hashes for the later stages
                if sign_with.len() > 0 {
                    ret.push(CoreMetadata::SignWith(MetaSignWith {
                        keys: sign_with,
                    }));
                }
            },
            WriteOption::Inherit => {
                return Err(LintError::Trust(TrustError::UnspecifiedWritability));
            },
            WriteOption::Everyone => { },
            WriteOption::Nobody => { },
        }

        // Now lets add all the encryption keys
        let auth = self.compute_auth(meta, trans_meta, ComputePhase::AfterStore)?;
        let key_hash = match &auth.read {
            ReadOption::Everyone(key) => {
                match key {
                    Some(a) => Some(a.short_hash()),
                    None => None,
                }
            }
            ReadOption::Specific(read_hash, derived) =>
            {
                let mut ret = session.read_keys()
                        .filter(|p| p.hash() == *read_hash)
                        .filter_map(|p| derived.transmute(p).ok())
                        .map(|p| p.short_hash())
                        .next();
                if ret.is_none() {
                    ret = session.private_read_keys()
                        .filter(|p| p.hash() == *read_hash)
                        .filter_map(|p| derived.transmute_private(p).ok())
                        .map(|p| p.short_hash())
                        .next();
                }
                if ret.is_none() {
                    if let Some(key) = meta.get_data_key() {
                        return Err(LintError::Trust(TrustError::NoAuthorizationRead(key, auth.read)));
                    }
                }
                ret
            },
            _ => None,
        };
        if let Some(key_hash) = key_hash {
            ret.push(CoreMetadata::Confidentiality(MetaConfidentiality {
                hash: key_hash,
                _cache: Some(auth.read)
            }));
        }

        // Now run the signature plugin
        ret.extend(self.signature_plugin.metadata_lint_event(meta, session, trans_meta)?);

        // We are done
        Ok(ret)
    }
}

impl EventDataTransformer
for TreeAuthorityPlugin
{
    fn clone_transformer(&self) -> Box<dyn EventDataTransformer> {
        Box::new(self.clone())
    }

    #[allow(unused_variables)]
    fn data_as_underlay(&self, meta: &mut Metadata, with: Bytes, session: &Session, trans_meta: &TransactionMetadata) -> Result<Bytes, TransformError>
    {
        let mut with = self.signature_plugin.data_as_underlay(meta, with, session, trans_meta)?;

        let cache = match meta.get_confidentiality() {
            Some(a) => a._cache.as_ref(),
            None => None,
        };

        let auth_store;
        let auth = match &cache {
            Some(a) => a,
            None => {
                auth_store = self.compute_auth(meta, trans_meta, ComputePhase::AfterStore)?;
                &auth_store.read
            }
        };

        if let Some((iv, key)) = self.generate_encrypt_key(auth, session)? {
            let encrypted = key.encrypt_with_iv(&iv, &with[..])?;
            meta.core.push(CoreMetadata::InitializationVector(iv));
            with = Bytes::from(encrypted);
        }

        Ok(with)
    }

    #[allow(unused_variables)]
    fn data_as_overlay(&self, meta: &Metadata, with: Bytes, session: &Session) -> Result<Bytes, TransformError>
    {
        let mut with = self.signature_plugin.data_as_overlay(meta, with, session)?;

        let iv = meta.get_iv().ok();
        match meta.get_confidentiality() {
            Some(confidentiality) => {
                if let Some(key) = self.get_encrypt_key(meta, confidentiality, iv, session)? {
                    let iv = match iv {
                        Some(a) => a,
                        None => { return Err(TransformError::CryptoError(CryptoError::NoIvPresent)); }
                    };
                    let decrypted = key.decrypt(&iv, &with[..])?;
                    with = Bytes::from(decrypted);
                }
            },
            None if iv.is_some() => { return Err(TransformError::UnspecifiedReadability); }
            None => {

            }
        };

        Ok(with)
    }
}

impl EventPlugin
for TreeAuthorityPlugin
{
    fn clone_plugin(&self) -> Box<dyn EventPlugin> {
        Box::new(self.clone())
    }

    fn rebuild(&mut self, headers: &Vec<EventHeader>, conversation: Option<&Arc<ConversationSession>>) -> Result<(), SinkError>
    {
        self.reset();
        self.signature_plugin.rebuild(headers, conversation)?;
        for header in headers {
            self.feed(header, conversation)?;
        }
        Ok(())
    }

    fn root_keys(&self) -> Vec<PublicSignKey>
    {
        self.root_keys.values().map(|a| a.clone()).collect::<Vec<_>>()
    }

    fn set_root_keys(&mut self, root_keys: &Vec<PublicSignKey>)
    {
        self.root_keys.clear();
        self.root = WriteOption::Everyone;

        for root_key in root_keys {
            debug!("chain_root_key: {}", root_key.hash().to_string());
            self.add_root_public_key(root_key);
        }
    }
}
#[derive(Debug, Default, Clone)]
pub struct TreeCompactor
{
    parent_needed: FxHashSet<PrimaryKey>,
}

impl EventSink
for TreeCompactor
{
    fn feed(&mut self, header: &EventHeader, _conversation: Option<&Arc<ConversationSession>>) -> Result<(), SinkError>
    {
        if let Some(parent) = header.meta.get_parent() {
            self.parent_needed.insert(parent.vec.parent_id);
        }
        Ok(())
    }
}

impl EventCompactor
for TreeCompactor
{
    fn clone_compactor(&self) -> Box<dyn EventCompactor> {
        Box::new(self.clone())
    }
    
    fn relevance(&mut self, header: &EventHeader) -> EventRelevance
    {
        if let Some(key) = header.meta.get_data_key()
        {
            if self.parent_needed.remove(&key) {
                return EventRelevance::ForceKeep;       
            }
        }

        return EventRelevance::Abstain;
    }
}