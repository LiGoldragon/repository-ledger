//! Repository ledger runtime library.
//!
//! `repository-ledger-daemon` owns this store and exposes ordinary and owner
//! Signal sockets over it.

use std::path::Path;

pub mod client;
pub mod daemon;
pub mod frame_io;
pub mod spool;

use owner_signal_repository_ledger::{
    MirrorPolicy, MirrorPolicySet, RepositoryRegistered, RepositoryRetired, RetireRepository,
    SpoolDirectoryPolicy, SpoolDirectoryPolicySet,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey, Retraction,
    TableDescriptor, TableName, TableReference,
};
use signal_core::{NonEmpty, OperationFailureReason, Reply, RequestRejectionReason, SubReply};
use signal_repository_ledger::{
    ChannelReply, ChannelRequest, RepositoryCatalogListing, RepositoryEvent,
    RepositoryEventListing, RepositoryEventQuery, RepositoryEventRecorded, RepositoryEventSequence,
    RepositoryLedgerReply, RepositoryLedgerRequest, RepositoryLedgerRequestUnimplemented,
    RepositoryLedgerUnimplementedReason, RepositoryReceiveHookNotification, RepositoryRegistration,
};

const SCHEMA_VERSION: u32 = 1;
const REPOSITORY_EVENTS: TableName = TableName::new("repository_events");
const REPOSITORY_REGISTRATIONS: TableName = TableName::new("repository_registrations");
const SPOOL_DIRECTORY_POLICY: TableName = TableName::new("spool_directory_policy");
const MIRROR_POLICIES: TableName = TableName::new("mirror_policies");

#[derive(Debug, thiserror::Error)]
pub enum RepositoryLedgerError {
    #[error("sema-engine error: {0}")]
    Engine(#[from] sema_engine::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("signal frame error: {0}")]
    Frame(#[from] signal_core::FrameError),

    #[error("NOTA decode error: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("configuration decode error: {0}")]
    Configuration(#[from] nota_config::Error),

    #[error("expected exactly one argument")]
    ExpectedSingleArgument,

    #[error("flag-style arguments are not part of component binaries: {0}")]
    FlagArgument(String),

    #[error("unexpected signal frame for this socket")]
    UnexpectedFrame,

    #[error("connection closed before a complete frame arrived")]
    ConnectionClosed,

    #[error("signal handshake was rejected")]
    HandshakeRejected,

    #[error("signal request was rejected before execution")]
    SignalRequestRejected,

    #[error("signal request failed during execution")]
    SignalRequestFailed,
}

pub type Result<T> = std::result::Result<T, RepositoryLedgerError>;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRepositoryEvent {
    pub sequence: RepositoryEventSequence,
    pub notification: RepositoryReceiveHookNotification,
}

impl StoredRepositoryEvent {
    pub fn into_contract(self) -> RepositoryEvent {
        RepositoryEvent {
            sequence: self.sequence,
            notification: self.notification,
        }
    }
}

impl EngineRecord for StoredRepositoryEvent {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.sequence.into_u64()))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRepositoryRegistration {
    pub registration: RepositoryRegistration,
}

impl EngineRecord for StoredRepositoryRegistration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.registration.repository_name.as_str().to_owned())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredSpoolDirectoryPolicy {
    pub policy: SpoolDirectoryPolicy,
}

impl EngineRecord for StoredSpoolDirectoryPolicy {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("active")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredMirrorPolicy {
    pub policy: MirrorPolicy,
}

impl EngineRecord for StoredMirrorPolicy {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.policy.repository_name.as_str().to_owned())
    }
}

pub struct RepositoryLedgerStore {
    engine: Engine,
    events: TableReference<StoredRepositoryEvent>,
    registrations: TableReference<StoredRepositoryRegistration>,
    spool_directory_policy: TableReference<StoredSpoolDirectoryPolicy>,
    mirror_policies: TableReference<StoredMirrorPolicy>,
}

impl RepositoryLedgerStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(
            path.as_ref().to_path_buf(),
            SchemaVersion::new(SCHEMA_VERSION),
        ))?;
        let events = engine.register_table(TableDescriptor::new(REPOSITORY_EVENTS))?;
        let registrations =
            engine.register_table(TableDescriptor::new(REPOSITORY_REGISTRATIONS))?;
        let spool_directory_policy =
            engine.register_table(TableDescriptor::new(SPOOL_DIRECTORY_POLICY))?;
        let mirror_policies = engine.register_table(TableDescriptor::new(MIRROR_POLICIES))?;
        Ok(Self {
            engine,
            events,
            registrations,
            spool_directory_policy,
            mirror_policies,
        })
    }

    pub fn record_hook_notification(
        &self,
        notification: RepositoryReceiveHookNotification,
    ) -> Result<RepositoryEventRecorded> {
        let sequence = self.next_event_sequence()?;
        self.engine.assert(Assertion::new(
            self.events,
            StoredRepositoryEvent {
                sequence,
                notification,
            },
        ))?;
        Ok(RepositoryEventRecorded { sequence })
    }

    pub fn register_repository(
        &self,
        registration: RepositoryRegistration,
    ) -> Result<RepositoryCatalogListing> {
        let record = StoredRepositoryRegistration { registration };
        match self
            .engine
            .mutate(Mutation::new(self.registrations, record.clone()))
        {
            Ok(_) => {}
            Err(sema_engine::Error::RecordNotFound { .. }) => {
                self.engine
                    .assert(Assertion::new(self.registrations, record))?;
            }
            Err(error) => return Err(error.into()),
        }
        self.repository_catalog()
    }

    pub fn retire_repository(&self, request: RetireRepository) -> Result<RepositoryRetired> {
        self.engine.retract(Retraction::new(
            self.registrations,
            RecordKey::new(request.repository_name.as_str().to_owned()),
        ))?;
        Ok(RepositoryRetired {
            repository_name: request.repository_name,
        })
    }

    pub fn set_spool_directory_policy(
        &self,
        policy: SpoolDirectoryPolicy,
    ) -> Result<SpoolDirectoryPolicySet> {
        let record = StoredSpoolDirectoryPolicy {
            policy: policy.clone(),
        };
        match self
            .engine
            .mutate(Mutation::new(self.spool_directory_policy, record.clone()))
        {
            Ok(_) => {}
            Err(sema_engine::Error::RecordNotFound { .. }) => {
                self.engine
                    .assert(Assertion::new(self.spool_directory_policy, record))?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(SpoolDirectoryPolicySet { path: policy.path })
    }

    pub fn set_mirror_policy(&self, policy: MirrorPolicy) -> Result<MirrorPolicySet> {
        let record = StoredMirrorPolicy {
            policy: policy.clone(),
        };
        match self
            .engine
            .mutate(Mutation::new(self.mirror_policies, record.clone()))
        {
            Ok(_) => {}
            Err(sema_engine::Error::RecordNotFound { .. }) => {
                self.engine
                    .assert(Assertion::new(self.mirror_policies, record))?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(MirrorPolicySet {
            repository_name: policy.repository_name,
            target: policy.target,
        })
    }

    pub fn repository_events(&self, query: RepositoryEventQuery) -> Result<RepositoryEventListing> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.events))?;
        let mut events: Vec<RepositoryEvent> = snapshot
            .records()
            .iter()
            .cloned()
            .filter(|event| {
                if let Some(repository_name) = &query.repository_name {
                    event.notification.repository_name == *repository_name
                } else {
                    true
                }
            })
            .filter(|event| {
                if let Some(since_sequence) = query.since_sequence {
                    event.sequence > since_sequence
                } else {
                    true
                }
            })
            .map(StoredRepositoryEvent::into_contract)
            .collect();
        events.sort_by_key(|event| event.sequence);
        events.truncate(query.limit.into_u64() as usize);
        Ok(RepositoryEventListing { events })
    }

    pub fn repository_catalog(&self) -> Result<RepositoryCatalogListing> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::all(self.registrations))?;
        let mut repositories: Vec<RepositoryRegistration> = snapshot
            .records()
            .iter()
            .map(|stored| stored.registration.clone())
            .collect();
        repositories.sort_by(|left, right| {
            left.repository_name
                .as_str()
                .cmp(right.repository_name.as_str())
        });
        Ok(RepositoryCatalogListing { repositories })
    }

    fn next_event_sequence(&self) -> Result<RepositoryEventSequence> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.events))?;
        let next = snapshot
            .records()
            .iter()
            .map(|event| event.sequence.into_u64())
            .max()
            .unwrap_or(0)
            + 1;
        Ok(RepositoryEventSequence::new(next))
    }

    pub fn handle_ordinary_request(&self, request: ChannelRequest) -> ChannelReply {
        let checked = match request.into_checked() {
            Ok(checked) => checked,
            Err((reason, _request)) => return Reply::rejected(reason),
        };
        if checked.operations.len() != 1 {
            return Reply::rejected(RequestRejectionReason::Internal);
        }

        let operation = checked.operations.into_head();
        let verb = operation.verb;
        let operation_kind = operation.payload.operation_kind();
        match self.execute_ordinary_payload(operation.payload) {
            Ok(payload) => Reply::completed(NonEmpty::single(SubReply::Ok { verb, payload })),
            Err(error) => Reply::aborted(
                0,
                OperationFailureReason::DomainRejection,
                NonEmpty::single(SubReply::Failed {
                    verb,
                    reason: OperationFailureReason::DomainRejection,
                    detail: Some(RepositoryLedgerReply::RepositoryLedgerRequestUnimplemented(
                        RepositoryLedgerRequestUnimplemented {
                            operation: operation_kind,
                            reason: error.as_unimplemented_reason(),
                        },
                    )),
                }),
            ),
        }
    }

    pub fn handle_owner_request(
        &self,
        request: owner_signal_repository_ledger::ChannelRequest,
    ) -> owner_signal_repository_ledger::ChannelReply {
        let checked = match request.into_checked() {
            Ok(checked) => checked,
            Err((reason, _request)) => return Reply::rejected(reason),
        };
        if checked.operations.len() != 1 {
            return Reply::rejected(RequestRejectionReason::Internal);
        }

        let operation = checked.operations.into_head();
        let verb = operation.verb;
        let operation_kind = operation.payload.operation_kind();
        match self.execute_owner_payload(operation.payload) {
            Ok(payload) => Reply::completed(NonEmpty::single(SubReply::Ok { verb, payload })),
            Err(error) => Reply::aborted(
                0,
                OperationFailureReason::DomainRejection,
                NonEmpty::single(SubReply::Failed {
                    verb,
                    reason: OperationFailureReason::DomainRejection,
                    detail: Some(error.into_owner_unimplemented(operation_kind)),
                }),
            ),
        }
    }

    fn execute_ordinary_payload(
        &self,
        payload: RepositoryLedgerRequest,
    ) -> Result<RepositoryLedgerReply> {
        match payload {
            RepositoryLedgerRequest::RepositoryReceiveHookNotification(notification) => self
                .record_hook_notification(notification)
                .map(RepositoryLedgerReply::RepositoryEventRecorded),
            RepositoryLedgerRequest::RepositoryEventQuery(query) => self
                .repository_events(query)
                .map(RepositoryLedgerReply::RepositoryEventListing),
            RepositoryLedgerRequest::RepositoryCatalogQuery(query) => {
                let RepositoryCatalogListing { repositories } = self.repository_catalog()?;
                let _ = query;
                Ok(RepositoryLedgerReply::RepositoryCatalogListing(
                    RepositoryCatalogListing { repositories },
                ))
            }
        }
    }

    fn execute_owner_payload(
        &self,
        payload: owner_signal_repository_ledger::OwnerRepositoryLedgerRequest,
    ) -> Result<owner_signal_repository_ledger::OwnerRepositoryLedgerReply> {
        match payload {
            owner_signal_repository_ledger::OwnerRepositoryLedgerRequest::RegisterRepository(
                registration,
            ) => {
                let repository_name = registration.repository_name.clone();
                self.register_repository(registration)?;
                Ok(owner_signal_repository_ledger::OwnerRepositoryLedgerReply::RepositoryRegistered(
                    RepositoryRegistered { repository_name },
                ))
            }
            owner_signal_repository_ledger::OwnerRepositoryLedgerRequest::RetireRepository(
                request,
            ) => self
                .retire_repository(request)
                .map(owner_signal_repository_ledger::OwnerRepositoryLedgerReply::RepositoryRetired),
            owner_signal_repository_ledger::OwnerRepositoryLedgerRequest::SetSpoolDirectoryPolicy(
                policy,
            ) => self.set_spool_directory_policy(policy).map(
                owner_signal_repository_ledger::OwnerRepositoryLedgerReply::SpoolDirectoryPolicySet,
            ),
            owner_signal_repository_ledger::OwnerRepositoryLedgerRequest::SetMirrorPolicy(
                policy,
            ) => self
                .set_mirror_policy(policy)
                .map(owner_signal_repository_ledger::OwnerRepositoryLedgerReply::MirrorPolicySet),
        }
    }
}

impl RepositoryLedgerError {
    fn as_unimplemented_reason(&self) -> RepositoryLedgerUnimplementedReason {
        match self {
            Self::Engine(_) => RepositoryLedgerUnimplementedReason::StoreUnavailable,
            _ => RepositoryLedgerUnimplementedReason::NotInPrototypeScope,
        }
    }

    fn into_owner_unimplemented(
        self,
        operation: owner_signal_repository_ledger::OwnerRepositoryLedgerOperationKind,
    ) -> owner_signal_repository_ledger::OwnerRepositoryLedgerReply {
        let reason = match self {
            Self::Engine(_) => {
                owner_signal_repository_ledger::OwnerRepositoryLedgerUnimplementedReason::StoreUnavailable
            }
            _ => {
                owner_signal_repository_ledger::OwnerRepositoryLedgerUnimplementedReason::NotInPrototypeScope
            }
        };
        owner_signal_repository_ledger::OwnerRepositoryLedgerReply::OwnerRepositoryLedgerRequestUnimplemented(
            owner_signal_repository_ledger::OwnerRepositoryLedgerRequestUnimplemented {
                operation,
                reason,
            },
        )
    }
}
