//! Repository ledger runtime library.
//!
//! `repository-ledger-daemon` owns this store and exposes ordinary and
//! meta-signal sockets over it.

use std::future;
use std::path::{Path, PathBuf};

pub mod client;
pub mod configuration;
pub mod daemon;
pub mod daemon_command;
pub mod spool;

pub mod schema {
    #[rustfmt::skip]
    pub mod daemon;
}

pub use configuration::{Configuration, ConfigurationError};
pub use daemon::{
    RepositoryLedgerDaemonError, RepositoryLedgerEngine, RepositoryLedgerProcessDaemon,
};
pub use daemon_command::{RepositoryLedgerDaemonCommand, RepositoryLedgerDaemonConfigurationFile};
pub use schema::daemon::{ComponentDaemon, DaemonCommand, DaemonEntry, DaemonError, ListenerTier};

use meta_signal_repository_ledger::{
    MirrorPolicy, MirrorPolicySet, Operation as MetaOperation, Registered, Reply as MetaReply,
    Retired, Retirement, SpoolDirectoryPolicy, SpoolDirectoryPolicySet,
};
use nota_next::NotaDecodeError;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey, Retraction,
    SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_frame::{
    BatchErrorClassification, BatchFailureReason, CommitStatus, NonEmpty, Reply as FrameReply,
    RetryClassification, SubReply,
};
use signal_repository_ledger::{
    Catalog, CatalogListing, ChangedFile, ChangedFileListing, ChangedFiles, Commit, CommitListing,
    CommitMessages, CommitObservation, Event, EventListing, EventRecorded, EventSequence, Events,
    Name, Operation as LedgerOperation, PushObservation, Query, QueryLimit, QueryResult,
    ReceiveHookNotification, RecentRepositories, RecentRepositoriesListing, RecentRepository,
    Registration, Reply as LedgerReply, ReplyEnvelope as LedgerChannelReply,
    Request as LedgerChannelRequest, RequestUnimplemented as LedgerRequestUnimplemented, Timestamp,
    UnimplementedReason as LedgerUnimplementedReason,
};
use triad_runtime::{
    ContinuationExhausted, NextStep, NexusAction as TriadNexusAction, Runner, RunnerEngines,
};

const SCHEMA_VERSION: u32 = 1;
const REPOSITORY_EVENTS: TableName = TableName::new("repository_events");
const REPOSITORY_COMMITS: TableName = TableName::new("repository_commits");
const REPOSITORY_REGISTRATIONS: TableName = TableName::new("repository_registrations");
const SPOOL_DIRECTORY_POLICY: TableName = TableName::new("spool_directory_policy");
const MIRROR_POLICIES: TableName = TableName::new("mirror_policies");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sema-engine error: {0}")]
    Engine(#[from] sema_engine::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("signal frame error: {0}")]
    Frame(#[from] signal_frame::FrameError),

    #[error("runtime frame error: {0}")]
    RuntimeFrame(#[from] triad_runtime::FrameError),

    #[error("command line route error: {0}")]
    CommandLineRoute(#[from] signal_frame::CommandLineRouteError),

    #[error("NOTA decode error: {0}")]
    Nota(#[from] NotaDecodeError),

    #[error("argument: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),

    #[error("actor call failed: {detail}")]
    ActorCall { detail: String },

    #[error("daemon runtime failed: {detail}")]
    DaemonRuntime { detail: String },

    #[error("configuration archive decode failed")]
    ConfigurationArchiveDecode,

    #[error("configuration archive encode failed")]
    ConfigurationArchiveEncode,

    #[error("configuration read failed at {path}: {source}")]
    ConfigurationRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("configuration write failed at {path}: {source}")]
    ConfigurationWrite {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("expected exactly one argument")]
    ExpectedSingleArgument,

    #[error("flag-style arguments are not part of component binaries: {0}")]
    FlagArgument(String),

    #[error("unexpected signal frame for this socket")]
    UnexpectedFrame,

    #[error("signal handshake was rejected")]
    HandshakeRejected,

    #[error("signal request was rejected before execution")]
    SignalRequestRejected,

    #[error("signal request failed during execution")]
    SignalRequestFailed,

    #[error(
        "repository-ledger Nexus runner accepts one operation per atomic batch, received {operation_count}"
    )]
    UnsupportedAtomicBatch { operation_count: usize },

    #[error("Nexus replied to the wrong signal tier")]
    NexusReplyTierMismatch,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub sequence: EventSequence,
    pub notification: ReceiveHookNotification,
}

impl StoredEvent {
    pub fn into_contract(self) -> Event {
        Event {
            sequence: self.sequence,
            notification: self.notification,
        }
    }
}

impl EngineRecord for StoredEvent {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.sequence.into_u64()))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredCommit {
    pub repository_name: Name,
    pub received_at: Timestamp,
    pub sequence: EventSequence,
    pub commit: CommitObservation,
}

impl StoredCommit {
    pub fn into_contract(self) -> Commit {
        Commit {
            repository_name: self.repository_name,
            received_at: self.received_at,
            sequence: self.sequence,
            object_identifier: self.commit.object_identifier,
            ref_name: self.commit.ref_name,
            commit_timestamp: self.commit.commit_timestamp,
            message: self.commit.message,
        }
    }
}

impl EngineRecord for StoredCommit {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{:020}-{}-{}",
            self.sequence.into_u64(),
            self.commit.object_identifier.as_str(),
            self.commit.ref_name.as_str()
        ))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRegistration {
    pub registration: Registration,
}

impl EngineRecord for StoredRegistration {
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum LedgerSemaWriteInput {
    RecordHookNotification(ReceiveHookNotification),
    RecordPushObservation(PushObservation),
    RegisterRepository(Registration),
    RetireRepository(Retirement),
    SetSpoolDirectoryPolicy(SpoolDirectoryPolicy),
    SetMirrorPolicy(MirrorPolicy),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LedgerSemaReadInput {
    ReadQuery(Query),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LedgerSemaWriteOutput {
    EventRecorded(EventRecorded),
    Registered(Registered),
    Retired(Retired),
    SpoolDirectoryPolicySet(SpoolDirectoryPolicySet),
    MirrorPolicySet(MirrorPolicySet),
    WriteRejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LedgerSemaReadOutput {
    QueryResult(QueryResult),
    ReadMiss,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RepositoryLedgerSignalInput {
    Ordinary(LedgerOperation),
    Meta(MetaOperation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RepositoryLedgerSignalOutput {
    Ordinary(LedgerReply),
    Meta(MetaReply),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RepositoryLedgerNexusWork {
    SignalArrived(RepositoryLedgerSignalInput),
    SemaWriteCompleted(LedgerSemaWriteOutput),
    SemaReadCompleted(LedgerSemaReadOutput),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RepositoryLedgerNexusAction {
    CommandSemaWrite(LedgerSemaWriteInput),
    CommandSemaRead(LedgerSemaReadInput),
    ReplyToSignal(RepositoryLedgerSignalOutput),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepositoryLedgerSignalTier {
    Ordinary,
    Meta,
}

struct RepositoryLedgerNexusEngines<'store> {
    store: &'store Store,
    signal_tier: Option<RepositoryLedgerSignalTier>,
    ordinary_failure_reply: Option<LedgerReply>,
    meta_failure_reply: Option<MetaReply>,
    last_error: Option<Error>,
}

impl triad_runtime::SemaWriteInput for LedgerSemaWriteInput {}

impl triad_runtime::SemaWriteOutput for LedgerSemaWriteOutput {}

impl triad_runtime::SemaReadInput for LedgerSemaReadInput {}

impl triad_runtime::SemaReadOutput for LedgerSemaReadOutput {}

impl triad_runtime::NexusWork for RepositoryLedgerNexusWork {}

impl TriadNexusAction for RepositoryLedgerNexusAction {
    type Reply = RepositoryLedgerSignalOutput;
    type SemaWrite = LedgerSemaWriteInput;
    type SemaRead = LedgerSemaReadInput;
    type Effect = std::convert::Infallible;
    type Work = RepositoryLedgerNexusWork;

    fn into_next_step(self) -> triad_runtime::NexusActionNextStep<Self> {
        match self {
            Self::CommandSemaWrite(input) => NextStep::SemaWrite(input),
            Self::CommandSemaRead(input) => NextStep::SemaRead(input),
            Self::ReplyToSignal(output) => NextStep::Reply(output),
        }
    }
}

impl RepositoryLedgerNexusEngines<'_> {
    fn new(store: &Store) -> RepositoryLedgerNexusEngines<'_> {
        RepositoryLedgerNexusEngines {
            store,
            signal_tier: None,
            ordinary_failure_reply: None,
            meta_failure_reply: None,
            last_error: None,
        }
    }

    fn take_last_error(&mut self) -> Option<Error> {
        self.last_error.take()
    }

    fn action_for_signal_input(
        &mut self,
        input: RepositoryLedgerSignalInput,
    ) -> RepositoryLedgerNexusAction {
        match input {
            RepositoryLedgerSignalInput::Ordinary(operation) => {
                self.signal_tier = Some(RepositoryLedgerSignalTier::Ordinary);
                self.ordinary_failure_reply = Some(Self::ordinary_unimplemented_reply(&operation));
                match operation {
                    LedgerOperation::Receive(notification) => {
                        RepositoryLedgerNexusAction::CommandSemaWrite(
                            LedgerSemaWriteInput::RecordHookNotification(notification),
                        )
                    }
                    LedgerOperation::Observe(observation) => {
                        RepositoryLedgerNexusAction::CommandSemaWrite(
                            LedgerSemaWriteInput::RecordPushObservation(observation),
                        )
                    }
                    LedgerOperation::Query(query) => RepositoryLedgerNexusAction::CommandSemaRead(
                        LedgerSemaReadInput::ReadQuery(query),
                    ),
                }
            }
            RepositoryLedgerSignalInput::Meta(operation) => {
                self.signal_tier = Some(RepositoryLedgerSignalTier::Meta);
                self.meta_failure_reply = Some(Self::meta_unimplemented_reply(&operation));
                match operation {
                    MetaOperation::Register(registration) => {
                        RepositoryLedgerNexusAction::CommandSemaWrite(
                            LedgerSemaWriteInput::RegisterRepository(registration),
                        )
                    }
                    MetaOperation::Retire(retirement) => {
                        RepositoryLedgerNexusAction::CommandSemaWrite(
                            LedgerSemaWriteInput::RetireRepository(retirement),
                        )
                    }
                    MetaOperation::SetSpoolDirectory(policy) => {
                        RepositoryLedgerNexusAction::CommandSemaWrite(
                            LedgerSemaWriteInput::SetSpoolDirectoryPolicy(policy),
                        )
                    }
                    MetaOperation::SetMirror(policy) => {
                        RepositoryLedgerNexusAction::CommandSemaWrite(
                            LedgerSemaWriteInput::SetMirrorPolicy(policy),
                        )
                    }
                }
            }
        }
    }

    fn action_for_sema_write_output(
        &self,
        output: LedgerSemaWriteOutput,
    ) -> RepositoryLedgerNexusAction {
        let signal_output = match output {
            LedgerSemaWriteOutput::EventRecorded(recorded) => {
                RepositoryLedgerSignalOutput::Ordinary(LedgerReply::EventRecorded(recorded))
            }
            LedgerSemaWriteOutput::Registered(registered) => {
                RepositoryLedgerSignalOutput::Meta(MetaReply::Registered(registered))
            }
            LedgerSemaWriteOutput::Retired(retired) => {
                RepositoryLedgerSignalOutput::Meta(MetaReply::Retired(retired))
            }
            LedgerSemaWriteOutput::SpoolDirectoryPolicySet(policy) => {
                RepositoryLedgerSignalOutput::Meta(MetaReply::SpoolDirectoryPolicySet(policy))
            }
            LedgerSemaWriteOutput::MirrorPolicySet(policy) => {
                RepositoryLedgerSignalOutput::Meta(MetaReply::MirrorPolicySet(policy))
            }
            LedgerSemaWriteOutput::WriteRejected => self.failure_signal_output(),
        };
        RepositoryLedgerNexusAction::ReplyToSignal(signal_output)
    }

    fn action_for_sema_read_output(
        &self,
        output: LedgerSemaReadOutput,
    ) -> RepositoryLedgerNexusAction {
        let signal_output = match output {
            LedgerSemaReadOutput::QueryResult(result) => {
                RepositoryLedgerSignalOutput::Ordinary(LedgerReply::QueryResult(result))
            }
            LedgerSemaReadOutput::ReadMiss => self.failure_signal_output(),
        };
        RepositoryLedgerNexusAction::ReplyToSignal(signal_output)
    }

    fn failure_signal_output(&self) -> RepositoryLedgerSignalOutput {
        match self
            .signal_tier
            .unwrap_or(RepositoryLedgerSignalTier::Ordinary)
        {
            RepositoryLedgerSignalTier::Ordinary => RepositoryLedgerSignalOutput::Ordinary(
                self.ordinary_failure_reply
                    .clone()
                    .unwrap_or_else(Self::ordinary_unknown_failure_reply),
            ),
            RepositoryLedgerSignalTier::Meta => RepositoryLedgerSignalOutput::Meta(
                self.meta_failure_reply
                    .clone()
                    .unwrap_or_else(Self::meta_unknown_failure_reply),
            ),
        }
    }

    fn ordinary_unimplemented_reply(operation: &LedgerOperation) -> LedgerReply {
        let query = match operation {
            LedgerOperation::Query(query) => Some(query.kind()),
            LedgerOperation::Receive(_) | LedgerOperation::Observe(_) => None,
        };
        LedgerReply::RequestUnimplemented(LedgerRequestUnimplemented {
            operation: operation.operation_kind(),
            query,
            reason: LedgerUnimplementedReason::StoreUnavailable,
        })
    }

    fn ordinary_unknown_failure_reply() -> LedgerReply {
        LedgerReply::RequestUnimplemented(LedgerRequestUnimplemented {
            operation: signal_repository_ledger::OperationKind::Receive,
            query: None,
            reason: LedgerUnimplementedReason::StoreUnavailable,
        })
    }

    fn meta_unimplemented_reply(operation: &MetaOperation) -> MetaReply {
        MetaReply::RequestUnimplemented(meta_signal_repository_ledger::RequestUnimplemented {
            operation: operation.operation_kind(),
            reason: meta_signal_repository_ledger::UnimplementedReason::StoreUnavailable,
        })
    }

    fn meta_unknown_failure_reply() -> MetaReply {
        MetaReply::RequestUnimplemented(meta_signal_repository_ledger::RequestUnimplemented {
            operation: meta_signal_repository_ledger::OperationKind::Register,
            reason: meta_signal_repository_ledger::UnimplementedReason::StoreUnavailable,
        })
    }
}

pub struct Store {
    engine: Engine,
    events: TableReference<StoredEvent>,
    commits: TableReference<StoredCommit>,
    registrations: TableReference<StoredRegistration>,
    spool_directory_policy: TableReference<StoredSpoolDirectoryPolicy>,
    mirror_policies: TableReference<StoredMirrorPolicy>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(
            path.as_ref().to_path_buf(),
            SchemaVersion::new(SCHEMA_VERSION),
        ))?;
        let events = engine.register_table(TableDescriptor::new(REPOSITORY_EVENTS))?;
        let commits = engine.register_table(TableDescriptor::new(REPOSITORY_COMMITS))?;
        let registrations =
            engine.register_table(TableDescriptor::new(REPOSITORY_REGISTRATIONS))?;
        let spool_directory_policy =
            engine.register_table(TableDescriptor::new(SPOOL_DIRECTORY_POLICY))?;
        let mirror_policies = engine.register_table(TableDescriptor::new(MIRROR_POLICIES))?;
        Ok(Self {
            engine,
            events,
            commits,
            registrations,
            spool_directory_policy,
            mirror_policies,
        })
    }

    pub fn record_hook_notification(
        &self,
        notification: ReceiveHookNotification,
    ) -> Result<EventRecorded> {
        let sequence = self.next_event_sequence()?;
        self.engine.assert(Assertion::new(
            self.events,
            StoredEvent {
                sequence,
                notification,
            },
        ))?;
        Ok(EventRecorded { sequence })
    }

    pub fn record_push_observation(&self, observation: PushObservation) -> Result<EventRecorded> {
        let PushObservation {
            notification,
            commits,
        } = observation;
        let repository_name = notification.repository_name.clone();
        let received_at = notification.received_at.clone();
        let recorded = self.record_hook_notification(notification)?;
        for commit in commits {
            self.engine.assert(Assertion::new(
                self.commits,
                StoredCommit {
                    repository_name: repository_name.clone(),
                    received_at: received_at.clone(),
                    sequence: recorded.sequence,
                    commit,
                },
            ))?;
        }
        Ok(recorded)
    }

    pub fn register_repository(&self, registration: Registration) -> Result<CatalogListing> {
        let record = StoredRegistration { registration };
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

    pub fn retire_repository(&self, request: Retirement) -> Result<Retired> {
        self.engine.retract(Retraction::new(
            self.registrations,
            RecordKey::new(request.repository_name.as_str().to_owned()),
        ))?;
        Ok(Retired {
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

    pub fn repository_events(&self, query: Events) -> Result<EventListing> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.events))?;
        let mut events: Vec<Event> = snapshot
            .records()
            .iter()
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
            .cloned()
            .map(StoredEvent::into_contract)
            .collect();
        events.sort_by_key(|event| event.sequence);
        events.truncate(query.limit.into_u64() as usize);
        Ok(EventListing { events })
    }

    pub fn recent_repositories(
        &self,
        query: RecentRepositories,
    ) -> Result<RecentRepositoriesListing> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.events))?;
        let mut repositories: Vec<RecentRepository> = Vec::new();
        for event in snapshot.records() {
            if let Some(since) = &query.since_received_at
                && event.notification.received_at.as_str() < since.as_str()
            {
                continue;
            }
            let Some(existing) = repositories
                .iter_mut()
                .find(|candidate| candidate.repository_name == event.notification.repository_name)
            else {
                repositories.push(RecentRepository {
                    repository_name: event.notification.repository_name.clone(),
                    latest_received_at: event.notification.received_at.clone(),
                    latest_sequence: event.sequence,
                    push_count: QueryLimit::new(1),
                });
                continue;
            };
            existing.push_count = QueryLimit::new(existing.push_count.into_u64() + 1);
            if event.notification.received_at.as_str() > existing.latest_received_at.as_str()
                || event.sequence > existing.latest_sequence
            {
                existing.latest_received_at = event.notification.received_at.clone();
                existing.latest_sequence = event.sequence;
            }
        }
        repositories.sort_by(|left, right| {
            right
                .latest_received_at
                .as_str()
                .cmp(left.latest_received_at.as_str())
                .then_with(|| right.latest_sequence.cmp(&left.latest_sequence))
                .then_with(|| {
                    left.repository_name
                        .as_str()
                        .cmp(right.repository_name.as_str())
                })
        });
        repositories.truncate(query.limit.into_u64() as usize);
        Ok(RecentRepositoriesListing { repositories })
    }

    pub fn changed_files(&self, query: ChangedFiles) -> Result<ChangedFileListing> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.commits))?;
        let mut files = Vec::new();
        for commit in snapshot.records() {
            if !self.commit_matches_common_filters(
                commit,
                query.repository_name.as_ref(),
                query.since_received_at.as_ref(),
                query.until_received_at.as_ref(),
            ) {
                continue;
            }
            for file in &commit.commit.changed_files {
                if let Some(search) = &query.path_contains
                    && !contains_case_insensitive(file.path.as_str(), search.as_str())
                {
                    continue;
                }
                files.push(ChangedFile {
                    repository_name: commit.repository_name.clone(),
                    received_at: commit.received_at.clone(),
                    sequence: commit.sequence,
                    commit_object_identifier: commit.commit.object_identifier.clone(),
                    ref_name: commit.commit.ref_name.clone(),
                    status: file.status.clone(),
                    path: file.path.clone(),
                    old_path: file.old_path.clone(),
                });
            }
        }
        files.sort_by(|left, right| {
            right
                .received_at
                .as_str()
                .cmp(left.received_at.as_str())
                .then_with(|| right.sequence.cmp(&left.sequence))
                .then_with(|| left.path.as_str().cmp(right.path.as_str()))
        });
        files.truncate(query.limit.into_u64() as usize);
        Ok(ChangedFileListing { files })
    }

    pub fn commit_messages(&self, query: CommitMessages) -> Result<CommitListing> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.commits))?;
        let mut commits: Vec<Commit> = snapshot
            .records()
            .iter()
            .filter(|commit| {
                self.commit_matches_common_filters(
                    commit,
                    query.repository_name.as_ref(),
                    query.since_received_at.as_ref(),
                    query.until_received_at.as_ref(),
                )
            })
            .filter(|commit| {
                if let Some(search) = &query.message_contains {
                    contains_case_insensitive(commit.commit.message.as_str(), search.as_str())
                } else {
                    true
                }
            })
            .cloned()
            .map(StoredCommit::into_contract)
            .collect();
        commits.sort_by(|left, right| {
            right
                .received_at
                .as_str()
                .cmp(left.received_at.as_str())
                .then_with(|| right.sequence.cmp(&left.sequence))
                .then_with(|| {
                    left.object_identifier
                        .as_str()
                        .cmp(right.object_identifier.as_str())
                })
        });
        commits.truncate(query.limit.into_u64() as usize);
        Ok(CommitListing { commits })
    }

    pub fn repository_catalog(&self) -> Result<CatalogListing> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::all(self.registrations))?;
        let mut repositories: Vec<Registration> = snapshot
            .records()
            .iter()
            .map(|stored| stored.registration.clone())
            .collect();
        repositories.sort_by(|left, right| {
            left.repository_name
                .as_str()
                .cmp(right.repository_name.as_str())
        });
        Ok(CatalogListing { repositories })
    }

    fn next_event_sequence(&self) -> Result<EventSequence> {
        let snapshot = self.engine.match_records(QueryPlan::all(self.events))?;
        let next = snapshot
            .records()
            .iter()
            .map(|event| event.sequence.into_u64())
            .max()
            .unwrap_or(0)
            + 1;
        Ok(EventSequence::new(next))
    }

    fn commit_matches_common_filters(
        &self,
        commit: &StoredCommit,
        repository_name: Option<&Name>,
        since_received_at: Option<&Timestamp>,
        until_received_at: Option<&Timestamp>,
    ) -> bool {
        if let Some(repository_name) = repository_name
            && commit.repository_name != *repository_name
        {
            return false;
        }
        if let Some(since) = since_received_at
            && commit.received_at.as_str() < since.as_str()
        {
            return false;
        }
        if let Some(until) = until_received_at
            && commit.received_at.as_str() > until.as_str()
        {
            return false;
        }
        true
    }

    pub async fn handle_ordinary_request(
        &self,
        request: LedgerChannelRequest,
    ) -> LedgerChannelReply {
        let operation_count = request.payloads().len();
        if operation_count != 1 {
            return self.batch_aborted_reply(
                &Error::UnsupportedAtomicBatch { operation_count },
                operation_count,
            );
        }
        let operation = request.payloads.into_head();
        let mut engines = RepositoryLedgerNexusEngines::new(self);
        let output = Runner::default()
            .drive(
                &mut engines,
                RepositoryLedgerNexusWork::SignalArrived(RepositoryLedgerSignalInput::Ordinary(
                    operation,
                )),
            )
            .await;
        if let Some(error) = engines.take_last_error() {
            return self.batch_aborted_reply(&error, operation_count);
        }
        match output {
            RepositoryLedgerSignalOutput::Ordinary(reply) => {
                FrameReply::committed(NonEmpty::single(SubReply::Ok(reply)))
            }
            RepositoryLedgerSignalOutput::Meta(_) => {
                self.batch_aborted_reply(&Error::NexusReplyTierMismatch, operation_count)
            }
        }
    }

    pub async fn handle_meta_request(
        &self,
        request: meta_signal_repository_ledger::ChannelRequest,
    ) -> meta_signal_repository_ledger::ChannelReply {
        let operation_count = request.payloads().len();
        if operation_count != 1 {
            return self.batch_aborted_reply(
                &Error::UnsupportedAtomicBatch { operation_count },
                operation_count,
            );
        }
        let operation = request.payloads.into_head();
        let mut engines = RepositoryLedgerNexusEngines::new(self);
        let output = Runner::default()
            .drive(
                &mut engines,
                RepositoryLedgerNexusWork::SignalArrived(RepositoryLedgerSignalInput::Meta(
                    operation,
                )),
            )
            .await;
        if let Some(error) = engines.take_last_error() {
            return self.batch_aborted_reply(&error, operation_count);
        }
        match output {
            RepositoryLedgerSignalOutput::Meta(reply) => {
                FrameReply::committed(NonEmpty::single(SubReply::Ok(reply)))
            }
            RepositoryLedgerSignalOutput::Ordinary(_) => {
                self.batch_aborted_reply(&Error::NexusReplyTierMismatch, operation_count)
            }
        }
    }

    fn apply_sema_write(&self, input: LedgerSemaWriteInput) -> Result<LedgerSemaWriteOutput> {
        match input {
            LedgerSemaWriteInput::RecordHookNotification(notification) => self
                .record_hook_notification(notification)
                .map(LedgerSemaWriteOutput::EventRecorded),
            LedgerSemaWriteInput::RecordPushObservation(observation) => self
                .record_push_observation(observation)
                .map(LedgerSemaWriteOutput::EventRecorded),
            LedgerSemaWriteInput::RegisterRepository(registration) => {
                let repository_name = registration.repository_name.clone();
                self.register_repository(registration)?;
                Ok(LedgerSemaWriteOutput::Registered(Registered {
                    repository_name,
                }))
            }
            LedgerSemaWriteInput::RetireRepository(request) => self
                .retire_repository(request)
                .map(LedgerSemaWriteOutput::Retired),
            LedgerSemaWriteInput::SetSpoolDirectoryPolicy(policy) => self
                .set_spool_directory_policy(policy)
                .map(LedgerSemaWriteOutput::SpoolDirectoryPolicySet),
            LedgerSemaWriteInput::SetMirrorPolicy(policy) => self
                .set_mirror_policy(policy)
                .map(LedgerSemaWriteOutput::MirrorPolicySet),
        }
    }

    fn observe_sema_read(&self, input: LedgerSemaReadInput) -> Result<LedgerSemaReadOutput> {
        match input {
            LedgerSemaReadInput::ReadQuery(query) => self
                .execute_query(query)
                .map(LedgerSemaReadOutput::QueryResult),
        }
    }

    fn batch_aborted_reply<ReplyPayload>(
        &self,
        error: &Error,
        operation_count: usize,
    ) -> FrameReply<ReplyPayload> {
        let mut per_operation = NonEmpty::single(SubReply::Invalidated);
        for _ in 1..operation_count {
            per_operation.push(SubReply::Invalidated);
        }
        FrameReply::batch_aborted(
            error.batch_failure_reason(),
            error.retry_classification(),
            error.commit_status(),
            per_operation,
        )
    }

    fn execute_query(&self, query: Query) -> Result<QueryResult> {
        match query {
            Query::Events(query) => self.repository_events(query).map(QueryResult::Events),
            Query::RecentRepositories(query) => self
                .recent_repositories(query)
                .map(QueryResult::RecentRepositories),
            Query::ChangedFiles(query) => self.changed_files(query).map(QueryResult::ChangedFiles),
            Query::CommitMessages(query) => self.commit_messages(query).map(QueryResult::Commits),
            Query::Catalog(query) => {
                let CatalogListing { repositories } = self.repository_catalog()?;
                let Catalog = query;
                Ok(QueryResult::Catalog(CatalogListing { repositories }))
            }
        }
    }
}

impl RunnerEngines for RepositoryLedgerNexusEngines<'_> {
    type Reply = RepositoryLedgerSignalOutput;
    type SemaWrite = LedgerSemaWriteInput;
    type SemaRead = LedgerSemaReadInput;
    type Effect = std::convert::Infallible;
    type Work = RepositoryLedgerNexusWork;

    fn decide_next_step(
        &mut self,
        work: Self::Work,
    ) -> NextStep<Self::Reply, Self::SemaWrite, Self::SemaRead, Self::Effect, Self::Work> {
        let action = match work {
            RepositoryLedgerNexusWork::SignalArrived(input) => self.action_for_signal_input(input),
            RepositoryLedgerNexusWork::SemaWriteCompleted(output) => {
                self.action_for_sema_write_output(output)
            }
            RepositoryLedgerNexusWork::SemaReadCompleted(output) => {
                self.action_for_sema_read_output(output)
            }
        };
        TriadNexusAction::into_next_step(action)
    }

    fn apply_sema_write(
        &mut self,
        write: Self::SemaWrite,
    ) -> impl future::Future<Output = Self::Work> + Send + '_ {
        let output = match self.store.apply_sema_write(write) {
            Ok(output) => output,
            Err(error) => {
                self.last_error = Some(error);
                LedgerSemaWriteOutput::WriteRejected
            }
        };
        future::ready(RepositoryLedgerNexusWork::SemaWriteCompleted(output))
    }

    fn observe_sema_read(
        &mut self,
        read: Self::SemaRead,
    ) -> impl future::Future<Output = Self::Work> + Send + '_ {
        let output = match self.store.observe_sema_read(read) {
            Ok(output) => output,
            Err(error) => {
                self.last_error = Some(error);
                LedgerSemaReadOutput::ReadMiss
            }
        };
        future::ready(RepositoryLedgerNexusWork::SemaReadCompleted(output))
    }

    async fn run_effect(&mut self, effect: Self::Effect) -> Self::Work {
        match effect {}
    }

    fn budget_exhausted_reply(&self, _exhausted: ContinuationExhausted) -> Self::Reply {
        self.failure_signal_output()
    }
}

impl signal_frame::BatchErrorClassification for Error {
    fn batch_failure_reason(&self) -> BatchFailureReason {
        match self {
            Self::Io(_)
            | Self::RuntimeFrame(_)
            | Self::ActorCall { .. }
            | Self::DaemonRuntime { .. } => BatchFailureReason::EngineUnavailable,
            _ => BatchFailureReason::EngineRejected,
        }
    }

    fn retry_classification(&self) -> RetryClassification {
        match self {
            Self::Io(_)
            | Self::RuntimeFrame(_)
            | Self::ActorCall { .. }
            | Self::DaemonRuntime { .. }
            | Self::Engine(_) => RetryClassification::Unknown,
            _ => RetryClassification::NotRetryable,
        }
    }

    fn commit_status(&self) -> CommitStatus {
        match self {
            Self::Engine(_)
            | Self::Io(_)
            | Self::RuntimeFrame(_)
            | Self::ActorCall { .. }
            | Self::DaemonRuntime { .. } => CommitStatus::Unknown,
            _ => CommitStatus::NotCommitted,
        }
    }
}

impl Error {
    pub fn command_line_route(error: signal_frame::CommandLineRouteError) -> Self {
        Self::CommandLineRoute(error)
    }
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}
