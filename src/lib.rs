//! Repository ledger runtime library.
//!
//! `repository-ledger-daemon` owns this store and exposes ordinary and
//! meta-signal sockets over it.

use std::future;
use std::path::Path;

pub mod client;
pub mod daemon;
pub mod frame_io;
pub mod spool;

use meta_signal_repository_ledger::{
    MirrorPolicy, MirrorPolicySet, Operation as MetaOperation, Registered, Reply as MetaReply,
    Retired, Retirement, SpoolDirectoryPolicy, SpoolDirectoryPolicySet,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey, Retraction,
    TableDescriptor, TableName, TableReference,
};
use signal_executor::{
    BatchEffects, BatchPlan, CommandEffect, CommandExecutor, Executor, Lowering, ObserverSet,
    OperationEffects, OperationPlan,
};
use signal_frame::{BatchFailureReason, CommitStatus, NonEmpty, RetryClassification};
use signal_repository_ledger::{
    Catalog, CatalogListing, ChangedFile, ChangedFileListing, ChangedFiles, Commit, CommitListing,
    CommitMessages, CommitObservation, Event, EventListing, EventRecorded, EventSequence, Events,
    Name, Operation as LedgerOperation, PushObservation, Query, QueryLimit, QueryResult,
    ReceiveHookNotification, RecentRepositories, RecentRepositoriesListing, RecentRepository,
    Registration, Reply as LedgerReply, ReplyEnvelope as LedgerChannelReply,
    Request as LedgerChannelRequest, Timestamp,
};
use signal_sema::{SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

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

    #[error("command line route error: {0}")]
    CommandLineRoute(#[from] signal_frame::CommandLineRouteError),

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

    #[error(
        "signal executor accepted only one repository-ledger operation per atomic batch, received {operation_count}"
    )]
    UnsupportedAtomicBatch { operation_count: usize },

    #[error(
        "signal executor accepted only one command per operation plan, received {command_count}"
    )]
    UnsupportedAtomicOperationPlan { command_count: usize },
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
pub enum LedgerCommand {
    RecordHookNotification(ReceiveHookNotification),
    RecordPushObservation(PushObservation),
    ReadQuery(Query),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerEffect {
    EventRecorded(EventRecorded),
    QueryResult(QueryResult),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaCommand {
    RegisterRepository(Registration),
    RetireRepository(Retirement),
    SetSpoolDirectoryPolicy(SpoolDirectoryPolicy),
    SetMirrorPolicy(MirrorPolicy),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaEffect {
    Registered(Registered),
    Retired(Retired),
    SpoolDirectoryPolicySet(SpoolDirectoryPolicySet),
    MirrorPolicySet(MirrorPolicySet),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LedgerLowering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetaLowering;

struct LedgerCommandExecutor<'store> {
    store: &'store Store,
}

struct MetaCommandExecutor<'store> {
    store: &'store Store,
}

impl LedgerCommand {
    fn from_operation(operation: LedgerOperation) -> Self {
        match operation {
            LedgerOperation::Receive(notification) => Self::RecordHookNotification(notification),
            LedgerOperation::Observe(observation) => Self::RecordPushObservation(observation),
            LedgerOperation::Query(query) => Self::ReadQuery(query),
        }
    }
}

impl LedgerEffect {
    fn into_reply(self) -> LedgerReply {
        match self {
            Self::EventRecorded(recorded) => LedgerReply::EventRecorded(recorded),
            Self::QueryResult(result) => LedgerReply::QueryResult(result),
        }
    }
}

impl MetaCommand {
    fn from_operation(operation: MetaOperation) -> Self {
        match operation {
            MetaOperation::Register(registration) => Self::RegisterRepository(registration),
            MetaOperation::Retire(retirement) => Self::RetireRepository(retirement),
            MetaOperation::SetSpoolDirectory(policy) => Self::SetSpoolDirectoryPolicy(policy),
            MetaOperation::SetMirror(policy) => Self::SetMirrorPolicy(policy),
        }
    }
}

impl MetaEffect {
    fn into_reply(self) -> MetaReply {
        match self {
            Self::Registered(registered) => MetaReply::Registered(registered),
            Self::Retired(retired) => MetaReply::Retired(retired),
            Self::SpoolDirectoryPolicySet(policy) => MetaReply::SpoolDirectoryPolicySet(policy),
            Self::MirrorPolicySet(policy) => MetaReply::MirrorPolicySet(policy),
        }
    }
}

impl ToSemaOperation for LedgerCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::RecordHookNotification(_) | Self::RecordPushObservation(_) => {
                SemaOperation::Assert
            }
            Self::ReadQuery(_) => SemaOperation::Match,
        }
    }
}

impl ToSemaOutcome for LedgerEffect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::EventRecorded(_) => SemaOutcome::Asserted,
            Self::QueryResult(_) => SemaOutcome::Matched,
        }
    }
}

impl ToSemaOperation for MetaCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::RegisterRepository(_)
            | Self::SetSpoolDirectoryPolicy(_)
            | Self::SetMirrorPolicy(_) => SemaOperation::Mutate,
            Self::RetireRepository(_) => SemaOperation::Retract,
        }
    }
}

impl ToSemaOutcome for MetaEffect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::Registered(_) | Self::SpoolDirectoryPolicySet(_) | Self::MirrorPolicySet(_) => {
                SemaOutcome::Mutated
            }
            Self::Retired(_) => SemaOutcome::Retracted,
        }
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
            if let Some(since) = &query.since_received_at {
                if event.notification.received_at.as_str() < since.as_str() {
                    continue;
                }
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
                if let Some(search) = &query.path_contains {
                    if !contains_case_insensitive(file.path.as_str(), search.as_str()) {
                        continue;
                    }
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
        if let Some(repository_name) = repository_name {
            if commit.repository_name != *repository_name {
                return false;
            }
        }
        if let Some(since) = since_received_at {
            if commit.received_at.as_str() < since.as_str() {
                return false;
            }
        }
        if let Some(until) = until_received_at {
            if commit.received_at.as_str() > until.as_str() {
                return false;
            }
        }
        true
    }

    pub fn handle_ordinary_request(&self, request: LedgerChannelRequest) -> LedgerChannelReply {
        let command_executor = LedgerCommandExecutor { store: self };
        let observers = ObserverSet::no_op();
        let mut executor = Executor::new(LedgerLowering, command_executor, observers);
        futures_executor::block_on(executor.execute(request))
    }

    pub fn handle_meta_request(
        &self,
        request: meta_signal_repository_ledger::ChannelRequest,
    ) -> meta_signal_repository_ledger::ChannelReply {
        let command_executor = MetaCommandExecutor { store: self };
        let observers = ObserverSet::no_op();
        let mut executor = Executor::new(MetaLowering, command_executor, observers);
        futures_executor::block_on(executor.execute(request))
    }

    fn execute_ledger_command(&self, command: LedgerCommand) -> Result<LedgerEffect> {
        match command {
            LedgerCommand::RecordHookNotification(notification) => self
                .record_hook_notification(notification)
                .map(LedgerEffect::EventRecorded),
            LedgerCommand::RecordPushObservation(observation) => self
                .record_push_observation(observation)
                .map(LedgerEffect::EventRecorded),
            LedgerCommand::ReadQuery(query) => {
                self.execute_query(query).map(LedgerEffect::QueryResult)
            }
        }
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

    fn execute_meta_command(&self, command: MetaCommand) -> Result<MetaEffect> {
        match command {
            MetaCommand::RegisterRepository(registration) => {
                let repository_name = registration.repository_name.clone();
                self.register_repository(registration)?;
                Ok(MetaEffect::Registered(Registered { repository_name }))
            }
            MetaCommand::RetireRepository(request) => {
                self.retire_repository(request).map(MetaEffect::Retired)
            }
            MetaCommand::SetSpoolDirectoryPolicy(policy) => self
                .set_spool_directory_policy(policy)
                .map(MetaEffect::SpoolDirectoryPolicySet),
            MetaCommand::SetMirrorPolicy(policy) => self
                .set_mirror_policy(policy)
                .map(MetaEffect::MirrorPolicySet),
        }
    }
}

impl Lowering for LedgerLowering {
    type Operation = LedgerOperation;
    type Reply = LedgerReply;
    type Command = LedgerCommand;
    type ComponentEffect = LedgerEffect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> std::result::Result<OperationPlan<Self::Command>, Self::Reply> {
        Ok(OperationPlan::single(LedgerCommand::from_operation(
            operation.clone(),
        )))
    }

    fn reply_from_effects(
        &self,
        _operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply {
        effects
            .component_effects()
            .last()
            .expect("repository-ledger ledger operation effects are non-empty")
            .clone()
            .into_reply()
    }
}

impl Lowering for MetaLowering {
    type Operation = MetaOperation;
    type Reply = MetaReply;
    type Command = MetaCommand;
    type ComponentEffect = MetaEffect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> std::result::Result<OperationPlan<Self::Command>, Self::Reply> {
        Ok(OperationPlan::single(MetaCommand::from_operation(
            operation.clone(),
        )))
    }

    fn reply_from_effects(
        &self,
        _operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply {
        effects
            .component_effects()
            .last()
            .expect("repository-ledger meta operation effects are non-empty")
            .clone()
            .into_reply()
    }
}

impl<'store> LedgerCommandExecutor<'store> {
    fn execute_atomic_batch_synchronously(
        &self,
        plan: BatchPlan<LedgerCommand>,
    ) -> Result<BatchEffects<LedgerCommand, LedgerEffect>> {
        let operation_count = plan.operations().len();
        if operation_count != 1 {
            return Err(Error::UnsupportedAtomicBatch { operation_count });
        }
        let operation_plan = plan.into_operations().into_head();
        let command = single_command_from_operation_plan(operation_plan)?;
        let effect = self.store.execute_ledger_command(command.clone())?;
        Ok(BatchEffects::single(OperationEffects::new(
            NonEmpty::single(CommandEffect::new(command, effect)),
        )))
    }
}

impl<'store> MetaCommandExecutor<'store> {
    fn execute_atomic_batch_synchronously(
        &self,
        plan: BatchPlan<MetaCommand>,
    ) -> Result<BatchEffects<MetaCommand, MetaEffect>> {
        let operation_count = plan.operations().len();
        if operation_count != 1 {
            return Err(Error::UnsupportedAtomicBatch { operation_count });
        }
        let operation_plan = plan.into_operations().into_head();
        let command = single_command_from_operation_plan(operation_plan)?;
        let effect = self.store.execute_meta_command(command.clone())?;
        Ok(BatchEffects::single(OperationEffects::new(
            NonEmpty::single(CommandEffect::new(command, effect)),
        )))
    }
}

impl CommandExecutor for LedgerCommandExecutor<'_> {
    type Command = LedgerCommand;
    type ComponentEffect = LedgerEffect;
    type Error = Error;

    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> impl future::Future<Output = Result<BatchEffects<Self::Command, Self::ComponentEffect>>>
    + Send
    + '_ {
        future::ready(self.execute_atomic_batch_synchronously(plan))
    }
}

impl CommandExecutor for MetaCommandExecutor<'_> {
    type Command = MetaCommand;
    type ComponentEffect = MetaEffect;
    type Error = Error;

    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> impl future::Future<Output = Result<BatchEffects<Self::Command, Self::ComponentEffect>>>
    + Send
    + '_ {
        future::ready(self.execute_atomic_batch_synchronously(plan))
    }
}

fn single_command_from_operation_plan<Command>(plan: OperationPlan<Command>) -> Result<Command> {
    let commands = plan.into_commands();
    let command_count = commands.len();
    if command_count != 1 {
        return Err(Error::UnsupportedAtomicOperationPlan { command_count });
    }
    Ok(commands.into_head())
}

impl signal_frame::BatchErrorClassification for Error {
    fn batch_failure_reason(&self) -> BatchFailureReason {
        match self {
            Self::Io(_) | Self::ConnectionClosed => BatchFailureReason::EngineUnavailable,
            _ => BatchFailureReason::EngineRejected,
        }
    }

    fn retry_classification(&self) -> RetryClassification {
        match self {
            Self::Io(_) | Self::ConnectionClosed | Self::Engine(_) => RetryClassification::Unknown,
            _ => RetryClassification::NotRetryable,
        }
    }

    fn commit_status(&self) -> CommitStatus {
        match self {
            Self::Engine(_) | Self::Io(_) | Self::ConnectionClosed => CommitStatus::Unknown,
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
