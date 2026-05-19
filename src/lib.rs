//! Repository ledger runtime library.
//!
//! The daemon will own this store from one actor. The first slice proves that
//! Gitolite hook notifications can be committed as typed sema-engine records.

use std::path::Path;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, TableDescriptor, TableName,
    TableReference,
};
use signal_repository_ledger::{
    RepositoryCatalogListing, RepositoryEvent, RepositoryEventListing, RepositoryEventQuery,
    RepositoryEventRecorded, RepositoryEventSequence, RepositoryReceiveHookNotification,
    RepositoryRegistration,
};

const SCHEMA_VERSION: u32 = 1;
const REPOSITORY_EVENTS: TableName = TableName::new("repository_events");
const REPOSITORY_REGISTRATIONS: TableName = TableName::new("repository_registrations");

#[derive(Debug, thiserror::Error)]
pub enum RepositoryLedgerError {
    #[error("sema-engine error: {0}")]
    Engine(#[from] sema_engine::Error),
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

pub struct RepositoryLedgerStore {
    engine: Engine,
    events: TableReference<StoredRepositoryEvent>,
    registrations: TableReference<StoredRepositoryRegistration>,
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
        Ok(Self {
            engine,
            events,
            registrations,
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
        self.engine.assert(Assertion::new(
            self.registrations,
            StoredRepositoryRegistration { registration },
        ))?;
        self.repository_catalog()
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
}
