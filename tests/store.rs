use repository_ledger::RepositoryLedgerStore;
use signal_repository_ledger::{
    GitoliteUser, RefUpdate, RepositoryClass, RepositoryEventQuery, RepositoryName,
    RepositoryObjectIdentifier, RepositoryQueryLimit, RepositoryReceiveHookNotification,
    RepositoryRefName, RepositoryRegistration, RepositoryTimestamp,
};

fn notification(
    repository_name: &str,
    new_object_identifier: &str,
) -> RepositoryReceiveHookNotification {
    RepositoryReceiveHookNotification {
        repository_name: RepositoryName::new(repository_name),
        gitolite_user: GitoliteUser::new("gitolite-admin"),
        received_at: RepositoryTimestamp::new("20260519T120000Z"),
        daemon_socket_present: false,
        ref_updates: vec![RefUpdate {
            old_object_identifier: RepositoryObjectIdentifier::new(
                "0000000000000000000000000000000000000000",
            ),
            new_object_identifier: RepositoryObjectIdentifier::new(new_object_identifier),
            ref_name: RepositoryRefName::new("refs/heads/main"),
        }],
    }
}

#[test]
fn hook_notifications_are_committed_as_typed_events() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = RepositoryLedgerStore::open(directory.path().join("repository-ledger.redb"))
        .expect("store opens");

    let first = store
        .record_hook_notification(notification(
            "repository-ledger",
            "1111111111111111111111111111111111111111",
        ))
        .expect("first notification records");
    let second = store
        .record_hook_notification(notification(
            "signal-repository-ledger",
            "2222222222222222222222222222222222222222",
        ))
        .expect("second notification records");

    assert_eq!(first.sequence.into_u64(), 1);
    assert_eq!(second.sequence.into_u64(), 2);

    let listing = store
        .repository_events(RepositoryEventQuery {
            repository_name: None,
            since_sequence: None,
            limit: RepositoryQueryLimit::new(10),
        })
        .expect("events list");
    assert_eq!(listing.events.len(), 2);
    assert_eq!(
        listing.events[0].notification.repository_name.as_str(),
        "repository-ledger"
    );
}

#[test]
fn repository_catalog_is_typed_and_sorted() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = RepositoryLedgerStore::open(directory.path().join("repository-ledger.redb"))
        .expect("store opens");

    store
        .register_repository(RepositoryRegistration {
            repository_name: RepositoryName::new("signal-repository-ledger"),
            repository_class: RepositoryClass::OrdinarySignalContract,
        })
        .expect("signal registration");
    let catalog = store
        .register_repository(RepositoryRegistration {
            repository_name: RepositoryName::new("repository-ledger"),
            repository_class: RepositoryClass::RuntimeComponent,
        })
        .expect("runtime registration");

    let names: Vec<&str> = catalog
        .repositories
        .iter()
        .map(|registration| registration.repository_name.as_str())
        .collect();
    assert_eq!(names, vec!["repository-ledger", "signal-repository-ledger"]);
}
