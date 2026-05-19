use std::os::unix::net::UnixStream;
use std::thread;

use repository_ledger::RepositoryLedgerStore;
use repository_ledger::daemon::RepositoryLedgerDaemon;
use repository_ledger::frame_io::{OrdinaryFrameIo, OwnerFrameIo};
use repository_ledger::spool::SpoolDirectory;
use signal_core::{
    ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, HandshakeReply, HandshakeRequest,
    LaneSequence, Reply, RequestPayload, SessionEpoch, SubReply,
};
use signal_repository_ledger::{
    GitoliteUser, RefUpdate, RepositoryCatalogQuery, RepositoryClass, RepositoryEventQuery,
    RepositoryLedgerReply, RepositoryLedgerRequest, RepositoryName, RepositoryObjectIdentifier,
    RepositoryQueryLimit, RepositoryReceiveHookNotification, RepositoryRefName,
    RepositoryRegistration, RepositoryTimestamp,
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

#[test]
fn spool_files_are_ingested_and_moved_to_processed() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = RepositoryLedgerStore::open(directory.path().join("repository-ledger.redb"))
        .expect("store opens");
    let spool = directory.path().join("spool");
    std::fs::create_dir_all(&spool).expect("spool dir");
    let file = spool.join("20260519T120000Z-repository-ledger-1.nota");
    std::fs::write(
        &file,
        r#"(RepositoryReceiveHookNotification
  (RepositoryName "repository-ledger")
  (GitoliteUser "gitolite-admin")
  (ReceivedAt "20260519T120000Z")
  (DaemonSocketPresent false)
  (RefUpdates
    (RefUpdate "0000000000000000000000000000000000000000" "1111111111111111111111111111111111111111" "refs/heads/main")
  )
)"#,
    )
    .expect("write spool");

    let summary = SpoolDirectory::new(&spool)
        .ingest_into(&store)
        .expect("ingest");
    assert_eq!(summary.files_seen, 1);
    assert_eq!(summary.files_recorded, 1);
    assert!(!file.exists());
    assert!(
        spool
            .join("processed")
            .join("20260519T120000Z-repository-ledger-1.nota")
            .exists()
    );

    let listing = store
        .repository_events(RepositoryEventQuery {
            repository_name: Some(RepositoryName::new("repository-ledger")),
            since_sequence: None,
            limit: RepositoryQueryLimit::new(10),
        })
        .expect("events");
    assert_eq!(listing.events.len(), 1);
    assert_eq!(
        listing.events[0].notification.ref_updates[0]
            .new_object_identifier
            .as_str(),
        "1111111111111111111111111111111111111111"
    );
}

#[test]
fn ordinary_signal_socket_answers_catalog_query() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = RepositoryLedgerStore::open(directory.path().join("repository-ledger.redb"))
        .expect("store opens");
    store
        .register_repository(RepositoryRegistration {
            repository_name: RepositoryName::new("repository-ledger"),
            repository_class: RepositoryClass::RuntimeComponent,
        })
        .expect("register");

    let (mut client, mut server) = UnixStream::pair().expect("pair");
    let handle = thread::spawn(move || {
        RepositoryLedgerDaemon::serve_ordinary_stream(&store, &mut server).expect("serve");
    });

    let handshake = signal_repository_ledger::Frame::new(ExchangeFrameBody::HandshakeRequest(
        HandshakeRequest::current(),
    ));
    OrdinaryFrameIo::write(&mut client, &handshake).expect("write handshake");
    let handshake_reply = OrdinaryFrameIo::read(&mut client).expect("handshake reply");
    assert!(matches!(
        handshake_reply.into_body(),
        ExchangeFrameBody::HandshakeReply(HandshakeReply::Accepted(_))
    ));

    let exchange = fresh_exchange();
    let request =
        RepositoryLedgerRequest::RepositoryCatalogQuery(RepositoryCatalogQuery).into_request();
    let frame =
        signal_repository_ledger::Frame::new(ExchangeFrameBody::Request { exchange, request });
    OrdinaryFrameIo::write(&mut client, &frame).expect("write request");
    let reply = OrdinaryFrameIo::read(&mut client).expect("read reply");
    match reply.into_body() {
        ExchangeFrameBody::Reply {
            exchange: reply_exchange,
            reply: Reply::Accepted { per_operation, .. },
        } => {
            assert_eq!(reply_exchange, exchange);
            match per_operation.into_head() {
                SubReply::Ok {
                    payload: RepositoryLedgerReply::RepositoryCatalogListing(listing),
                    ..
                } => assert_eq!(listing.repositories.len(), 1),
                other => panic!("unexpected reply {other:?}"),
            }
        }
        other => panic!("unexpected frame {other:?}"),
    }
    handle.join().expect("server thread");
}

#[test]
fn owner_signal_socket_registers_repository() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = RepositoryLedgerStore::open(directory.path().join("repository-ledger.redb"))
        .expect("store opens");

    let (mut client, mut server) = UnixStream::pair().expect("pair");
    let handle = thread::spawn(move || {
        RepositoryLedgerDaemon::serve_owner_stream(&store, &mut server).expect("serve");
    });

    let exchange = fresh_exchange();
    let request = owner_signal_repository_ledger::OwnerRepositoryLedgerRequest::RegisterRepository(
        RepositoryRegistration {
            repository_name: RepositoryName::new("owner-signal-repository-ledger"),
            repository_class: RepositoryClass::OwnerSignalContract,
        },
    )
    .into_request();
    let frame = owner_signal_repository_ledger::Frame::new(ExchangeFrameBody::Request {
        exchange,
        request,
    });
    OwnerFrameIo::write(&mut client, &frame).expect("write request");
    let reply = OwnerFrameIo::read(&mut client).expect("read reply");
    match reply.into_body() {
        ExchangeFrameBody::Reply {
            exchange: reply_exchange,
            reply: Reply::Accepted { per_operation, .. },
        } => {
            assert_eq!(reply_exchange, exchange);
            match per_operation.into_head() {
                SubReply::Ok {
                    payload:
                        owner_signal_repository_ledger::OwnerRepositoryLedgerReply::RepositoryRegistered(
                            registered,
                        ),
                    ..
                } => assert_eq!(
                    registered.repository_name.as_str(),
                    "owner-signal-repository-ledger"
                ),
                other => panic!("unexpected reply {other:?}"),
            }
        }
        other => panic!("unexpected frame {other:?}"),
    }
    handle.join().expect("server thread");
}

fn fresh_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}
