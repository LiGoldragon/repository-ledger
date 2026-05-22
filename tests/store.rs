use std::os::unix::net::UnixStream;
use std::thread;

use nota_codec::NotaEncode;
use owner_signal_repository_ledger::Operation as OwnerOperation;
use repository_ledger::Store;
use repository_ledger::client::{CliRequest, CommandLineDispatch};
use repository_ledger::daemon::Daemon;
use repository_ledger::frame_io::{OrdinaryFrameIo, OwnerFrameIo};
use repository_ledger::spool::SpoolDirectory;
use signal_frame::{
    AcceptedOutcome, CommandLineSocket, ExchangeFrameBody, ExchangeIdentifier, ExchangeLane,
    HandshakeReply, HandshakeRequest, LaneSequence, Reply as FrameReply, RequestBuilder,
    RequestPayload, SessionEpoch, SubReply,
};
use signal_repository_ledger::{
    Catalog, ChangedFiles, Class, CommitMessage, CommitMessages, CommitObservation, Events,
    FileChange, FilePath, FileStatus, GitoliteUser, Name, ObjectIdentifier,
    Operation as LedgerOperation, PushObservation, Query, QueryLimit, QueryResult,
    ReceiveHookNotification, RecentRepositories, RefName, RefUpdate, Registration,
    Reply as LedgerReply, TextSearch, Timestamp,
};

fn notification(repository_name: &str, new_object_identifier: &str) -> ReceiveHookNotification {
    ReceiveHookNotification {
        repository_name: Name::new(repository_name),
        gitolite_user: GitoliteUser::new("gitolite-admin"),
        received_at: Timestamp::new("20260519T120000Z"),
        daemon_socket_present: false,
        ref_updates: vec![RefUpdate {
            old_object_identifier: ObjectIdentifier::new(
                "0000000000000000000000000000000000000000",
            ),
            new_object_identifier: ObjectIdentifier::new(new_object_identifier),
            ref_name: RefName::new("refs/heads/main"),
        }],
    }
}

fn push_observation(
    repository_name: &str,
    received_at: &str,
    commit_object_identifier: &str,
    message: &str,
    changed_files: Vec<FileChange>,
) -> PushObservation {
    let mut notification = notification(repository_name, commit_object_identifier);
    notification.received_at = Timestamp::new(received_at);
    PushObservation {
        notification,
        commits: vec![CommitObservation {
            object_identifier: ObjectIdentifier::new(commit_object_identifier),
            ref_name: RefName::new("refs/heads/main"),
            commit_timestamp: Timestamp::new(received_at),
            message: CommitMessage::new(message),
            changed_files,
        }],
    }
}

fn changed_file(status: &str, path: &str) -> FileChange {
    FileChange {
        status: FileStatus::new(status),
        path: FilePath::new(path),
        old_path: None,
    }
}

fn encode_to_text(value: &impl NotaEncode) -> String {
    let mut encoder = nota_codec::Encoder::new();
    value.encode(&mut encoder).expect("encode");
    encoder.into_string()
}

#[test]
fn command_line_dispatch_routes_working_and_owner_heads() {
    let dispatch = CommandLineDispatch::new();

    assert_eq!(
        dispatch.route_head("Receive").expect("working head"),
        CommandLineSocket::Working
    );
    assert_eq!(
        dispatch.route_head("Register").expect("owner head"),
        CommandLineSocket::Owner
    );
}

#[test]
fn command_line_request_decodes_owner_contract_by_head() {
    let request = OwnerOperation::Register(Registration {
        repository_name: Name::new("repository-ledger"),
        repository_class: Class::RuntimeComponent,
    })
    .into_request();
    let text = encode_to_text(&request);

    match CliRequest::from_nota(&text).expect("owner request") {
        CliRequest::Owner(decoded) => assert_eq!(decoded, request),
        other => panic!("expected owner request, got {other:?}"),
    }
}

#[test]
fn hook_notifications_are_committed_as_typed_events() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");

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
        .repository_events(Events {
            repository_name: None,
            since_sequence: None,
            limit: QueryLimit::new(10),
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
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");

    store
        .register_repository(Registration {
            repository_name: Name::new("signal-repository-ledger"),
            repository_class: Class::OrdinarySignalContract,
        })
        .expect("signal registration");
    let catalog = store
        .register_repository(Registration {
            repository_name: Name::new("repository-ledger"),
            repository_class: Class::RuntimeComponent,
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
fn push_observations_support_recent_repository_file_and_message_queries() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");

    store
        .record_push_observation(push_observation(
            "repository-ledger",
            "20260519T120000Z",
            "1111111111111111111111111111111111111111",
            "add repository query surface",
            vec![
                changed_file("M", "src/lib.rs"),
                changed_file("A", "tests/store.rs"),
            ],
        ))
        .expect("first observation");
    store
        .record_push_observation(push_observation(
            "signal-repository-ledger",
            "20260519T130000Z",
            "2222222222222222222222222222222222222222",
            "add commit message query records",
            vec![changed_file("M", "src/lib.rs")],
        ))
        .expect("second observation");

    let recent = store
        .recent_repositories(RecentRepositories {
            since_received_at: Some(Timestamp::new("20260519T000000Z")),
            limit: QueryLimit::new(10),
        })
        .expect("recent repositories");
    let recent_names: Vec<&str> = recent
        .repositories
        .iter()
        .map(|repository| repository.repository_name.as_str())
        .collect();
    assert_eq!(
        recent_names,
        vec!["signal-repository-ledger", "repository-ledger"]
    );

    let files = store
        .changed_files(ChangedFiles {
            repository_name: Some(Name::new("repository-ledger")),
            since_received_at: Some(Timestamp::new("20260519T000000Z")),
            until_received_at: Some(Timestamp::new("20260519T235959Z")),
            path_contains: Some(TextSearch::new("store")),
            limit: QueryLimit::new(10),
        })
        .expect("changed files");
    assert_eq!(files.files.len(), 1);
    assert_eq!(files.files[0].path.as_str(), "tests/store.rs");

    let commits = store
        .commit_messages(CommitMessages {
            repository_name: None,
            since_received_at: None,
            until_received_at: None,
            message_contains: Some(TextSearch::new("QUERY")),
            limit: QueryLimit::new(10),
        })
        .expect("commit messages");
    assert_eq!(commits.commits.len(), 2);
    assert_eq!(
        commits.commits[0].repository_name.as_str(),
        "signal-repository-ledger"
    );
}

#[test]
fn spool_files_are_ingested_and_moved_to_processed() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");
    let spool = directory.path().join("spool");
    std::fs::create_dir_all(&spool).expect("spool dir");
    let file = spool.join("20260519T120000Z-repository-ledger-1.nota");
    std::fs::write(
        &file,
        r#"(ReceiveHookNotification
  (Name "repository-ledger")
  (GitoliteUser "gitolite-admin")
  (ReceivedAt "20260519T120000Z")
  (DaemonSocketPresent False)
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
        .repository_events(Events {
            repository_name: Some(Name::new("repository-ledger")),
            since_sequence: None,
            limit: QueryLimit::new(10),
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
fn ordinary_signal_executor_rejects_multi_operation_batches_before_commit() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");

    let request = RequestBuilder::new()
        .with(LedgerOperation::Receive(notification(
            "repository-ledger",
            "1111111111111111111111111111111111111111",
        )))
        .with(LedgerOperation::Receive(notification(
            "signal-repository-ledger",
            "2222222222222222222222222222222222222222",
        )))
        .build()
        .expect("multi-operation request");

    let reply = store.handle_ordinary_request(request);
    match reply {
        FrameReply::Accepted {
            outcome: AcceptedOutcome::BatchAborted { commit, retry, .. },
            per_operation,
        } => {
            assert_eq!(per_operation.len(), 2);
            assert_eq!(commit, signal_frame::CommitStatus::NotCommitted);
            assert_eq!(retry, signal_frame::RetryClassification::NotRetryable);
        }
        other => panic!("expected executor batch abort, got {other:?}"),
    }

    let listing = store
        .repository_events(Events {
            repository_name: None,
            since_sequence: None,
            limit: QueryLimit::new(10),
        })
        .expect("events list");
    assert_eq!(listing.events.len(), 0);
}

#[test]
fn ordinary_signal_socket_answers_catalog_query() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");
    store
        .register_repository(Registration {
            repository_name: Name::new("repository-ledger"),
            repository_class: Class::RuntimeComponent,
        })
        .expect("register");

    let (mut client, mut server) = UnixStream::pair().expect("pair");
    let handle = thread::spawn(move || {
        Daemon::serve_ordinary_stream(&store, &mut server).expect("serve");
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
    let request = LedgerOperation::Query(Query::Catalog(Catalog)).into_request();
    let frame =
        signal_repository_ledger::Frame::new(ExchangeFrameBody::Request { exchange, request });
    OrdinaryFrameIo::write(&mut client, &frame).expect("write request");
    let reply = OrdinaryFrameIo::read(&mut client).expect("read reply");
    match reply.into_body() {
        ExchangeFrameBody::Reply {
            exchange: reply_exchange,
            reply: FrameReply::Accepted { per_operation, .. },
        } => {
            assert_eq!(reply_exchange, exchange);
            match per_operation.into_head() {
                SubReply::Ok(LedgerReply::QueryResult(QueryResult::Catalog(listing))) => {
                    assert_eq!(listing.repositories.len(), 1)
                }
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
    let store = Store::open(directory.path().join("repository-ledger.redb")).expect("store opens");

    let (mut client, mut server) = UnixStream::pair().expect("pair");
    let handle = thread::spawn(move || {
        Daemon::serve_owner_stream(&store, &mut server).expect("serve");
    });

    let exchange = fresh_exchange();
    let request = owner_signal_repository_ledger::Operation::Register(Registration {
        repository_name: Name::new("owner-signal-repository-ledger"),
        repository_class: Class::OwnerSignalContract,
    })
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
            reply: FrameReply::Accepted { per_operation, .. },
        } => {
            assert_eq!(reply_exchange, exchange);
            match per_operation.into_head() {
                SubReply::Ok(owner_signal_repository_ledger::Reply::Registered(registered)) => {
                    assert_eq!(
                        registered.repository_name.as_str(),
                        "owner-signal-repository-ledger"
                    )
                }
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
