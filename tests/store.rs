use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use meta_signal_repository_ledger::Operation as MetaOperation;
use nota_next::NotaEncode;
use repository_ledger::client::Client;
use repository_ledger::spool::SpoolDirectory;
use repository_ledger::{
    LedgerHistoryRetention, RepositoryLedgerDaemonCommand, RepositoryLedgerDaemonConfigurationFile,
    Store,
};
use signal_frame::{
    AcceptedOutcome, ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, LaneSequence,
    Reply as FrameReply, RequestBuilder, RequestPayload, SessionEpoch, SubReply,
};
use signal_repository_ledger::{
    Catalog, ChangedFiles, Class, CommitMessage, CommitMessages, CommitObservation,
    DaemonConfiguration, Events, FileChange, FilePath, FileStatus, FilesystemPath, GitoliteUser,
    Name, ObjectIdentifier, Operation as LedgerOperation, PushObservation, Query, QueryLimit,
    QueryResult, ReceiveHookNotification, RecentRepositories, RefName, RefUpdate, Registration,
    Reply as LedgerReply, SocketMode, TextSearch, Timestamp,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

fn block_on_request<RequestFuture: std::future::Future>(
    request: RequestFuture,
) -> RequestFuture::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test tokio runtime")
        .block_on(request)
}

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
    value.to_nota()
}

#[test]
fn ordinary_command_line_decodes_only_ordinary_contract_operations() {
    let operation = LedgerOperation::Receive(notification(
        "repository-ledger",
        "1111111111111111111111111111111111111111",
    ));
    let text = encode_to_text(&operation);

    assert_eq!(
        Client::working_operation_from_nota(&text).expect("ordinary operation"),
        operation
    );
    assert!(Client::meta_operation_from_nota(&text).is_err());
}

#[test]
fn meta_command_line_decodes_only_meta_contract_operations() {
    let operation = MetaOperation::Register(Registration {
        repository_name: Name::new("repository-ledger"),
        repository_class: Class::RuntimeComponent,
    });
    let text = encode_to_text(&operation);

    assert_eq!(
        Client::meta_operation_from_nota(&text).expect("meta operation"),
        operation
    );
    assert!(Client::working_operation_from_nota(&text).is_err());
}

#[test]
fn hook_notifications_are_committed_as_typed_events() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");

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
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");

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
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");

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
fn history_retention_keeps_the_newest_events_and_never_reuses_sequences() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");
    for identifier in ["1", "2", "3"] {
        store
            .record_hook_notification(notification("repository-ledger", identifier))
            .expect("hook event records");
    }

    assert_eq!(
        store
            .compact_history(LedgerHistoryRetention::new(1))
            .expect("history compacts"),
        2
    );
    let retained = store
        .repository_events(Events {
            repository_name: None,
            since_sequence: None,
            limit: QueryLimit::new(10),
        })
        .expect("events list");
    assert_eq!(retained.events.len(), 1);
    let next = store
        .record_hook_notification(notification("repository-ledger", "4"))
        .expect("new event records after compaction");
    assert!(next.sequence > retained.events[0].sequence);
}

#[test]
fn committed_spool_files_are_reclaimed_immediately() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");
    let spool = directory.path().join("spool");
    std::fs::create_dir_all(&spool).expect("spool dir");
    let file = spool.join("20260519T120000Z-repository-ledger-1.nota");
    std::fs::write(
        &file,
        r#"(ReceiveHookNotification
  (Name repository-ledger)
  (GitoliteUser gitolite-admin)
  (ReceivedAt 20260519T120000Z)
  (DaemonSocketPresent False)
  (RefUpdates
    (RefUpdate 0000000000000000000000000000000000000000 1111111111111111111111111111111111111111 refs/heads/main)
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
        !spool.join("processed").exists(),
        "committed fallback projections are terminal and leave no processed history"
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
fn ordinary_nexus_runner_rejects_multi_operation_batches_before_commit() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");

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

    let reply = block_on_request(store.handle_ordinary_request(request));
    match reply {
        FrameReply::Accepted {
            outcome: AcceptedOutcome::BatchAborted { commit, retry, .. },
            per_operation,
        } => {
            assert_eq!(per_operation.len(), 2);
            assert_eq!(commit, signal_frame::CommitStatus::NotCommitted);
            assert_eq!(retry, signal_frame::RetryClassification::NotRetryable);
        }
        other => panic!("expected Nexus runner batch abort, got {other:?}"),
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
fn store_answers_ordinary_catalog_query() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");
    store
        .register_repository(Registration {
            repository_name: Name::new("repository-ledger"),
            repository_class: Class::RuntimeComponent,
        })
        .expect("register");

    let request = LedgerOperation::Query(Query::Catalog(Catalog)).into_request();
    match block_on_request(store.handle_ordinary_request(request)) {
        FrameReply::Accepted { per_operation, .. } => match per_operation.into_head() {
            SubReply::Ok(LedgerReply::QueryResult(QueryResult::Catalog(listing))) => {
                assert_eq!(listing.repositories.len(), 1)
            }
            other => panic!("unexpected reply {other:?}"),
        },
        other => panic!("unexpected reply {other:?}"),
    }
}

#[test]
fn store_answers_meta_repository_registration() {
    let directory = tempfile::tempdir().expect("temp dir");
    let store = Store::open(directory.path().join("repository-ledger.sema")).expect("store opens");

    let request = meta_signal_repository_ledger::Operation::Register(Registration {
        repository_name: Name::new("meta-signal-repository-ledger"),
        repository_class: Class::MetaSignalContract,
    })
    .into_request();
    match block_on_request(store.handle_meta_request(request)) {
        FrameReply::Accepted { per_operation, .. } => match per_operation.into_head() {
            SubReply::Ok(meta_signal_repository_ledger::Reply::Registered(registered)) => {
                assert_eq!(
                    registered.repository_name.as_str(),
                    "meta-signal-repository-ledger"
                )
            }
            other => panic!("unexpected reply {other:?}"),
        },
        other => panic!("unexpected reply {other:?}"),
    }
}

#[test]
fn daemon_configuration_accepts_binary_file_argument() {
    let directory = tempfile::tempdir().expect("temp dir");
    let configuration_path = directory.path().join("repository-ledger-daemon.rkyv");
    let configuration = daemon_configuration(directory.path());

    RepositoryLedgerDaemonConfigurationFile::new(&configuration_path)
        .write_configuration(&configuration)
        .expect("write daemon configuration");

    let decoded =
        RepositoryLedgerDaemonCommand::from_arguments([configuration_path.display().to_string()])
            .configuration()
            .expect("read daemon configuration");

    assert_eq!(decoded.wire(), &configuration);
}

#[test]
fn daemon_configuration_rejects_nota_arguments() {
    let directory = tempfile::tempdir().expect("temp dir");
    let nota_path = directory.path().join("repository-ledger-daemon.nota");
    std::fs::write(&nota_path, "(DaemonConfiguration)").expect("write nota fixture");

    let inline = RepositoryLedgerDaemonCommand::from_arguments(["(DaemonConfiguration)"])
        .configuration()
        .expect_err("inline NOTA is rejected");
    let file = RepositoryLedgerDaemonCommand::from_arguments([nota_path.display().to_string()])
        .configuration()
        .expect_err(".nota file is rejected");

    assert!(matches!(inline, repository_ledger::Error::Argument(_)));
    assert!(matches!(file, repository_ledger::Error::Argument(_)));
}

#[test]
fn daemon_process_starts_from_binary_configuration_and_answers_catalog_query() {
    let directory = tempfile::tempdir().expect("temp dir");
    let configuration_path = directory.path().join("repository-ledger-daemon.rkyv");
    let configuration = daemon_configuration(directory.path());
    RepositoryLedgerDaemonConfigurationFile::new(&configuration_path)
        .write_configuration(&configuration)
        .expect("write daemon configuration");

    let mut child = Command::new(env!("CARGO_BIN_EXE_repository-ledger-daemon"))
        .arg(&configuration_path)
        .spawn()
        .expect("repository-ledger-daemon starts");

    let ordinary_socket = directory.path().join("repository-ledger.sock");
    wait_for_socket(&ordinary_socket);

    let mut client = UnixStream::connect(&ordinary_socket).expect("client connects");
    let exchange = fresh_exchange();
    let request = LedgerOperation::Query(Query::Catalog(Catalog)).into_request();
    let frame =
        signal_repository_ledger::Frame::new(ExchangeFrameBody::Request { exchange, request });
    write_ordinary_frame(&mut client, &frame);
    let reply = read_ordinary_frame(&mut client);
    match reply.into_body() {
        ExchangeFrameBody::Reply {
            exchange: reply_exchange,
            reply: FrameReply::Accepted { per_operation, .. },
        } => {
            assert_eq!(reply_exchange, exchange);
            match per_operation.into_head() {
                SubReply::Ok(LedgerReply::QueryResult(QueryResult::Catalog(listing))) => {
                    assert_eq!(listing.repositories.len(), 0)
                }
                other => panic!("unexpected reply {other:?}"),
            }
        }
        other => panic!("unexpected frame {other:?}"),
    }

    stop_child(&mut child);
}

#[test]
fn daemon_process_starts_from_binary_configuration_and_answers_meta_registration() {
    let directory = tempfile::tempdir().expect("temp dir");
    let configuration_path = directory.path().join("repository-ledger-daemon.rkyv");
    let configuration = daemon_configuration(directory.path());
    RepositoryLedgerDaemonConfigurationFile::new(&configuration_path)
        .write_configuration(&configuration)
        .expect("write daemon configuration");

    let mut child = Command::new(env!("CARGO_BIN_EXE_repository-ledger-daemon"))
        .arg(&configuration_path)
        .spawn()
        .expect("repository-ledger-daemon starts");

    let meta_socket = directory.path().join("meta-repository-ledger.sock");
    wait_for_socket(&meta_socket);

    let mut client = UnixStream::connect(&meta_socket).expect("client connects");
    let exchange = fresh_exchange();
    let request = meta_signal_repository_ledger::Operation::Register(Registration {
        repository_name: Name::new("repository-ledger"),
        repository_class: Class::RuntimeComponent,
    })
    .into_request();
    let frame =
        meta_signal_repository_ledger::Frame::new(ExchangeFrameBody::Request { exchange, request });
    write_meta_frame(&mut client, &frame);
    let reply = read_meta_frame(&mut client);
    match reply.into_body() {
        ExchangeFrameBody::Reply {
            exchange: reply_exchange,
            reply: FrameReply::Accepted { per_operation, .. },
        } => {
            assert_eq!(reply_exchange, exchange);
            match per_operation.into_head() {
                SubReply::Ok(meta_signal_repository_ledger::Reply::Registered(registered)) => {
                    assert_eq!(registered.repository_name.as_str(), "repository-ledger")
                }
                other => panic!("unexpected reply {other:?}"),
            }
        }
        other => panic!("unexpected frame {other:?}"),
    }

    stop_child(&mut child);
}

#[test]
fn daemon_source_runs_the_emitted_shell_over_the_kameo_actors() {
    let daemon = std::fs::read_to_string("src/daemon.rs").expect("daemon source");
    // The daemon module hand-writes only the component hooks; the listener
    // spine is emitted into src/schema/daemon.rs.
    assert!(daemon.contains("impl ComponentDaemon for RepositoryLedgerProcessDaemon"));
    assert!(daemon.contains("RepositoryLedgerStoreActor"));
    assert!(daemon.contains("SpoolIngestActor"));
    // The hand-wired accept-loop spine and its synchronous block_on drive are
    // gone — the emitted shell owns listener mechanics and the actor handlers
    // await the Nexus runner natively.
    assert!(!daemon.contains("AsyncMultiListenerDaemon"));
    assert!(!daemon.contains("AsyncMultiConnectionRuntime"));
    assert!(!daemon.contains("block_on"));
    assert!(!daemon.contains("std::os::unix::net::UnixListener"));
    assert!(!daemon.contains("thread::spawn"));
    assert!(!daemon.contains("thread::sleep"));
    assert!(!daemon.contains("Arc<Mutex"));
    assert!(!Path::new("src/frame_io.rs").exists());

    let library = std::fs::read_to_string("src/lib.rs").expect("library source");
    assert!(!library.contains("block_on"));
    assert!(!library.contains("futures_executor"));

    // The emitted listener shell is generated, not hand-written.
    let emitted = std::fs::read_to_string("src/schema/daemon.rs").expect("emitted daemon shell");
    assert!(emitted.contains("AsyncMultiListenerDaemon"));
    assert!(emitted.contains("@generated by schema-rust"));
}

fn fresh_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn daemon_configuration(directory: &Path) -> DaemonConfiguration {
    DaemonConfiguration {
        ordinary_socket_path: FilesystemPath::new(
            directory
                .join("repository-ledger.sock")
                .display()
                .to_string(),
        ),
        ordinary_socket_mode: SocketMode::new(0o600),
        meta_socket_path: FilesystemPath::new(
            directory
                .join("meta-repository-ledger.sock")
                .display()
                .to_string(),
        ),
        meta_socket_mode: SocketMode::new(0o600),
        store_path: FilesystemPath::new(
            directory
                .join("repository-ledger.sema")
                .display()
                .to_string(),
        ),
        spool_directory: FilesystemPath::new(directory.join("spool").display().to_string()),
    }
}

fn wait_for_socket(socket: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("socket was not created: {}", socket.display());
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn write_ordinary_frame(stream: &mut UnixStream, frame: &signal_repository_ledger::Frame) {
    LengthPrefixedCodec::default()
        .write_body(
            stream,
            &FrameBody::new(frame.encode().expect("encode frame")),
        )
        .expect("write frame");
}

fn read_ordinary_frame(stream: &mut UnixStream) -> signal_repository_ledger::Frame {
    let body = LengthPrefixedCodec::default()
        .read_body(stream)
        .expect("read frame");
    signal_repository_ledger::Frame::decode(body.bytes()).expect("decode frame")
}

fn write_meta_frame(stream: &mut UnixStream, frame: &meta_signal_repository_ledger::Frame) {
    LengthPrefixedCodec::default()
        .write_body(
            stream,
            &FrameBody::new(frame.encode().expect("encode frame")),
        )
        .expect("write frame");
}

fn read_meta_frame(stream: &mut UnixStream) -> meta_signal_repository_ledger::Frame {
    let body = LengthPrefixedCodec::default()
        .read_body(stream)
        .expect("read frame");
    meta_signal_repository_ledger::Frame::decode(body.bytes()).expect("decode frame")
}
