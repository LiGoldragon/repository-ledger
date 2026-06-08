//! Repository-ledger's daemon hooks — the only daemon code the component
//! hand-writes (record 1488 escape hatches).
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, the decode -> serve -> encode spine, and the `ExitReport` entry) is
//! emitted into `src/schema/daemon.rs` by schema-rust-next's daemon emitter.
//! Repository-ledger runs the **component-decoded** working tier: its public
//! ordinary and meta sockets still speak the relation-specific
//! `signal_channel!` `ExchangeFrame` wire (not a schema-emitted root), so the
//! component owns the per-tier frame decode/encode and drives the existing
//! kameo store actors — the emitted shell owns only listener mechanics.
//!
//! `RepositoryLedgerEngine` is the [`ComponentDaemon::Engine`]. It opens the
//! durable [`Store`] behind a `RepositoryLedgerStoreActor` (whose mailbox
//! serialises every read and write) and a `SpoolIngestActor` driven by a
//! periodic ticker. The engine is shared `&` across connections, so it holds
//! the runtime in a [`OnceCell`] and starts the actors on first use; each
//! request is an `ask` against the store actor; the Nexus runner is awaited
//! natively in the actor handler, so the synchronous executor drive the old
//! spine used is gone.

use std::path::PathBuf;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::OnceCell;

use signal_frame::{
    ExchangeFrameBody, HandshakeRejectionReason, HandshakeReply, ProtocolVersion,
    SIGNAL_FRAME_PROTOCOL_VERSION,
};
use triad_runtime::{
    AcceptedConnection, FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec,
    MaximumFrameLength,
};

use crate::schema::daemon::ComponentDaemon;
use crate::spool::{SpoolDirectory, SpoolIngestSummary};
use crate::{Configuration, ConfigurationError, Error, Result, Store};

const MAXIMUM_REQUEST_FRAME_BYTES: usize = 8 * 1024 * 1024;
const SPOOL_INGEST_INTERVAL: Duration = Duration::from_secs(2);

/// The type-level selector for repository-ledger's emitted daemon. It carries
/// no runtime data — it is the marker the emitted
/// `DaemonCommand<RepositoryLedgerProcessDaemon>` and the generated runtime
/// dispatch on, selecting the component's `Configuration` / `Engine` / `Error`
/// types through the `ComponentDaemon` associated types.
#[derive(Debug)]
pub struct RepositoryLedgerProcessDaemon;

/// Repository-ledger's daemon error: the engine-facing variants the emitted
/// spine needs (`From<FrameError>`) plus the component's domain error.
#[derive(Debug, Error)]
pub enum RepositoryLedgerDaemonError {
    #[error("daemon IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("daemon frame error: {0}")]
    Frame(#[from] FrameError),

    #[error("daemon signal frame error: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("repository-ledger engine error: {0}")]
    Engine(#[from] Error),
}

/// The component-decoded engine: the durable store opened behind a serialising
/// store actor, plus the spool directory the periodic ingester drains. The
/// engine is shared `&` across connections; the store actor and spool ingester
/// start lazily on first use through the [`OnceCell`].
pub struct RepositoryLedgerEngine {
    store_path: PathBuf,
    spool_directory: PathBuf,
    runtime: OnceCell<ActorRef<RepositoryLedgerStoreActor>>,
}

/// The kameo actor owning the durable [`Store`]. Its mailbox serialises every
/// ordinary and meta request, so the store needs no interior lock.
pub struct RepositoryLedgerStoreActor {
    store: Store,
}

/// The kameo actor that drains the spool directory into the store on demand.
struct SpoolIngestActor {
    store: ActorRef<RepositoryLedgerStoreActor>,
    spool_directory: PathBuf,
}

/// The periodic driver that asks the spool ingester to drain on a fixed
/// interval.
struct SpoolIngestTicker {
    spool: ActorRef<SpoolIngestActor>,
    interval: Duration,
}

struct HandleOrdinaryRequest {
    request: signal_repository_ledger::Request,
}

struct HandleMetaRequest {
    request: meta_signal_repository_ledger::ChannelRequest,
}

struct IngestSpool;

struct IngestSpoolInDirectory {
    spool_directory: PathBuf,
}

#[derive(kameo::Reply)]
struct OrdinaryRequestHandled {
    reply: signal_repository_ledger::ReplyEnvelope,
}

#[derive(kameo::Reply)]
struct MetaRequestHandled {
    reply: meta_signal_repository_ledger::ChannelReply,
}

#[derive(kameo::Reply)]
struct SpoolIngested {
    result: Result<SpoolIngestSummary>,
}

impl RepositoryLedgerEngine {
    pub fn from_configuration(configuration: &Configuration) -> Result<Self> {
        Ok(Self {
            store_path: configuration.store_path().to_path_buf(),
            spool_directory: configuration.spool_directory().to_path_buf(),
            runtime: OnceCell::new(),
        })
    }

    /// Lazily open the store, start the store actor, and spawn the spool
    /// ingester plus its periodic ticker; subsequent connections reuse the
    /// started store actor.
    async fn store(&self) -> Result<&ActorRef<RepositoryLedgerStoreActor>> {
        self.runtime
            .get_or_try_init(|| async {
                let store = RepositoryLedgerStoreActor::start(Store::open(&self.store_path)?).await;
                let spool =
                    SpoolIngestActor::start(store.clone(), self.spool_directory.clone()).await;
                SpoolIngestActor::ingest(&spool).await?;
                SpoolIngestTicker::new(spool, SPOOL_INGEST_INTERVAL).spawn();
                Ok(store)
            })
            .await
    }

    async fn handle_working_connection(&self, mut connection: AcceptedConnection) -> Result<()> {
        loop {
            let frame = self.read_ordinary_frame(&mut connection).await?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::HandshakeReply(
                            Self::handshake_reply_for(request.version()),
                        ),
                    );
                    self.write_ordinary_frame(&mut connection, &reply).await?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = self
                        .store()
                        .await?
                        .ask(HandleOrdinaryRequest { request })
                        .await
                        .map_err(|error| Error::ActorCall {
                            detail: error.to_string(),
                        })?
                        .reply;
                    let frame = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::Reply { exchange, reply },
                    );
                    self.write_ordinary_frame(&mut connection, &frame).await?;
                    return Ok(());
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }

    async fn handle_meta_connection(&self, mut connection: AcceptedConnection) -> Result<()> {
        loop {
            let frame = self.read_meta_frame(&mut connection).await?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = meta_signal_repository_ledger::Frame::new(
                        meta_signal_repository_ledger::FrameBody::HandshakeReply(
                            Self::handshake_reply_for(request.version()),
                        ),
                    );
                    self.write_meta_frame(&mut connection, &reply).await?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = self
                        .store()
                        .await?
                        .ask(HandleMetaRequest { request })
                        .await
                        .map_err(|error| Error::ActorCall {
                            detail: error.to_string(),
                        })?
                        .reply;
                    let frame = meta_signal_repository_ledger::Frame::new(
                        meta_signal_repository_ledger::FrameBody::Reply { exchange, reply },
                    );
                    self.write_meta_frame(&mut connection, &frame).await?;
                    return Ok(());
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }

    async fn read_ordinary_frame(
        &self,
        connection: &mut AcceptedConnection,
    ) -> Result<signal_repository_ledger::Frame> {
        let body = Self::request_codec()
            .read_body_async(connection.stream_mut())
            .await?;
        Ok(signal_repository_ledger::Frame::decode(body.bytes())?)
    }

    async fn write_ordinary_frame(
        &self,
        connection: &mut AcceptedConnection,
        frame: &signal_repository_ledger::Frame,
    ) -> Result<()> {
        Self::request_codec()
            .write_body_async(
                connection.stream_mut(),
                &LengthPrefixedFrameBody::new(frame.encode()?),
            )
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }

    async fn read_meta_frame(
        &self,
        connection: &mut AcceptedConnection,
    ) -> Result<meta_signal_repository_ledger::Frame> {
        let body = Self::request_codec()
            .read_body_async(connection.stream_mut())
            .await?;
        Ok(meta_signal_repository_ledger::Frame::decode(body.bytes())?)
    }

    async fn write_meta_frame(
        &self,
        connection: &mut AcceptedConnection,
        frame: &meta_signal_repository_ledger::Frame,
    ) -> Result<()> {
        Self::request_codec()
            .write_body_async(
                connection.stream_mut(),
                &LengthPrefixedFrameBody::new(frame.encode()?),
            )
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }

    fn request_codec() -> LengthPrefixedCodec {
        LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES))
    }

    /// Negotiate the signal-frame protocol version against a connecting peer:
    /// accept when the local version subsumes the peer's, otherwise reject with
    /// the incompatibility.
    fn handshake_reply_for(peer: ProtocolVersion) -> HandshakeReply {
        let local = SIGNAL_FRAME_PROTOCOL_VERSION;
        if local.accepts(peer) {
            HandshakeReply::Accepted(local)
        } else {
            HandshakeReply::Rejected(HandshakeRejectionReason::IncompatibleVersion { local, peer })
        }
    }
}

impl ComponentDaemon for RepositoryLedgerProcessDaemon {
    type Configuration = Configuration;
    type ConfigurationError = ConfigurationError;
    type Engine = RepositoryLedgerEngine;
    type Error = RepositoryLedgerDaemonError;

    const PROCESS_NAME: &'static str = "repository-ledger-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> std::result::Result<Self::Configuration, Self::ConfigurationError> {
        Configuration::from_binary_path(path)
    }

    fn build_runtime(
        configuration: &Self::Configuration,
    ) -> std::result::Result<Self::Engine, Self::Error> {
        Ok(RepositoryLedgerEngine::from_configuration(configuration)?)
    }

    async fn handle_working_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> std::result::Result<(), Self::Error> {
        Ok(engine.handle_working_connection(connection).await?)
    }

    async fn handle_meta_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> std::result::Result<(), Self::Error> {
        Ok(engine.handle_meta_connection(connection).await?)
    }
}

impl RepositoryLedgerStoreActor {
    async fn start(store: Store) -> ActorRef<Self> {
        let actor = Self::spawn(Self { store });
        actor.wait_for_startup().await;
        actor
    }
}

impl Actor for RepositoryLedgerStoreActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl Message<HandleOrdinaryRequest> for RepositoryLedgerStoreActor {
    type Reply = OrdinaryRequestHandled;

    async fn handle(
        &mut self,
        message: HandleOrdinaryRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        OrdinaryRequestHandled {
            reply: self.store.handle_ordinary_request(message.request).await,
        }
    }
}

impl Message<HandleMetaRequest> for RepositoryLedgerStoreActor {
    type Reply = MetaRequestHandled;

    async fn handle(
        &mut self,
        message: HandleMetaRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        MetaRequestHandled {
            reply: self.store.handle_meta_request(message.request).await,
        }
    }
}

impl Message<IngestSpoolInDirectory> for RepositoryLedgerStoreActor {
    type Reply = SpoolIngested;

    async fn handle(
        &mut self,
        message: IngestSpoolInDirectory,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SpoolIngested {
            result: SpoolDirectory::new(message.spool_directory).ingest_into(&self.store),
        }
    }
}

impl SpoolIngestActor {
    async fn start(
        store: ActorRef<RepositoryLedgerStoreActor>,
        spool_directory: PathBuf,
    ) -> ActorRef<Self> {
        let actor = Self::spawn(Self {
            store,
            spool_directory,
        });
        actor.wait_for_startup().await;
        actor
    }

    async fn ingest(actor: &ActorRef<Self>) -> Result<SpoolIngestSummary> {
        actor
            .ask(IngestSpool)
            .await
            .map_err(|error| Error::ActorCall {
                detail: error.to_string(),
            })?
            .result
    }
}

impl Actor for SpoolIngestActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl Message<IngestSpool> for SpoolIngestActor {
    type Reply = SpoolIngested;

    async fn handle(
        &mut self,
        _message: IngestSpool,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self
            .store
            .ask(IngestSpoolInDirectory {
                spool_directory: self.spool_directory.clone(),
            })
            .await
        {
            Ok(ingested) => ingested,
            Err(error) => SpoolIngested {
                result: Err(Error::ActorCall {
                    detail: error.to_string(),
                }),
            },
        }
    }
}

impl SpoolIngestTicker {
    fn new(spool: ActorRef<SpoolIngestActor>, interval: Duration) -> Self {
        Self { spool, interval }
    }

    fn spawn(self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(self) {
        let mut interval = tokio::time::interval(self.interval);
        loop {
            interval.tick().await;
            match SpoolIngestActor::ingest(&self.spool).await {
                Ok(_) => {}
                Err(error) => eprintln!("(SpoolIngestError [{error}])"),
            }
        }
    }
}
