use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_frame::{
    ExchangeFrameBody, HandshakeRejectionReason, HandshakeReply, ProtocolVersion,
    SIGNAL_FRAME_PROTOCOL_VERSION,
};
use signal_repository_ledger::DaemonConfiguration;
use triad_runtime::{
    AcceptedConnection, ActorListenerSocket, ActorMultiConnectionRuntime, ActorMultiListenerDaemon,
    FrameBody, LengthPrefixedCodec, MaximumFrameLength, RequestConcurrencyLimit, RequestErrorLog,
    SocketMode,
};

use crate::spool::{SpoolDirectory, SpoolIngestSummary};
use crate::{Error, Result, Store};

const MAXIMUM_REQUEST_FRAME_BYTES: usize = 8 * 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
const SPOOL_INGEST_INTERVAL: Duration = Duration::from_secs(2);
const REQUEST_CONCURRENCY_LIMIT: usize = 16;

pub struct Daemon {
    configuration: DaemonConfiguration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListenerTier {
    Ordinary,
    Meta,
}

#[derive(Clone)]
struct RepositoryLedgerRuntime {
    store: ActorRef<RepositoryLedgerStoreActor>,
}

pub struct RepositoryLedgerStoreActor {
    store: Store,
}

struct SpoolIngestActor {
    store: ActorRef<RepositoryLedgerStoreActor>,
    spool_directory: PathBuf,
}

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

impl Daemon {
    pub fn new(configuration: DaemonConfiguration) -> Self {
        Self { configuration }
    }

    pub fn run(self) -> Result<()> {
        tokio::runtime::Runtime::new()
            .map_err(|error| Error::DaemonRuntime {
                detail: error.to_string(),
            })?
            .block_on(self.run_actor_native())
    }

    pub fn ingest_spool(&self) -> Result<SpoolIngestSummary> {
        let store = Store::open(self.configuration.store_path.as_str())?;
        SpoolDirectory::new(self.configuration.spool_directory.as_str()).ingest_into(&store)
    }

    async fn run_actor_native(self) -> Result<()> {
        let store =
            RepositoryLedgerStoreActor::start(Store::open(self.configuration.store_path.as_str())?)
                .await;
        let spool = SpoolIngestActor::start(
            store.clone(),
            PathBuf::from(self.configuration.spool_directory.as_str()),
        )
        .await;
        SpoolIngestTicker::new(spool.clone(), SPOOL_INGEST_INTERVAL).spawn();
        SpoolIngestActor::ingest(&spool).await?;

        let runtime = RepositoryLedgerRuntime::new(store);
        let listener_sockets = [
            ActorListenerSocket::new(
                ListenerTier::Ordinary,
                self.configuration.ordinary_socket_path.as_str(),
            )
            .with_socket_mode(SocketMode::new(
                self.configuration.ordinary_socket_mode.into_u32(),
            )),
            ActorListenerSocket::new(
                ListenerTier::Meta,
                self.configuration.meta_socket_path.as_str(),
            )
            .with_socket_mode(SocketMode::new(
                self.configuration.meta_socket_mode.into_u32(),
            )),
        ];
        ActorMultiListenerDaemon::new(
            listener_sockets,
            runtime,
            RequestErrorLog::new("repository-ledger-daemon"),
        )
        .with_concurrency_limit(RequestConcurrencyLimit::new(REQUEST_CONCURRENCY_LIMIT))
        .run()
        .await
        .map_err(|error| Error::DaemonRuntime {
            detail: error.to_string(),
        })
    }
}

impl Display for ListenerTier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary => formatter.write_str("ordinary"),
            Self::Meta => formatter.write_str("meta"),
        }
    }
}

impl RepositoryLedgerRuntime {
    fn new(store: ActorRef<RepositoryLedgerStoreActor>) -> Self {
        Self { store }
    }

    async fn handle_ordinary_connection(&self, mut connection: AcceptedConnection) -> Result<()> {
        loop {
            let frame = self.read_ordinary_frame(&mut connection).await?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::HandshakeReply(handshake_reply_for(
                            request.version(),
                        )),
                    );
                    self.write_ordinary_frame(&mut connection, &reply).await?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = self
                        .store
                        .ask(HandleOrdinaryRequest::new(request))
                        .await
                        .map_err(|error| Error::ActorCall {
                            detail: error.to_string(),
                        })?
                        .into_reply();
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
                            handshake_reply_for(request.version()),
                        ),
                    );
                    self.write_meta_frame(&mut connection, &reply).await?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = self
                        .store
                        .ask(HandleMetaRequest::new(request))
                        .await
                        .map_err(|error| Error::ActorCall {
                            detail: error.to_string(),
                        })?
                        .into_reply();
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
        let codec = Self::request_codec();
        let body = tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| Error::DaemonRuntime {
            detail: String::from("ordinary request frame read timed out"),
        })??;
        Ok(signal_repository_ledger::Frame::decode(body.bytes())?)
    }

    async fn write_ordinary_frame(
        &self,
        connection: &mut AcceptedConnection,
        frame: &signal_repository_ledger::Frame,
    ) -> Result<()> {
        Self::request_codec()
            .write_body_async(connection.stream_mut(), &FrameBody::new(frame.encode()?))
            .await?;
        Ok(())
    }

    async fn read_meta_frame(
        &self,
        connection: &mut AcceptedConnection,
    ) -> Result<meta_signal_repository_ledger::Frame> {
        let codec = Self::request_codec();
        let body = tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| Error::DaemonRuntime {
            detail: String::from("meta request frame read timed out"),
        })??;
        Ok(meta_signal_repository_ledger::Frame::decode(body.bytes())?)
    }

    async fn write_meta_frame(
        &self,
        connection: &mut AcceptedConnection,
        frame: &meta_signal_repository_ledger::Frame,
    ) -> Result<()> {
        Self::request_codec()
            .write_body_async(connection.stream_mut(), &FrameBody::new(frame.encode()?))
            .await?;
        Ok(())
    }

    fn request_codec() -> LengthPrefixedCodec {
        LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES))
    }
}

impl ActorMultiConnectionRuntime for RepositoryLedgerRuntime {
    type Listener = ListenerTier;
    type Error = Error;

    async fn handle_connection(
        &self,
        listener: Self::Listener,
        connection: AcceptedConnection,
    ) -> Result<()> {
        match listener {
            ListenerTier::Ordinary => self.handle_ordinary_connection(connection).await,
            ListenerTier::Meta => self.handle_meta_connection(connection).await,
        }
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
        OrdinaryRequestHandled::new(self.store.handle_ordinary_request(message.request))
    }
}

impl Message<HandleMetaRequest> for RepositoryLedgerStoreActor {
    type Reply = MetaRequestHandled;

    async fn handle(
        &mut self,
        message: HandleMetaRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        MetaRequestHandled::new(self.store.handle_meta_request(message.request))
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
            .into_result()
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
        self.store
            .ask(IngestSpoolInDirectory::new(self.spool_directory.clone()))
            .await
            .map_err(|error| Error::ActorCall {
                detail: error.to_string(),
            })
            .into()
    }
}

struct IngestSpoolInDirectory {
    spool_directory: PathBuf,
}

impl IngestSpoolInDirectory {
    fn new(spool_directory: PathBuf) -> Self {
        Self { spool_directory }
    }
}

impl Message<IngestSpoolInDirectory> for RepositoryLedgerStoreActor {
    type Reply = SpoolIngested;

    async fn handle(
        &mut self,
        message: IngestSpoolInDirectory,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SpoolIngested::new(SpoolDirectory::new(message.spool_directory).ingest_into(&self.store))
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

impl HandleOrdinaryRequest {
    fn new(request: signal_repository_ledger::Request) -> Self {
        Self { request }
    }
}

impl HandleMetaRequest {
    fn new(request: meta_signal_repository_ledger::ChannelRequest) -> Self {
        Self { request }
    }
}

impl OrdinaryRequestHandled {
    fn new(reply: signal_repository_ledger::ReplyEnvelope) -> Self {
        Self { reply }
    }

    fn into_reply(self) -> signal_repository_ledger::ReplyEnvelope {
        self.reply
    }
}

impl MetaRequestHandled {
    fn new(reply: meta_signal_repository_ledger::ChannelReply) -> Self {
        Self { reply }
    }

    fn into_reply(self) -> meta_signal_repository_ledger::ChannelReply {
        self.reply
    }
}

impl SpoolIngested {
    fn new(result: Result<SpoolIngestSummary>) -> Self {
        Self { result }
    }

    fn into_result(self) -> Result<SpoolIngestSummary> {
        self.result
    }
}

impl From<Result<SpoolIngested>> for SpoolIngested {
    fn from(result: Result<SpoolIngested>) -> Self {
        match result {
            Ok(reply) => reply,
            Err(error) => Self::new(Err(error)),
        }
    }
}

fn handshake_reply_for(peer: ProtocolVersion) -> HandshakeReply {
    let local = SIGNAL_FRAME_PROTOCOL_VERSION;
    if local.accepts(peer) {
        HandshakeReply::Accepted(local)
    } else {
        HandshakeReply::Rejected(HandshakeRejectionReason::IncompatibleVersion { local, peer })
    }
}
