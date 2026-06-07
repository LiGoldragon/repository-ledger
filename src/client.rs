use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use meta_signal_repository_ledger::{ChannelRequest as MetaRequest, Reply as MetaReply};
use nota_next::{Delimiter, NotaBlock, NotaEncode, NotaSource};
use signal_frame::{
    CommandLineSocket, ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, HandshakeReply,
    HandshakeRequest, LaneSequence, Reply as FrameReply, SessionEpoch, SubReply,
};
use signal_repository_ledger::{Reply as LedgerReply, Request as LedgerRequest};

use crate::frame_io::{MetaFrameIo, OrdinaryFrameIo};
use crate::{Error, Result};

const DEFAULT_ORDINARY_SOCKET_PATH: &str = "/run/repository-ledger/repository-ledger.sock";
const DEFAULT_META_SOCKET_PATH: &str = "/run/repository-ledger/repository-ledger-meta.sock";
const ORDINARY_SOCKET_ENVIRONMENT_VARIABLE: &str = "REPOSITORY_LEDGER_SOCKET_PATH";
const META_SOCKET_ENVIRONMENT_VARIABLE: &str = "REPOSITORY_LEDGER_META_SOCKET_PATH";

signal_frame::signal_cli! {
    pub struct CommandLineDispatch {
        working signal_repository_ledger::Operation;
        meta meta_signal_repository_ledger::Operation;
    }
}

pub struct Client {
    ordinary_socket_path: PathBuf,
    meta_socket_path: PathBuf,
}

impl Client {
    pub fn new(ordinary_socket_path: impl Into<PathBuf>) -> Self {
        Self::with_sockets(
            ordinary_socket_path,
            PathBuf::from(DEFAULT_META_SOCKET_PATH),
        )
    }

    pub fn with_sockets(
        ordinary_socket_path: impl Into<PathBuf>,
        meta_socket_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            ordinary_socket_path: ordinary_socket_path.into(),
            meta_socket_path: meta_socket_path.into(),
        }
    }

    pub fn from_environment() -> Self {
        let ordinary_socket_path = std::env::var_os(ORDINARY_SOCKET_ENVIRONMENT_VARIABLE)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_ORDINARY_SOCKET_PATH));
        let meta_socket_path = std::env::var_os(META_SOCKET_ENVIRONMENT_VARIABLE)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_META_SOCKET_PATH));
        Self::with_sockets(ordinary_socket_path, meta_socket_path)
    }

    pub fn send_working(&self, request: LedgerRequest) -> Result<LedgerReply> {
        let mut stream = UnixStream::connect(&self.ordinary_socket_path)?;
        self.handshake_working(&mut stream)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let frame =
            signal_repository_ledger::Frame::new(ExchangeFrameBody::Request { exchange, request });
        OrdinaryFrameIo::write(&mut stream, &frame)?;
        stream.flush()?;

        let reply = OrdinaryFrameIo::read(&mut stream)?;
        match reply.into_body() {
            ExchangeFrameBody::Reply {
                exchange: reply_exchange,
                reply,
            } if reply_exchange == exchange => Self::unwrap_single_reply(reply),
            _ => Err(Error::UnexpectedFrame),
        }
    }

    pub fn send_meta(&self, request: MetaRequest) -> Result<MetaReply> {
        let mut stream = UnixStream::connect(&self.meta_socket_path)?;
        self.handshake_meta(&mut stream)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let frame = meta_signal_repository_ledger::Frame::new(ExchangeFrameBody::Request {
            exchange,
            request,
        });
        MetaFrameIo::write(&mut stream, &frame)?;
        stream.flush()?;

        let reply = MetaFrameIo::read(&mut stream)?;
        match reply.into_body() {
            ExchangeFrameBody::Reply {
                exchange: reply_exchange,
                reply,
            } if reply_exchange == exchange => Self::unwrap_single_meta_reply(reply),
            _ => Err(Error::UnexpectedFrame),
        }
    }

    pub fn run_from_environment() -> Result<String> {
        let request = CliRequest::from_arguments(std::env::args_os().skip(1))?;
        let client = Self::from_environment();
        match request {
            CliRequest::Working(request) => {
                let reply = client.send_working(request)?;
                encode_reply(&reply)
            }
            CliRequest::Meta(request) => {
                let reply = client.send_meta(request)?;
                encode_reply(&reply)
            }
        }
    }

    fn handshake_working(&self, stream: &mut UnixStream) -> Result<()> {
        let frame = signal_repository_ledger::Frame::new(ExchangeFrameBody::HandshakeRequest(
            HandshakeRequest::current(),
        ));
        OrdinaryFrameIo::write(stream, &frame)?;
        let reply = OrdinaryFrameIo::read(stream)?;
        match reply.into_body() {
            ExchangeFrameBody::HandshakeReply(HandshakeReply::Accepted(_)) => Ok(()),
            ExchangeFrameBody::HandshakeReply(HandshakeReply::Rejected(_)) => {
                Err(Error::HandshakeRejected)
            }
            _ => Err(Error::UnexpectedFrame),
        }
    }

    fn handshake_meta(&self, stream: &mut UnixStream) -> Result<()> {
        let frame = meta_signal_repository_ledger::Frame::new(ExchangeFrameBody::HandshakeRequest(
            HandshakeRequest::current(),
        ));
        MetaFrameIo::write(stream, &frame)?;
        let reply = MetaFrameIo::read(stream)?;
        match reply.into_body() {
            ExchangeFrameBody::HandshakeReply(HandshakeReply::Accepted(_)) => Ok(()),
            ExchangeFrameBody::HandshakeReply(HandshakeReply::Rejected(_)) => {
                Err(Error::HandshakeRejected)
            }
            _ => Err(Error::UnexpectedFrame),
        }
    }

    fn unwrap_single_reply(reply: FrameReply<LedgerReply>) -> Result<LedgerReply> {
        match reply {
            FrameReply::Accepted { per_operation, .. } => {
                match per_operation.into_head_and_tail() {
                    (SubReply::Ok(payload), tail) if tail.is_empty() => Ok(payload),
                    _ => Err(Error::SignalRequestFailed),
                }
            }
            FrameReply::Rejected { .. } => Err(Error::SignalRequestRejected),
        }
    }

    fn unwrap_single_meta_reply(reply: FrameReply<MetaReply>) -> Result<MetaReply> {
        match reply {
            FrameReply::Accepted { per_operation, .. } => {
                match per_operation.into_head_and_tail() {
                    (SubReply::Ok(payload), tail) if tail.is_empty() => Ok(payload),
                    _ => Err(Error::SignalRequestFailed),
                }
            }
            FrameReply::Rejected { .. } => Err(Error::SignalRequestRejected),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliRequest {
    Working(LedgerRequest),
    Meta(MetaRequest),
}

impl CliRequest {
    pub fn from_arguments<I, S>(arguments: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let arguments: Vec<OsString> = arguments
            .into_iter()
            .map(|argument| argument.as_ref().to_owned())
            .collect();
        let [argument] = arguments.as_slice() else {
            return Err(Error::ExpectedSingleArgument);
        };
        let text = argument.to_str().ok_or(Error::ExpectedSingleArgument)?;
        if text.starts_with("--") {
            return Err(Error::FlagArgument(text.to_owned()));
        }
        let source = if text.starts_with('(') || text.starts_with('[') {
            text.to_owned()
        } else {
            std::fs::read_to_string(PathBuf::from(argument))?
        };
        Self::from_nota(&source)
    }

    pub fn from_nota(text: &str) -> Result<Self> {
        match RequestHead::from_text(text)?.route()? {
            CommandLineSocket::Working => Self::decode_working(text),
            CommandLineSocket::Meta => Self::decode_meta(text),
        }
    }

    fn decode_working(text: &str) -> Result<Self> {
        let payload = NotaSource::new(text).parse::<LedgerRequest>()?;
        Ok(Self::Working(payload))
    }

    fn decode_meta(text: &str) -> Result<Self> {
        let payload = NotaSource::new(text).parse::<MetaRequest>()?;
        Ok(Self::Meta(payload))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    head: String,
}

impl RequestHead {
    pub fn from_text(text: &str) -> Result<Self> {
        let root = NotaSource::new(text).parse_root()?;
        let first_payload = root
            .as_delimited(Delimiter::SquareBracket)
            .and_then(|payloads| payloads.first())
            .unwrap_or(&root);
        let children = NotaBlock::new(first_payload)
            .expect_delimited(Delimiter::Parenthesis, "request payload")?;
        let head = children
            .first()
            .and_then(|block| block.demote_to_string())
            .ok_or(nota_next::NotaDecodeError::ExpectedAtom {
                type_name: "request head",
            })?
            .to_owned();
        Ok(Self { head })
    }

    pub fn route(&self) -> Result<CommandLineSocket> {
        CommandLineDispatch::new()
            .route_head(&self.head)
            .map_err(Error::command_line_route)
    }
}

fn encode_reply(reply: &impl NotaEncode) -> Result<String> {
    Ok(reply.to_nota())
}
