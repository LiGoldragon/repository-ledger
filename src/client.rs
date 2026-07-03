use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use meta_signal_repository_ledger::{Operation as MetaOperation, Reply as MetaReply};
use nota::{NotaEncode, NotaSource};
use signal_frame::{
    ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, HandshakeReply, HandshakeRequest,
    LaneSequence, Reply as FrameReply, RequestPayload, SessionEpoch, SubReply,
};
use signal_repository_ledger::{Operation as LedgerOperation, Reply as LedgerReply};

use crate::{Error, Result};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

const DEFAULT_ORDINARY_SOCKET_PATH: &str = "/run/repository-ledger/repository-ledger.sock";
const DEFAULT_META_SOCKET_PATH: &str = "/run/repository-ledger/repository-ledger-meta.sock";
const ORDINARY_SOCKET_ENVIRONMENT_VARIABLE: &str = "REPOSITORY_LEDGER_SOCKET_PATH";
const META_SOCKET_ENVIRONMENT_VARIABLE: &str = "REPOSITORY_LEDGER_META_SOCKET_PATH";

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

    pub fn send_working(&self, operation: LedgerOperation) -> Result<LedgerReply> {
        let mut stream = UnixStream::connect(&self.ordinary_socket_path)?;
        self.handshake_working(&mut stream)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let request = operation.into_request();
        let frame =
            signal_repository_ledger::Frame::new(ExchangeFrameBody::Request { exchange, request });
        self.write_working_frame(&mut stream, &frame)?;
        stream.flush()?;

        let reply = self.read_working_frame(&mut stream)?;
        match reply.into_body() {
            ExchangeFrameBody::Reply {
                exchange: reply_exchange,
                reply,
            } if reply_exchange == exchange => Self::unwrap_single_reply(reply),
            _ => Err(Error::UnexpectedFrame),
        }
    }

    pub fn send_meta(&self, operation: MetaOperation) -> Result<MetaReply> {
        let mut stream = UnixStream::connect(&self.meta_socket_path)?;
        self.handshake_meta(&mut stream)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let request = operation.into_request();
        let frame = meta_signal_repository_ledger::Frame::new(ExchangeFrameBody::Request {
            exchange,
            request,
        });
        self.write_meta_frame(&mut stream, &frame)?;
        stream.flush()?;

        let reply = self.read_meta_frame(&mut stream)?;
        match reply.into_body() {
            ExchangeFrameBody::Reply {
                exchange: reply_exchange,
                reply,
            } if reply_exchange == exchange => Self::unwrap_single_meta_reply(reply),
            _ => Err(Error::UnexpectedFrame),
        }
    }

    pub fn run_working_from_environment() -> Result<String> {
        let operation = CommandLineInput::from_arguments(std::env::args_os().skip(1))?
            .into_working_operation()?;
        let client = Self::from_environment();
        let reply = client.send_working(operation)?;
        Self::encode_reply(&reply)
    }

    pub fn run_meta_from_environment() -> Result<String> {
        let operation =
            CommandLineInput::from_arguments(std::env::args_os().skip(1))?.into_meta_operation()?;
        let client = Self::from_environment();
        let reply = client.send_meta(operation)?;
        Self::encode_reply(&reply)
    }

    pub fn working_operation_from_nota(text: &str) -> Result<LedgerOperation> {
        CommandLineInput::from_nota(text).into_working_operation()
    }

    pub fn meta_operation_from_nota(text: &str) -> Result<MetaOperation> {
        CommandLineInput::from_nota(text).into_meta_operation()
    }

    fn handshake_working(&self, stream: &mut UnixStream) -> Result<()> {
        let frame = signal_repository_ledger::Frame::new(ExchangeFrameBody::HandshakeRequest(
            HandshakeRequest::current(),
        ));
        self.write_working_frame(stream, &frame)?;
        let reply = self.read_working_frame(stream)?;
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
        self.write_meta_frame(stream, &frame)?;
        let reply = self.read_meta_frame(stream)?;
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

    fn read_working_frame(
        &self,
        stream: &mut UnixStream,
    ) -> Result<signal_repository_ledger::Frame> {
        let body = LengthPrefixedCodec::default().read_body(stream)?;
        Ok(signal_repository_ledger::Frame::decode(body.bytes())?)
    }

    fn write_working_frame(
        &self,
        stream: &mut UnixStream,
        frame: &signal_repository_ledger::Frame,
    ) -> Result<()> {
        LengthPrefixedCodec::default().write_body(stream, &FrameBody::new(frame.encode()?))?;
        Ok(())
    }

    fn read_meta_frame(
        &self,
        stream: &mut UnixStream,
    ) -> Result<meta_signal_repository_ledger::Frame> {
        let body = LengthPrefixedCodec::default().read_body(stream)?;
        Ok(meta_signal_repository_ledger::Frame::decode(body.bytes())?)
    }

    fn write_meta_frame(
        &self,
        stream: &mut UnixStream,
        frame: &meta_signal_repository_ledger::Frame,
    ) -> Result<()> {
        LengthPrefixedCodec::default().write_body(stream, &FrameBody::new(frame.encode()?))?;
        Ok(())
    }

    fn encode_reply(reply: &impl NotaEncode) -> Result<String> {
        Ok(reply.to_nota())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLineInput {
    text: String,
}

impl CommandLineInput {
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
            std::fs::read_to_string(PathBuf::from(argument.as_os_str()))?
        };
        Ok(Self { text: source })
    }

    pub fn from_nota(text: &str) -> Self {
        Self {
            text: text.to_owned(),
        }
    }

    pub fn into_working_operation(self) -> Result<LedgerOperation> {
        Ok(NotaSource::new(&self.text).parse::<LedgerOperation>()?)
    }

    pub fn into_meta_operation(self) -> Result<MetaOperation> {
        Ok(NotaSource::new(&self.text).parse::<MetaOperation>()?)
    }
}
