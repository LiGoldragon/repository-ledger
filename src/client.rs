use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_frame::{
    ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, HandshakeReply, HandshakeRequest,
    LaneSequence, Reply as FrameReply, SessionEpoch, SubReply,
};
use signal_repository_ledger::{Reply as LedgerReply, Request as LedgerRequest};

use crate::frame_io::OrdinaryFrameIo;
use crate::{Error, Result};

const DEFAULT_SOCKET_PATH: &str = "/run/repository-ledger/repository-ledger.sock";
const SOCKET_ENVIRONMENT_VARIABLE: &str = "REPOSITORY_LEDGER_SOCKET_PATH";

pub struct Client {
    socket_path: PathBuf,
}

impl Client {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    pub fn from_environment() -> Self {
        let socket_path = std::env::var_os(SOCKET_ENVIRONMENT_VARIABLE)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET_PATH));
        Self::new(socket_path)
    }

    pub fn send(&self, request: LedgerRequest) -> Result<LedgerReply> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        self.handshake(&mut stream)?;
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

    pub fn run_from_environment() -> Result<String> {
        let request = CliRequest::from_arguments(std::env::args_os().skip(1))?;
        let reply = Self::from_environment().send(request.payload)?;
        let mut encoder = Encoder::new();
        reply.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }

    fn handshake(&self, stream: &mut UnixStream) -> Result<()> {
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
}

pub struct CliRequest {
    payload: LedgerRequest,
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
        let source = if text.starts_with('(') {
            text.to_owned()
        } else {
            std::fs::read_to_string(PathBuf::from(argument))?
        };
        Self::from_nota(&source)
    }

    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let payload = LedgerRequest::decode(&mut decoder)?;
        if decoder.peek_token()?.is_some() {
            return Err(Error::UnexpectedFrame);
        }
        Ok(Self { payload })
    }
}
