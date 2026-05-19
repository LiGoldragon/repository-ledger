use std::io::{Read, Write};

use signal_core::{
    HandshakeRejectionReason, HandshakeReply, ProtocolVersion, SIGNAL_CORE_PROTOCOL_VERSION,
};

use crate::{Error, Result};

const LENGTH_PREFIX_BYTES: usize = 4;

pub struct FrameBytes {
    bytes: Vec<u8>,
}

impl FrameBytes {
    pub fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut prefix = [0_u8; LENGTH_PREFIX_BYTES];
        match reader.read_exact(&mut prefix) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::ConnectionClosed);
            }
            Err(error) => return Err(error.into()),
        }
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(LENGTH_PREFIX_BYTES + length);
        bytes.extend_from_slice(&prefix);
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body)?;
        bytes.extend_from_slice(&body);
        Ok(Self { bytes })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct OrdinaryFrameIo;

impl OrdinaryFrameIo {
    pub fn read(reader: &mut impl Read) -> Result<signal_repository_ledger::Frame> {
        let bytes = FrameBytes::read_from(reader)?;
        Ok(signal_repository_ledger::Frame::decode_length_prefixed(
            bytes.as_slice(),
        )?)
    }

    pub fn write(writer: &mut impl Write, frame: &signal_repository_ledger::Frame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        Ok(())
    }
}

pub struct OwnerFrameIo;

impl OwnerFrameIo {
    pub fn read(reader: &mut impl Read) -> Result<owner_signal_repository_ledger::Frame> {
        let bytes = FrameBytes::read_from(reader)?;
        Ok(owner_signal_repository_ledger::Frame::decode_length_prefixed(bytes.as_slice())?)
    }

    pub fn write(
        writer: &mut impl Write,
        frame: &owner_signal_repository_ledger::Frame,
    ) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        Ok(())
    }
}

pub fn handshake_reply_for(peer: ProtocolVersion) -> HandshakeReply {
    let local = SIGNAL_CORE_PROTOCOL_VERSION;
    if local.accepts(peer) {
        HandshakeReply::Accepted(local)
    } else {
        HandshakeReply::Rejected(HandshakeRejectionReason::IncompatibleVersion { local, peer })
    }
}
