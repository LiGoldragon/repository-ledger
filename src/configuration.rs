//! The component's daemon configuration: the binary rkyv startup message the
//! emitted shell decodes from the daemon's single argument.
//!
//! Daemons never parse NOTA (hard override): the wire form is
//! [`signal_repository_ledger::DaemonConfiguration`] encoded to rkyv. This
//! wrapper owns the decoded paths the emitted shell binds its listeners from
//! through the [`triad_runtime::DaemonConfiguration`] trait, plus the store and
//! spool paths the engine opens.

use std::path::{Path, PathBuf};

use signal_repository_ledger::DaemonConfiguration as WireConfiguration;
use thiserror::Error;
use triad_runtime::{DaemonConfiguration, SocketMode as RuntimeSocketMode};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    wire: WireConfiguration,
    ordinary_socket_path: PathBuf,
    meta_socket_path: PathBuf,
    store_path: PathBuf,
    spool_directory: PathBuf,
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("failed to read repository-ledger daemon configuration {path:?}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to decode repository-ledger daemon configuration archive {path:?}")]
    Decode { path: PathBuf },

    #[error("failed to encode repository-ledger daemon configuration archive")]
    Encode,

    #[error("failed to write repository-ledger daemon configuration {path:?}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl Configuration {
    pub fn from_wire(wire: WireConfiguration) -> Self {
        Self {
            ordinary_socket_path: PathBuf::from(wire.ordinary_socket_path.as_str()),
            meta_socket_path: PathBuf::from(wire.meta_socket_path.as_str()),
            store_path: PathBuf::from(wire.store_path.as_str()),
            spool_directory: PathBuf::from(wire.spool_directory.as_str()),
            wire,
        }
    }

    pub fn from_binary_path(path: &Path) -> Result<Self, ConfigurationError> {
        let bytes = std::fs::read(path).map_err(|source| ConfigurationError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_binary_bytes(&bytes).map_err(|_| ConfigurationError::Decode {
            path: path.to_path_buf(),
        })
    }

    pub fn from_binary_bytes(bytes: &[u8]) -> Result<Self, ConfigurationError> {
        let wire =
            rkyv::from_bytes::<WireConfiguration, rkyv::rancor::Error>(bytes).map_err(|_| {
                ConfigurationError::Decode {
                    path: PathBuf::new(),
                }
            })?;
        Ok(Self::from_wire(wire))
    }

    pub fn to_binary_bytes(&self) -> Result<Vec<u8>, ConfigurationError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.wire)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ConfigurationError::Encode)
    }

    pub fn wire(&self) -> &WireConfiguration {
        &self.wire
    }

    pub fn store_path(&self) -> &Path {
        &self.store_path
    }

    pub fn spool_directory(&self) -> &Path {
        &self.spool_directory
    }

    fn ordinary_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(self.wire.ordinary_socket_mode.into_u32())
    }

    fn meta_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(self.wire.meta_socket_mode.into_u32())
    }
}

impl DaemonConfiguration for Configuration {
    fn socket_path(&self) -> &Path {
        &self.ordinary_socket_path
    }

    fn socket_mode(&self) -> Option<RuntimeSocketMode> {
        Some(self.ordinary_socket_mode())
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(&self.meta_socket_path)
    }

    fn meta_socket_mode(&self) -> Option<RuntimeSocketMode> {
        Some(self.meta_socket_mode())
    }

    fn database_path(&self) -> &Path {
        &self.store_path
    }
}

impl From<WireConfiguration> for Configuration {
    fn from(wire: WireConfiguration) -> Self {
        Self::from_wire(wire)
    }
}
