//! The component-owned argv front door: turns the daemon's single argument into
//! a decoded [`Configuration`], rejecting inline NOTA and `.nota` files (the
//! daemon-binary-only override). The emitted
//! `DaemonCommand<RepositoryLedgerProcessDaemon>` is the process entry the
//! `repository-ledger-daemon` binary runs; this helper exposes the same
//! argv -> binary configuration decode for tests and tooling, plus the rkyv
//! configuration-file writer the deploy path encodes with.
//!
//! [`ConfigurationWriteRequest`] is the deploy-time NOTA-to-binary boundary:
//! the `repository-ledger-write-configuration` binary decodes one of these from
//! typed NOTA and writes the daemon's binary rkyv startup file, since the daemon
//! itself rejects `.nota` paths and accepts only a pre-encoded signal file.

use std::path::{Path, PathBuf};

use nota::NotaDecode;
use signal_repository_ledger::{
    DaemonConfiguration as WireConfiguration, FilesystemPath, SocketMode,
};
use triad_runtime::{ComponentArgument, ComponentCommand, SignalFile};

use crate::{Configuration, Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryLedgerDaemonCommand {
    command: ComponentCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryLedgerDaemonConfigurationFile {
    path: PathBuf,
}

impl RepositoryLedgerDaemonCommand {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
        }
    }

    pub fn configuration(&self) -> Result<Configuration> {
        match self.command.signal_file_argument()? {
            ComponentArgument::SignalFile(file) => {
                RepositoryLedgerDaemonConfigurationFile::from_signal_file(file).configuration()
            }
            ComponentArgument::InlineNota(_) | ComponentArgument::NotaFile(_) => {
                Err(triad_runtime::ArgumentError::ExpectedSignalFile.into())
            }
        }
    }
}

impl RepositoryLedgerDaemonConfigurationFile {
    pub fn from_signal_file(file: SignalFile) -> Self {
        Self {
            path: file.into_path(),
        }
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    pub fn configuration(&self) -> Result<Configuration> {
        let bytes = std::fs::read(&self.path).map_err(|source| Error::ConfigurationRead {
            path: self.path.clone(),
            source,
        })?;
        rkyv::from_bytes::<WireConfiguration, rkyv::rancor::Error>(&bytes)
            .map(Configuration::from_wire)
            .map_err(|_| Error::ConfigurationArchiveDecode)
    }

    pub fn write_configuration(&self, configuration: &WireConfiguration) -> Result<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(configuration)
            .map_err(|_| Error::ConfigurationArchiveEncode)?;
        std::fs::write(&self.path, bytes.as_ref()).map_err(|source| Error::ConfigurationWrite {
            path: self.path.clone(),
            source,
        })
    }
}

/// The deploy-time encoder request: the typed NOTA the
/// `repository-ledger-write-configuration` binary decodes, carrying the rkyv
/// output path plus the six [`WireConfiguration`] fields in declared order. The
/// NOTA body is `(ConfigurationWriteRequest (<ordinary-socket> <ordinary-mode>
/// <meta-socket> <meta-mode> <store> <spool> <output.rkyv>))`; the final field
/// is the destination the daemon's binary startup file is written to.
#[derive(Clone, Debug, Eq, PartialEq, NotaDecode)]
pub struct ConfigurationWriteRequest {
    ordinary_socket_path: FilesystemPath,
    ordinary_socket_mode: SocketMode,
    meta_socket_path: FilesystemPath,
    meta_socket_mode: SocketMode,
    store_path: FilesystemPath,
    spool_directory: FilesystemPath,
    output_path: FilesystemPath,
}

impl ConfigurationWriteRequest {
    pub fn output_file(&self) -> RepositoryLedgerDaemonConfigurationFile {
        RepositoryLedgerDaemonConfigurationFile::new(PathBuf::from(self.output_path.as_str()))
    }

    pub fn wire_configuration(&self) -> WireConfiguration {
        WireConfiguration {
            ordinary_socket_path: self.ordinary_socket_path.clone(),
            ordinary_socket_mode: self.ordinary_socket_mode,
            meta_socket_path: self.meta_socket_path.clone(),
            meta_socket_mode: self.meta_socket_mode,
            store_path: self.store_path.clone(),
            spool_directory: self.spool_directory.clone(),
        }
    }

    /// Build the wire configuration and write its rkyv archive to the request's
    /// output path through the existing [`RepositoryLedgerDaemonConfigurationFile`]
    /// writer, returning that output path for the caller's receipt.
    pub fn write(&self) -> Result<PathBuf> {
        let output_file = self.output_file();
        output_file.write_configuration(&self.wire_configuration())?;
        Ok(output_file.as_path().to_path_buf())
    }
}
