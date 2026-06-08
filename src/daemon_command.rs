//! The component-owned argv front door: turns the daemon's single argument into
//! a decoded [`Configuration`], rejecting inline NOTA and `.nota` files (the
//! daemon-binary-only override). The emitted
//! `DaemonCommand<RepositoryLedgerProcessDaemon>` is the process entry the
//! `repository-ledger-daemon` binary runs; this helper exposes the same
//! argv -> binary configuration decode for tests and tooling, plus the rkyv
//! configuration-file writer the deploy path encodes with.

use std::path::{Path, PathBuf};

use signal_repository_ledger::DaemonConfiguration as WireConfiguration;
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
