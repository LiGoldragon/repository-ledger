use std::path::{Path, PathBuf};

use signal_repository_ledger::DaemonConfiguration;
use triad_runtime::{ComponentArgument, ComponentCommand, SignalFile};

use crate::daemon::Daemon;
use crate::{Error, Result};

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

    pub fn configuration(&self) -> Result<DaemonConfiguration> {
        match self.command.signal_file_argument()? {
            ComponentArgument::SignalFile(file) => {
                RepositoryLedgerDaemonConfigurationFile::from_signal_file(file).configuration()
            }
            ComponentArgument::InlineNota(_) | ComponentArgument::NotaFile(_) => {
                Err(triad_runtime::ArgumentError::ExpectedSignalFile.into())
            }
        }
    }

    pub fn run(&self) -> Result<()> {
        Daemon::new(self.configuration()?).run()
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

    pub fn configuration(&self) -> Result<DaemonConfiguration> {
        let bytes = std::fs::read(&self.path).map_err(|source| Error::ConfigurationRead {
            path: self.path.clone(),
            source,
        })?;
        rkyv::from_bytes::<DaemonConfiguration, rkyv::rancor::Error>(&bytes)
            .map_err(|_| Error::ConfigurationArchiveDecode)
    }

    pub fn write_configuration(&self, configuration: &DaemonConfiguration) -> Result<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(configuration)
            .map_err(|_| Error::ConfigurationArchiveEncode)?;
        std::fs::write(&self.path, bytes.as_ref()).map_err(|source| Error::ConfigurationWrite {
            path: self.path.clone(),
            source,
        })
    }
}
