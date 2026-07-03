//! `repository-ledger-write-configuration` — the deploy/bootstrap tool that
//! encodes a typed NOTA configuration request into the binary rkyv startup file
//! the daemon consumes. `repository-ledger-daemon` itself takes only a
//! pre-generated rkyv signal-file argument and rejects inline NOTA and `.nota`
//! paths, so this binary is the NOTA-to-binary boundary at deploy time. Mirrors
//! `lojix-write-configuration` and `mirror-write-configuration`.

use std::path::PathBuf;

use nota::{NotaDecode, NotaDecodeError, NotaSource};
use repository_ledger::ConfigurationWriteRequest;
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

fn main() -> std::process::ExitCode {
    match ConfigurationWriterCommand::from_environment().run() {
        Ok(output_path) => {
            println!("(ConfigurationWritten [{}])", output_path.display());
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("repository-ledger-write-configuration: {error}");
            std::process::ExitCode::from(2)
        }
    }
}

/// The writer command: one NOTA argument carrying a
/// [`ConfigurationWriteRequest`].
struct ConfigurationWriterCommand {
    command: ComponentCommand,
}

/// The single-variant top-level NOTA the deploy path authors, so the body reads
/// `(ConfigurationWriteRequest (...))`.
#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
enum ConfigurationWriterInput {
    ConfigurationWriteRequest(ConfigurationWriteRequest),
}

impl ConfigurationWriterCommand {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<PathBuf, ConfigurationWriterError> {
        let text = self.source_text()?;
        let request = NotaSource::new(&text)
            .parse::<ConfigurationWriterInput>()?
            .into_request();
        request.write().map_err(ConfigurationWriterError::Write)
    }

    fn source_text(&self) -> Result<String, ConfigurationWriterError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| ConfigurationWriterError::ReadNotaFile { path, source })
            }
            ComponentArgument::SignalFile(file) => {
                Err(ConfigurationWriterError::UnsupportedSignalFile {
                    path: file.into_path(),
                })
            }
        }
    }
}

impl ConfigurationWriterInput {
    fn into_request(self) -> ConfigurationWriteRequest {
        match self {
            Self::ConfigurationWriteRequest(request) => request,
        }
    }
}

#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error(transparent)]
    Argument(#[from] ArgumentError),
    #[error("read NOTA file {}: {source}", path.display())]
    ReadNotaFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("signal-encoded configuration requests are not supported: {}", path.display())]
    UnsupportedSignalFile { path: PathBuf },
    #[error(transparent)]
    Decode(#[from] NotaDecodeError),
    #[error("write configuration archive: {0}")]
    Write(repository_ledger::Error),
}
