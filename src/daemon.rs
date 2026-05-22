use std::fs;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use signal_frame::ExchangeFrameBody;
use signal_repository_ledger::DaemonConfiguration;

use crate::frame_io::{OrdinaryFrameIo, OwnerFrameIo, handshake_reply_for};
use crate::spool::{SpoolDirectory, SpoolIngestSummary};
use crate::{Error, Result, Store};

pub struct Daemon {
    configuration: DaemonConfiguration,
}

impl Daemon {
    pub fn new(configuration: DaemonConfiguration) -> Self {
        Self { configuration }
    }

    pub fn run(self) -> Result<()> {
        let store = Arc::new(Mutex::new(Store::open(
            self.configuration.store_path.as_str(),
        )?));
        let ordinary_listener = Self::bind_socket(
            self.configuration.ordinary_socket_path.as_str(),
            self.configuration.ordinary_socket_mode.into_u32(),
        )?;
        let owner_listener = Self::bind_socket(
            self.configuration.owner_socket_path.as_str(),
            self.configuration.owner_socket_mode.into_u32(),
        )?;
        let spool_directory = PathBuf::from(self.configuration.spool_directory.as_str());

        Self::ingest_spool_with_store(&store, &spool_directory)?;

        let ordinary_store = Arc::clone(&store);
        thread::spawn(move || {
            Self::run_ordinary_listener(ordinary_listener, ordinary_store);
        });

        let owner_store = Arc::clone(&store);
        thread::spawn(move || {
            Self::run_owner_listener(owner_listener, owner_store);
        });

        loop {
            thread::sleep(Duration::from_secs(2));
            if let Err(error) = Self::ingest_spool_with_store(&store, &spool_directory) {
                eprintln!("(SpoolIngestError \"{error}\")");
            }
        }
    }

    pub fn ingest_spool(&self) -> Result<SpoolIngestSummary> {
        let store = Store::open(self.configuration.store_path.as_str())?;
        SpoolDirectory::new(self.configuration.spool_directory.as_str()).ingest_into(&store)
    }

    pub fn serve_ordinary_stream(store: &Store, stream: &mut UnixStream) -> Result<()> {
        loop {
            let frame = OrdinaryFrameIo::read(stream)?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::HandshakeReply(handshake_reply_for(
                            request.version(),
                        )),
                    );
                    OrdinaryFrameIo::write(stream, &reply)?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = store.handle_ordinary_request(request);
                    let frame = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::Reply { exchange, reply },
                    );
                    OrdinaryFrameIo::write(stream, &frame)?;
                    return Ok(());
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }

    pub fn serve_owner_stream(store: &Store, stream: &mut UnixStream) -> Result<()> {
        loop {
            let frame = OwnerFrameIo::read(stream)?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = owner_signal_repository_ledger::Frame::new(
                        owner_signal_repository_ledger::FrameBody::HandshakeReply(
                            handshake_reply_for(request.version()),
                        ),
                    );
                    OwnerFrameIo::write(stream, &reply)?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = store.handle_owner_request(request);
                    let frame = owner_signal_repository_ledger::Frame::new(
                        owner_signal_repository_ledger::FrameBody::Reply { exchange, reply },
                    );
                    OwnerFrameIo::write(stream, &frame)?;
                    return Ok(());
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }

    fn ingest_spool_with_store(
        store: &Arc<Mutex<Store>>,
        spool_directory: &Path,
    ) -> Result<SpoolIngestSummary> {
        let store = store
            .lock()
            .expect("repository ledger store mutex should not be poisoned");
        SpoolDirectory::new(spool_directory).ingest_into(&store)
    }

    fn run_ordinary_listener(listener: UnixListener, store: Arc<Mutex<Store>>) {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    if let Err(error) = Self::serve_ordinary_stream_shared(&store, &mut stream) {
                        eprintln!("(OrdinarySocketError \"{error}\")");
                    }
                }
                Err(error) => eprintln!("(OrdinaryAcceptError \"{error}\")"),
            }
        }
    }

    fn run_owner_listener(listener: UnixListener, store: Arc<Mutex<Store>>) {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    if let Err(error) = Self::serve_owner_stream_shared(&store, &mut stream) {
                        eprintln!("(OwnerSocketError \"{error}\")");
                    }
                }
                Err(error) => eprintln!("(OwnerAcceptError \"{error}\")"),
            }
        }
    }

    fn bind_socket(path: impl AsRef<Path>, mode: u32) -> Result<UnixListener> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if path.exists() {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.file_type().is_socket() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("refusing to replace non-socket path {}", path.display()),
                )));
            }
            fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
        Ok(listener)
    }

    fn serve_ordinary_stream_shared(
        store: &Arc<Mutex<Store>>,
        stream: &mut UnixStream,
    ) -> Result<()> {
        loop {
            let frame = OrdinaryFrameIo::read(stream)?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::HandshakeReply(handshake_reply_for(
                            request.version(),
                        )),
                    );
                    OrdinaryFrameIo::write(stream, &reply)?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = {
                        let store = store
                            .lock()
                            .expect("repository ledger store mutex should not be poisoned");
                        store.handle_ordinary_request(request)
                    };
                    let frame = signal_repository_ledger::Frame::new(
                        signal_repository_ledger::FrameBody::Reply { exchange, reply },
                    );
                    OrdinaryFrameIo::write(stream, &frame)?;
                    return Ok(());
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }

    fn serve_owner_stream_shared(store: &Arc<Mutex<Store>>, stream: &mut UnixStream) -> Result<()> {
        loop {
            let frame = OwnerFrameIo::read(stream)?;
            match frame.into_body() {
                ExchangeFrameBody::HandshakeRequest(request) => {
                    let reply = owner_signal_repository_ledger::Frame::new(
                        owner_signal_repository_ledger::FrameBody::HandshakeReply(
                            handshake_reply_for(request.version()),
                        ),
                    );
                    OwnerFrameIo::write(stream, &reply)?;
                }
                ExchangeFrameBody::Request { exchange, request } => {
                    let reply = {
                        let store = store
                            .lock()
                            .expect("repository ledger store mutex should not be poisoned");
                        store.handle_owner_request(request)
                    };
                    let frame = owner_signal_repository_ledger::Frame::new(
                        owner_signal_repository_ledger::FrameBody::Reply { exchange, reply },
                    );
                    OwnerFrameIo::write(stream, &frame)?;
                    return Ok(());
                }
                _ => return Err(Error::UnexpectedFrame),
            }
        }
    }
}
