use std::fs;
use std::path::{Path, PathBuf};

use nota_next::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaSource};
use signal_repository_ledger::{
    GitoliteUser, Name, ObjectIdentifier, ReceiveHookNotification, RefName, RefUpdate, Timestamp,
};

use crate::{Result, Store};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolIngestSummary {
    pub files_seen: usize,
    pub files_recorded: usize,
}

impl SpoolIngestSummary {
    pub const fn empty() -> Self {
        Self {
            files_seen: 0,
            files_recorded: 0,
        }
    }

    fn record_file(&mut self) {
        self.files_seen += 1;
        self.files_recorded += 1;
    }
}

pub struct SpoolDirectory {
    path: PathBuf,
}

impl SpoolDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn ingest_into(&self, store: &Store) -> Result<SpoolIngestSummary> {
        if !self.path.exists() {
            return Ok(SpoolIngestSummary::empty());
        }
        let processed_directory = self.path.join("processed");
        fs::create_dir_all(&processed_directory)?;

        let mut candidates = Vec::new();
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("nota")
            {
                candidates.push(path);
            }
        }
        candidates.sort();

        let mut summary = SpoolIngestSummary::empty();
        for path in candidates {
            let notification = SpoolNotificationFile::from_path(&path)?.decode()?;
            store.record_hook_notification(notification)?;
            Self::move_to_processed(&path, &processed_directory)?;
            summary.record_file();
        }
        Ok(summary)
    }

    fn move_to_processed(path: &Path, processed_directory: &Path) -> Result<()> {
        let file_name = path
            .file_name()
            .expect("spool candidate path came from a directory entry");
        fs::rename(path, processed_directory.join(file_name))?;
        Ok(())
    }
}

struct SpoolNotificationFile {
    text: String,
}

impl SpoolNotificationFile {
    fn from_path(path: &Path) -> Result<Self> {
        Ok(Self {
            text: fs::read_to_string(path)?,
        })
    }

    fn decode(&self) -> Result<ReceiveHookNotification> {
        let root = NotaSource::new(&self.text).parse_root()?;
        let fields = Self::expect_record_body(&root, "ReceiveHookNotification")?;
        if fields.len() != 5 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "ReceiveHookNotification",
                expected: 5,
                found: fields.len(),
            }
            .into());
        }
        let repository_name = Name::new(Self::decode_named_string(&fields[0], "Name")?);
        let gitolite_user =
            GitoliteUser::new(Self::decode_named_string(&fields[1], "GitoliteUser")?);
        let received_at = Self::decode_received_at(&fields[2])?;
        let daemon_socket_present = Self::decode_daemon_socket_present(&fields[3])?;
        let ref_updates = Self::decode_ref_updates(&fields[4])?;
        Ok(ReceiveHookNotification {
            repository_name,
            gitolite_user,
            received_at,
            daemon_socket_present,
            ref_updates,
        })
    }

    fn expect_record_body<'block>(
        block: &'block Block,
        head: &'static str,
    ) -> std::result::Result<&'block [Block], NotaDecodeError> {
        let children = NotaBlock::new(block).expect_delimited(Delimiter::Parenthesis, head)?;
        let actual = children
            .first()
            .and_then(|block| block.demote_to_string())
            .ok_or(NotaDecodeError::ExpectedAtom { type_name: head })?;
        if actual != head {
            return Err(NotaDecodeError::UnknownVariant {
                enum_name: head,
                variant: actual.to_owned(),
            });
        }
        Ok(&children[1..])
    }

    fn decode_received_at(block: &Block) -> std::result::Result<Timestamp, NotaDecodeError> {
        let value = Self::decode_named_string(block, "ReceivedAt")?;
        Ok(Timestamp::new(value))
    }

    fn decode_named_string(
        block: &Block,
        head: &'static str,
    ) -> std::result::Result<String, NotaDecodeError> {
        let fields = Self::expect_record_body(block, head)?;
        if fields.len() != 1 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: head,
                expected: 1,
                found: fields.len(),
            });
        }
        String::from_nota_block(&fields[0])
    }

    fn decode_daemon_socket_present(block: &Block) -> std::result::Result<bool, NotaDecodeError> {
        let fields = Self::expect_record_body(block, "DaemonSocketPresent")?;
        if fields.len() != 1 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "DaemonSocketPresent",
                expected: 1,
                found: fields.len(),
            });
        }
        bool::from_nota_block(&fields[0])
    }

    fn decode_ref_updates(block: &Block) -> std::result::Result<Vec<RefUpdate>, NotaDecodeError> {
        let update_blocks = Self::expect_record_body(block, "RefUpdates")?;
        let mut updates = Vec::new();
        for update_block in update_blocks {
            let fields = Self::expect_record_body(update_block, "RefUpdate")?;
            if fields.len() != 3 {
                return Err(NotaDecodeError::ExpectedRootCount {
                    type_name: "RefUpdate",
                    expected: 3,
                    found: fields.len(),
                });
            }
            let old_object_identifier = String::from_nota_block(&fields[0])?;
            let new_object_identifier = String::from_nota_block(&fields[1])?;
            let ref_name = String::from_nota_block(&fields[2])?;
            updates.push(RefUpdate {
                old_object_identifier: ObjectIdentifier::new(old_object_identifier),
                new_object_identifier: ObjectIdentifier::new(new_object_identifier),
                ref_name: RefName::new(ref_name),
            });
        }
        Ok(updates)
    }
}
