use std::fs;
use std::path::{Path, PathBuf};

use nota_codec::{Decoder, NotaDecode};
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
        let mut decoder = Decoder::new(&self.text);
        decoder.expect_record_head("ReceiveHookNotification")?;
        let repository_name = Name::new(Self::decode_named_string(&mut decoder, "Name")?);
        let gitolite_user =
            GitoliteUser::new(Self::decode_named_string(&mut decoder, "GitoliteUser")?);
        let received_at = Self::decode_received_at(&mut decoder)?;
        let daemon_socket_present = Self::decode_daemon_socket_present(&mut decoder)?;
        let ref_updates = Self::decode_ref_updates(&mut decoder)?;
        decoder.expect_record_end()?;
        Ok(ReceiveHookNotification {
            repository_name,
            gitolite_user,
            received_at,
            daemon_socket_present,
            ref_updates,
        })
    }

    fn decode_received_at(decoder: &mut Decoder<'_>) -> nota_codec::Result<Timestamp> {
        let value = Self::decode_named_string(decoder, "ReceivedAt")?;
        Ok(Timestamp::new(value))
    }

    fn decode_named_string(
        decoder: &mut Decoder<'_>,
        head: &'static str,
    ) -> nota_codec::Result<String> {
        decoder.expect_record_head(head)?;
        let value = String::decode(decoder)?;
        decoder.expect_record_end()?;
        Ok(value)
    }

    fn decode_daemon_socket_present(decoder: &mut Decoder<'_>) -> nota_codec::Result<bool> {
        decoder.expect_record_head("DaemonSocketPresent")?;
        let value = bool::decode(decoder)?;
        decoder.expect_record_end()?;
        Ok(value)
    }

    fn decode_ref_updates(decoder: &mut Decoder<'_>) -> nota_codec::Result<Vec<RefUpdate>> {
        decoder.expect_record_head("RefUpdates")?;
        let mut updates = Vec::new();
        while !decoder.peek_is_record_end()? {
            decoder.expect_record_head("RefUpdate")?;
            let old_object_identifier = String::decode(decoder)?;
            let new_object_identifier = String::decode(decoder)?;
            let ref_name = String::decode(decoder)?;
            decoder.expect_record_end()?;
            updates.push(RefUpdate {
                old_object_identifier: ObjectIdentifier::new(old_object_identifier),
                new_object_identifier: ObjectIdentifier::new(new_object_identifier),
                ref_name: RefName::new(ref_name),
            });
        }
        decoder.expect_record_end()?;
        Ok(updates)
    }
}
