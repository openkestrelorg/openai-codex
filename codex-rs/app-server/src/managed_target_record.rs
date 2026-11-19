use crate::managed_transition::MANAGED_PROFILE_ENV_VAR;
use codex_app_server_transport::AppServerTransport;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use uuid::Uuid;

pub(crate) const MANAGED_TARGET_RECORD_ENV_VAR: &str = "KESTREL_CODEX_MANAGED_TARGET_RECORD";

/// Startup-only target publication selected explicitly by a disposable
/// qualification harness. Absence of the opt-in environment variable creates
/// no file and leaves ordinary app-server startup unchanged.
pub(crate) struct ManagedTargetRecordSetup {
    pub(crate) process_instance_id: Option<String>,
    pub(crate) control_endpoint: Option<String>,
    publication: Option<ManagedTargetRecordPublication>,
    published: Option<PublishedTargetRecord>,
}

impl ManagedTargetRecordSetup {
    pub(crate) fn from_env(transport: &AppServerTransport) -> io::Result<Self> {
        let control_endpoint = match transport {
            AppServerTransport::UnixSocket { socket_path } => {
                Some(socket_path.display().to_string())
            }
            _ => None,
        };
        let Some(record_path) = std::env::var_os(MANAGED_TARGET_RECORD_ENV_VAR) else {
            return Ok(Self {
                process_instance_id: None,
                control_endpoint,
                publication: None,
                published: None,
            });
        };
        let AppServerTransport::UnixSocket { socket_path } = transport else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed target record requires a Unix socket transport",
            ));
        };
        let record_path = validated_record_path(record_path)?;
        let profile = required_profile()?;
        let artifact = std::env::current_exe()?.canonicalize()?;
        let process_instance_id = Uuid::now_v7().to_string();
        // This identity names one immutable publication for the process. It
        // is distinct from the process instance because a record publication
        // is independently selected and immutable.
        let record_identity = Uuid::now_v7().to_string();
        let record = ManagedTargetRecord {
            profile,
            artifact: path_text(&artifact)?,
            artifact_sha256: sha256_file(&artifact)?,
            socket: path_text(socket_path.as_path())?,
            pid: std::process::id(),
            process_instance_id: process_instance_id.clone(),
            record_identity,
        };
        Ok(Self {
            process_instance_id: Some(process_instance_id),
            control_endpoint,
            publication: Some(ManagedTargetRecordPublication {
                path: record_path,
                record,
            }),
            published: None,
        })
    }

    pub(crate) fn publish_after_socket_bound(&mut self) -> io::Result<()> {
        let Some(publication) = self.publication.take() else {
            return Ok(());
        };
        self.published = Some(publication.publish()?);
        Ok(())
    }

    /// Removes only the immutable record that this process published when a
    /// later startup step fails before the coordinator is constructed.
    pub(crate) fn remove_after_startup_failure(&mut self) -> io::Result<()> {
        let Some(publication) = self.published.as_ref() else {
            return Ok(());
        };
        publication.remove_if_unchanged()?;
        self.published = None;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedTargetRecord {
    profile: String,
    artifact: String,
    artifact_sha256: String,
    socket: String,
    pid: u32,
    process_instance_id: String,
    record_identity: String,
}

struct ManagedTargetRecordPublication {
    path: PathBuf,
    record: ManagedTargetRecord,
}

#[derive(Debug)]
struct PublishedTargetRecord {
    path: PathBuf,
    // Keeping the published inode open prevents unlink/recreate from reusing
    // its inode number while this rollback authority still exists.
    #[cfg(unix)]
    _publication_file: File,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    record_identity: String,
}

impl ManagedTargetRecordPublication {
    fn publish(self) -> io::Result<PublishedTargetRecord> {
        let parent = self.path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "target record has no parent")
        })?;
        let mut temporary = NamedTempFile::new_in(parent)?;
        serde_json::to_writer(temporary.as_file_mut(), &self.record).map_err(io::Error::other)?;
        temporary.as_file_mut().write_all(b"\n")?;
        temporary.as_file_mut().sync_all()?;

        // Linking a complete same-directory temporary inode publishes the
        // record atomically and refuses if the selected path already exists.
        // Dropping `temporary` removes only its private link.
        #[cfg(unix)]
        let publication_file = temporary.as_file().try_clone()?;
        #[cfg(unix)]
        let metadata = publication_file.metadata()?;
        std::fs::hard_link(temporary.path(), &self.path)?;
        Ok(PublishedTargetRecord {
            path: self.path,
            #[cfg(unix)]
            _publication_file: publication_file,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(not(unix))]
            record_identity: self.record.record_identity,
        })
    }
}

impl PublishedTargetRecord {
    fn remove_if_unchanged(&self) -> io::Result<()> {
        let metadata = std::fs::symlink_metadata(&self.path)?;
        if metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "published target record replaced by a symlink before rollback",
            ));
        }
        #[cfg(unix)]
        let unchanged = metadata.dev() == self.device && metadata.ino() == self.inode;
        #[cfg(not(unix))]
        let unchanged = {
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&self.path)?).map_err(io::Error::other)?;
            record
                .get("recordIdentity")
                .and_then(serde_json::Value::as_str)
                == Some(self.record_identity.as_str())
        };
        if !unchanged {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "published target record identity changed before rollback",
            ));
        }
        // Unlike no-overwrite publication, unlink cannot combine the identity
        // check and removal atomically. Startup has already stopped acceptors;
        // this is best-effort rollback of the record this process published.
        std::fs::remove_file(&self.path)
    }
}

fn validated_record_path(value: OsString) -> io::Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record path must be absolute",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record path has no parent",
        )
    })?;
    if !parent.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "managed target record parent is unavailable",
        ));
    }
    Ok(path)
}

fn required_profile() -> io::Result<String> {
    let profile = std::env::var(MANAGED_PROFILE_ENV_VAR).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record requires a managed profile",
        )
    })?;
    if profile.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record requires a managed profile",
        ));
    }
    Ok(profile)
}

fn path_text(path: &Path) -> io::Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "managed target record path is not UTF-8",
        )
    })
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut source = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn publication(path: PathBuf, artifact: &Path) -> ManagedTargetRecordPublication {
        ManagedTargetRecordPublication {
            path,
            record: ManagedTargetRecord {
                profile: "synthetic".to_owned(),
                artifact: path_text(artifact).unwrap(),
                artifact_sha256: sha256_file(artifact).unwrap(),
                socket: "/tmp/disposable.sock".to_owned(),
                pid: 4242,
                process_instance_id: "process-instance".to_owned(),
                record_identity: "publication-record".to_owned(),
            },
        }
    }

    #[test]
    fn from_env_without_opt_in_publishes_nothing() {
        // Never mutate the process environment; opt-in-present coverage uses
        // disposable parent processes instead of racing this unit test.
        assert!(std::env::var_os(MANAGED_TARGET_RECORD_ENV_VAR).is_none());
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("control.sock");
        let mut setup = ManagedTargetRecordSetup::from_env(&AppServerTransport::UnixSocket {
            socket_path: socket_path.clone(),
        })
        .unwrap();
        assert!(setup.process_instance_id.is_none());
        assert_eq!(
            setup.control_endpoint,
            Some(socket_path.display().to_string())
        );
        assert!(setup.publication.is_none());
        setup.publish_after_socket_bound().unwrap();
        setup.remove_after_startup_failure().unwrap();
        assert!(setup.published.is_none());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn publication_refuses_a_preexisting_symlink_without_following_it() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("codex");
        std::fs::write(&artifact, b"synthetic artifact").unwrap();
        let sentinel = directory.path().join("sentinel.json");
        let preimage = b"sentinel must not change";
        std::fs::write(&sentinel, preimage).unwrap();
        let record_path = directory.path().join("target.json");
        std::os::unix::fs::symlink(&sentinel, &record_path).unwrap();
        let error = publication(record_path.clone(), &artifact)
            .publish()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&sentinel).unwrap(), preimage);
        assert!(record_path.is_symlink());
        assert_eq!(std::fs::read_link(&record_path).unwrap(), sentinel);
    }

    #[test]
    fn publication_writes_complete_expected_record_without_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("codex");
        std::fs::write(&artifact, b"synthetic artifact").unwrap();
        let record_path = directory.path().join("target.json");

        publication(record_path.clone(), &artifact)
            .publish()
            .unwrap();
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
        assert_eq!(record["profile"], "synthetic");
        assert_eq!(record["artifact"], path_text(&artifact).unwrap());
        assert_eq!(
            record["artifactSha256"],
            "169e73adeeb9ee2cbf78b5878c95ebef88d580774a6005a367c8e35458658027"
        );
        assert_eq!(record["socket"], "/tmp/disposable.sock");
        assert_eq!(record["pid"], 4242);
        assert_eq!(record["processInstanceId"], "process-instance");
        assert_eq!(record["recordIdentity"], "publication-record");
        assert_ne!(record["recordIdentity"], record["processInstanceId"]);

        let error = publication(record_path.clone(), &artifact)
            .publish()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(record_path).unwrap())
                .unwrap(),
            record
        );
    }

    #[test]
    fn published_record_cleanup_removes_only_the_matching_publication() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("codex");
        std::fs::write(&artifact, b"synthetic artifact").unwrap();
        let record_path = directory.path().join("target.json");

        let published = publication(record_path.clone(), &artifact)
            .publish()
            .unwrap();
        published.remove_if_unchanged().unwrap();
        assert!(!record_path.exists());

        let published = publication(record_path.clone(), &artifact)
            .publish()
            .unwrap();
        let replacement = std::fs::read(&record_path).unwrap();
        std::fs::remove_file(&record_path).unwrap();
        std::fs::write(&record_path, &replacement).unwrap();

        let error = published.remove_if_unchanged().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(record_path).unwrap(), replacement);
    }

    #[cfg(unix)]
    #[test]
    fn published_record_cleanup_refuses_a_replacement_symlink_to_the_same_inode() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("codex");
        std::fs::write(&artifact, b"synthetic artifact").unwrap();
        let record_path = directory.path().join("target.json");
        let published = publication(record_path.clone(), &artifact)
            .publish()
            .unwrap();
        let retained = directory.path().join("retained.json");
        std::fs::rename(&record_path, &retained).unwrap();
        std::os::unix::fs::symlink(&retained, &record_path).unwrap();

        let error = published.remove_if_unchanged().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(record_path.is_symlink());
        assert!(retained.is_file());
    }
}
