//! Profile identity and storage boundaries.
//!
//! Stores, journals, and uploads are profile-scoped. Repositories, worktrees,
//! agent credentials, UI settings, and the device id remain device-scoped under
//! the engine data directory.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeron_proto::ProfileScope;

use crate::EngineError;

const LOCAL_PROFILE_FILE: &str = "local-profile.json";
const LOCAL_ORGANIZATION_ID: &str = "local";

/// A resolved, immutable identity and storage boundary for one engine runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineProfile {
    scope: ProfileScope,
    device_root: PathBuf,
    store_root: PathBuf,
    uploads_root: PathBuf,
    organization_id: String,
    user_id: String,
}

impl EngineProfile {
    /// Load or create the installation's stable local identity.
    pub fn local(data_dir: &Path) -> Result<Self, EngineError> {
        let profile_id = load_or_create_local_profile_id(data_dir)?;
        let store_root = data_dir.join("profiles").join("local");
        Ok(Self {
            scope: ProfileScope::Local,
            device_root: data_dir.to_path_buf(),
            uploads_root: store_root.join("uploads"),
            store_root,
            organization_id: LOCAL_ORGANIZATION_ID.to_string(),
            user_id: profile_id,
        })
    }

    /// Resolve an authenticated profile under its account-specific storage root.
    pub fn synced(data_dir: &Path, organization_id: &str, user_id: &str) -> Self {
        Self::account_scoped(data_dir, organization_id, user_id, ProfileScope::Synced)
    }

    /// Resolve the explicit development profile under its account-specific storage root.
    pub fn development(data_dir: &Path, organization_id: &str, user_id: &str) -> Self {
        Self::account_scoped(
            data_dir,
            organization_id,
            user_id,
            ProfileScope::Development,
        )
    }

    fn account_scoped(
        data_dir: &Path,
        organization_id: &str,
        user_id: &str,
        scope: ProfileScope,
    ) -> Self {
        let store_root = data_dir
            .join("orgs")
            .join(sanitize_path_id(organization_id))
            .join(sanitize_path_id(user_id));
        Self {
            scope,
            device_root: data_dir.to_path_buf(),
            uploads_root: store_root.join("uploads"),
            store_root,
            organization_id: organization_id.to_string(),
            user_id: user_id.to_string(),
        }
    }

    pub fn scope(&self) -> ProfileScope {
        self.scope
    }

    pub fn device_root(&self) -> &Path {
        &self.device_root
    }

    pub fn store_root(&self) -> &Path {
        &self.store_root
    }

    pub fn uploads_root(&self) -> &Path {
        &self.uploads_root
    }

    pub fn organization_id(&self) -> &str {
        &self.organization_id
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalProfileFile {
    id: Uuid,
}

fn load_or_create_local_profile_id(data_dir: &Path) -> Result<String, EngineError> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join(LOCAL_PROFILE_FILE);
    match read_local_profile_id(&path) {
        Ok(id) => return Ok(id),
        Err(ProfileReadError::Missing) => {}
        Err(ProfileReadError::Engine(err)) => return Err(err),
    }

    let id = Uuid::new_v4();
    let mut bytes = serde_json::to_vec_pretty(&LocalProfileFile { id })
        .map_err(|err| EngineError::Other(format!("serialize local profile: {err}")))?;
    bytes.push(b'\n');

    // Publish a fully-written same-directory file without replacing a profile
    // another process may have created concurrently.
    let temp_path = data_dir.join(format!(
        ".{LOCAL_PROFILE_FILE}.tmp-{}-{}",
        std::process::id(),
        Uuid::new_v4()
    ));
    let write_result = (|| -> Result<(), EngineError> {
        let mut temp = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        temp.write_all(&bytes)?;
        temp.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }

    let publish_result = std::fs::hard_link(&temp_path, &path);
    let _ = std::fs::remove_file(&temp_path);
    match publish_result {
        Ok(()) => Ok(id.to_string()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            read_local_profile_id(&path).map_err(ProfileReadError::into_engine)
        }
        Err(err) => Err(err.into()),
    }
}

enum ProfileReadError {
    Missing,
    Engine(EngineError),
}

impl ProfileReadError {
    fn into_engine(self) -> EngineError {
        match self {
            Self::Missing => EngineError::Other("local profile disappeared during creation".into()),
            Self::Engine(err) => err,
        }
    }
}

fn read_local_profile_id(path: &Path) -> Result<String, ProfileReadError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(ProfileReadError::Missing);
        }
        Err(err) => return Err(ProfileReadError::Engine(err.into())),
    };
    let profile: LocalProfileFile = serde_json::from_slice(&bytes).map_err(|err| {
        ProfileReadError::Engine(EngineError::Other(format!(
            "invalid local profile {}: {err}",
            path.display()
        )))
    })?;
    if profile.id.is_nil() {
        return Err(ProfileReadError::Engine(EngineError::Other(format!(
            "invalid local profile {}: id must not be nil",
            path.display()
        ))));
    }
    Ok(profile.id.to_string())
}

fn sanitize_path_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_profile_id_is_stable_and_not_the_dev_identity() {
        let dir = tempfile::tempdir().unwrap();
        let first = EngineProfile::local(dir.path()).unwrap();
        let second = EngineProfile::local(dir.path()).unwrap();

        assert!(!first.user_id().is_empty());
        assert_ne!(first.user_id(), "dev-user");
        assert_eq!(first.user_id(), second.user_id());
        assert!(Uuid::parse_str(first.user_id()).is_ok());
    }

    #[test]
    fn local_profiles_in_different_data_dirs_have_different_ids() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();

        let first = EngineProfile::local(first_dir.path()).unwrap();
        let second = EngineProfile::local(second_dir.path()).unwrap();

        assert_ne!(first.user_id(), second.user_id());
    }

    #[test]
    fn local_profile_resolves_isolated_store_and_upload_roots() {
        let dir = tempfile::tempdir().unwrap();
        let profile = EngineProfile::local(dir.path()).unwrap();
        let local_root = dir.path().join("profiles/local");

        assert_eq!(profile.scope(), ProfileScope::Local);
        assert_eq!(profile.store_root(), local_root);
        assert_eq!(profile.uploads_root(), local_root.join("uploads"));
    }

    #[test]
    fn synced_profile_uses_account_scoped_roots() {
        let dir = tempfile::tempdir().unwrap();
        let profile = EngineProfile::synced(dir.path(), "org/example", "user@example.com");

        assert_eq!(profile.scope(), ProfileScope::Synced);
        assert_eq!(
            profile.store_root(),
            dir.path().join("orgs/org_example/user_example_com")
        );
        assert_eq!(
            profile.uploads_root(),
            dir.path().join("orgs/org_example/user_example_com/uploads")
        );
    }

    #[tokio::test]
    async fn profile_assembler_uses_the_resolved_local_roots() {
        let dir = tempfile::tempdir().unwrap();
        let profile = EngineProfile::local(dir.path()).unwrap();
        let core = crate::EngineCore::assemble_with_profile(
            profile,
            std::sync::Arc::new(crate::default_registry()),
            zeron_proto::HarnessId::Mock,
            None,
        )
        .unwrap();

        assert_eq!(core.profile_scope(), ProfileScope::Local);
        assert_eq!(
            core.uploads.dir(),
            dir.path().join("profiles/local/uploads")
        );
        assert!(dir.path().join("profiles/local").is_dir());
        assert!(!dir.path().join("orgs/dev-org/dev-user").exists());
        core.shutdown().await;
    }

    #[tokio::test]
    async fn development_constructor_uses_account_scoped_uploads() {
        let dir = tempfile::tempdir().unwrap();
        let organization_id = crate::organization_env_or(crate::DEFAULT_ORGANIZATION_ID);
        let user_id = crate::env_or("ZERON_USER_ID", crate::DEFAULT_USER_ID);
        let expected = EngineProfile::development(dir.path(), &organization_id, &user_id);
        let core = crate::EngineCore::assemble(
            dir.path(),
            std::sync::Arc::new(crate::default_registry()),
            zeron_proto::HarnessId::Mock,
            None,
        )
        .unwrap();

        assert_eq!(core.profile_scope(), ProfileScope::Development);
        assert_eq!(core.uploads.dir(), expected.store_root().join("uploads"));
        assert!(expected.store_root().is_dir());
        if std::env::var("ZERON_ORGANIZATION_ID").is_err()
            && std::env::var("ZERON_USER_ID").is_err()
        {
            assert_eq!(
                expected.store_root(),
                dir.path().join("orgs/dev-org/dev-user")
            );
        }
        core.shutdown().await;
    }
}
