//! zeron-engine — the headless backend: sessions engine, doc host + command executor,
//! run journal + crash recovery, and the IPC RPC server.
//!
//! Spec: ARCHITECTURE.md §5 and docs/research/feature-inventory.md §3. M2 surface:
//! sessions + docs + commands + minimal IPC. Terminals, repos/diffs, uploads, auth,
//! agent accounts, and the device-room host land in later milestones.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
pub use zeron_proto::{EngineInfo, HarnessId, ProfileIdentity, ProfileScope};
use zeron_rpc::{RpcError, RpcReply, RpcService, methods};

use zeron_sync::DocsStore;

pub mod agent_accounts;
pub mod auth;
pub mod change_requests;
pub mod chat2_host;
pub mod diff_sync;
pub mod doc_host;
pub mod instance_lock;
pub mod local_import;
pub mod profile;
pub mod registry;
pub mod registry_host;
pub mod repos;
pub mod rpc;
pub mod run_journal;
pub mod sessions;
pub mod source_control;
pub mod spaces;
pub mod terminals;
pub mod titles;
pub mod uploads;

pub use agent_accounts::{AgentAccounts, AgentAccountsConfig};
pub use auth::{Auth, AuthConfig, AuthState, AuthUser, OrganizationMembership};
pub use change_requests::{ChangeRequestCacheKey, CheckoutChangeRequests};
pub use diff_sync::{
    CheckoutDiffSync, DiffFileTextPair, DiffSnapshot, TurnSnapshot, capture_commit_diff,
    capture_diff, capture_diff_against, capture_turn_diff, merge_base, read_diff_file_text,
    snapshot_tree, working_diff_base,
};
pub use doc_host::{ChatDocHandle, DocHost, DocHostConfig, EdgeConfig};
pub use instance_lock::InstanceLock;
pub use profile::EngineProfile;
pub use registry::{HarnessDescriptor, HarnessRegistry, default_registry};
pub use registry_host::{
    DEFAULT_ORGANIZATION_ID, DEFAULT_USER_ID, LEGACY_WORKSPACE_V2_DOC_ID, RegistryHost,
    RegistryHostConfig,
};
pub use repos::{CheckoutIdentity, Repos, worktree_branch_from_title};
pub use rpc::EngineRpc;
pub use run_journal::{JournalError, RunJournal};
pub use sessions::{JournaledEvent, SessionsEngine, SteerOutcome};
pub use source_control::{
    BranchHeadContext, ChangeRequestError, ChangeRequestProvider, ChangeRequestResolution,
    ChangeRequestResolver, CheckoutChangeRequestLookup, CheckoutSourceContext, GitHubCli,
    GitRemote, parse_git_remote,
};
pub use spaces::SpacesSync;
pub use terminals::Terminals;
pub use titles::TitleGenerator;
pub use uploads::{AttachmentChunk, Uploads};

pub(crate) const LEGACY_UNKNOWN_DEVICE_NAME: &str = "unknown-device";

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("doc: {0}")]
    Doc(#[from] zeron_doc::DocError),
    #[error("journal: {0}")]
    Journal(#[from] run_journal::JournalError),
    #[error("store: {0}")]
    Store(#[from] zeron_sync::StoreError),
    #[error("harness: {0}")]
    Harness(#[from] zeron_harness::HarnessError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

/// Epoch millis now — the doc/journal timestamp base.
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Token provider bound to one immutable engine profile. Authentication is
/// intentionally mutable so WorkOS can refresh and select another Organization,
/// but an already-open runtime must never carry that new user or organization bearer into
/// its old stores, rooms, relay, or peer links during the shutdown handoff.
struct ProfileTokenSource {
    source: Arc<dyn zeron_rpc::TokenSource>,
    expected_profile: Option<ProfileIdentity>,
}

impl ProfileTokenSource {
    fn new(
        source: Arc<dyn zeron_rpc::TokenSource>,
        expected_profile: Option<ProfileIdentity>,
    ) -> Self {
        Self {
            source,
            expected_profile,
        }
    }
}

#[async_trait]
impl zeron_rpc::TokenSource for ProfileTokenSource {
    async fn token(&self) -> Option<String> {
        let token = self.source.token().await?;
        let Some(expected) = self.expected_profile.as_ref() else {
            return Some(token);
        };
        let actual = jwt_profile(&token);
        if actual.as_ref() != Some(expected) {
            tracing::warn!(
                expected_organization = expected.organization_id,
                expected_user = expected.user_id,
                actual_organization = actual
                    .as_ref()
                    .map(|profile| profile.organization_id.as_str())
                    .unwrap_or("<missing>"),
                actual_user = actual
                    .as_ref()
                    .map(|profile| profile.user_id.as_str())
                    .unwrap_or("<missing>"),
                "blocked bearer outside the runtime's fixed user and Organization profile"
            );
            return None;
        }
        Some(token)
    }

    fn subscribe(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.source.subscribe()
    }
}

fn jwt_profile(token: &str) -> Option<ProfileIdentity> {
    #[derive(serde::Deserialize)]
    struct Claims {
        #[serde(rename = "org_id")]
        organization_id: Option<String>,
        sub: Option<String>,
    }

    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims = serde_json::from_slice::<Claims>(&bytes).ok()?;
    Some(ProfileIdentity {
        organization_id: claims.organization_id?,
        user_id: claims.sub?,
    })
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Data directory (default `~/.zeron`, dev `~/.zeron-dev`).
    pub data_dir: PathBuf,
    /// Edge base URL.
    pub edge_url: String,
    /// Explicit development bearer for edge room joins. Synced WorkOS runtimes
    /// obtain their bearer from [`Auth`]; development stays offline when this is absent.
    pub edge_token: Option<String>,
    /// Localhost IPC port for the UI.
    pub ipc_port: u16,
    /// Harness for doc-command runs on chats without a registry `config` row.
    pub default_harness: HarnessId,
    /// Organization that scopes the registry. `None` = `$ZERON_ORGANIZATION_ID` or the dev default.
    /// `$ZERON_ORG_ID` remains a read-only legacy fallback.
    /// In WorkOS mode the signed-in session's organization wins.
    pub organization_id: Option<String>,
    /// WorkOS client id — enables real auth; `None` = dev mode (bearer = `edge_token`).
    pub workos_client_id: Option<String>,
}

/// The assembled engine core — also constructible without the IPC server for tests
/// and the in-process (headed) mode.
pub struct EngineCore {
    pub sessions: SessionsEngine,
    pub doc_host: DocHost,
    pub registry: RegistryHost,
    pub harness_registry: Arc<HarnessRegistry>,
    pub repos: Repos,
    pub terminals: Terminals,
    pub change_requests: CheckoutChangeRequests,
    pub diff_sync: CheckoutDiffSync,
    pub spaces_sync: SpacesSync,
    pub uploads: Uploads,
    pub agent_accounts: AgentAccounts,
    pub device_id: String,
    /// Local→synced profile import (account-scoped runtimes only).
    pub local_import: Option<local_import::LocalImporter>,
    profile_scope: ProfileScope,
    profile_identity: ProfileIdentity,
    /// Auth service (attached by [`Engine::run`]; a lazy dev-mode instance otherwise).
    auth: std::sync::Mutex<Option<Auth>>,
    /// Peer link cache for `targetDeviceId` routing (attached when edge+auth are ready).
    links: std::sync::Mutex<Option<Arc<zeron_rpc::LinkCache>>>,
    /// Release checker (attached by [`Engine::assemble_runtime`]) — the
    /// UpdateStatus stream + ApplyUpdate.
    updater: std::sync::Mutex<Option<zeron_update::Updater>>,
    /// The updater's token-change wake forwarder — owned so shutdown can end it.
    updater_wake: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Exclusive data-dir lock — held for the engine's lifetime (single-instance).
    _instance_lock: InstanceLock,
}

impl EngineCore {
    /// Open stores under `data_dir`, wire sessions ⇄ doc host ⇄ registry host, and
    /// recover stale journals from a previous crash. Identity comes from
    /// `$ZERON_ORGANIZATION_ID` / `$ZERON_USER_ID` (dev defaults `dev-org` / `dev-user`);
    /// use [`Self::assemble_with_identity`] to pass one explicitly.
    pub fn assemble(
        data_dir: &Path,
        harness_registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
        edge: Option<EdgeConfig>,
    ) -> Result<Self, EngineError> {
        let organization_id = organization_env_or(DEFAULT_ORGANIZATION_ID);
        let user_id = env_or("ZERON_USER_ID", DEFAULT_USER_ID);
        let profile = EngineProfile::development(data_dir, &organization_id, &user_id);
        Self::assemble_with_profile(profile, harness_registry, default_harness, edge)
    }

    pub fn assemble_with_identity(
        data_dir: &Path,
        harness_registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
        edge: Option<EdgeConfig>,
        organization_id: &str,
        user_id: &str,
    ) -> Result<Self, EngineError> {
        let profile = EngineProfile::synced(data_dir, organization_id, user_id);
        Self::assemble_with_profile(profile, harness_registry, default_harness, edge)
    }

    /// Assemble the engine against one resolved, immutable registry profile.
    pub fn assemble_with_profile(
        profile: EngineProfile,
        harness_registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
        edge: Option<EdgeConfig>,
    ) -> Result<Self, EngineError> {
        let data_dir = profile.device_root();
        std::fs::create_dir_all(data_dir)?;
        // Single-instance guard: two engines on one data dir would race the
        // SQLite snapshots + journals. Taken before any store opens or the IPC
        // port binds; held (and kernel-released on crash) for the engine's life.
        let lock = InstanceLock::acquire(data_dir)?;
        Self::assemble_with_profile_locked(profile, harness_registry, default_harness, edge, lock)
    }

    /// Assemble against a pre-acquired [`InstanceLock`]. The headed app takes
    /// the lock before binding the IPC port so the listener owner and the
    /// data-dir owner cannot diverge when several viewports bootstrap at once.
    pub fn assemble_with_profile_locked(
        profile: EngineProfile,
        harness_registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
        edge: Option<EdgeConfig>,
        lock: InstanceLock,
    ) -> Result<Self, EngineError> {
        let profile_identity = ProfileIdentity {
            user_id: profile.user_id().to_string(),
            organization_id: profile.organization_id().to_string(),
        };
        let data_dir = profile.device_root();
        std::fs::create_dir_all(data_dir)?;
        let legacy_uploads_root = profile.claim_legacy_uploads_root()?;
        let device_id = load_or_create_device_id(data_dir)?;
        // This device's harness enablement (Settings → Agents) rides the
        // engine data dir — per-device, like the CLI installs it gates.
        harness_registry.load_prefs(data_dir);
        let store = Arc::new(DocsStore::open(profile.store_root())?);
        let store_for_import = store.clone();
        let journal = Arc::new(RunJournal::open(profile.store_root().join("journals"))?);
        let sessions = SessionsEngine::new(device_id.clone(), journal, harness_registry.clone());
        let doc_host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: device_id.clone(),
                user_id: profile.user_id().to_string(),
                default_harness,
                edge: edge.clone(),
            },
        );
        let registry = RegistryHost::open(
            store,
            RegistryHostConfig {
                device_id: device_id.clone(),
                device_name: local_device_name(&device_id),
                platform: std::env::consts::OS.to_string(),
                organization_id: profile.organization_id().to_string(),
                user_id: profile.user_id().to_string(),
                edge: edge.clone(),
            },
        )?;
        doc_host.set_registry(registry.clone());
        doc_host.set_sessions(sessions.clone());
        sessions.set_doc_host(doc_host.clone());
        match sessions.recover_stale() {
            Ok(0) => {}
            Ok(recovered) => tracing::info!(recovered, "stale sessions recovered on boot"),
            Err(err) => tracing::error!(error = %err, "stale-session recovery failed"),
        }
        doc_host.spawn_transcript_salvage(profile.store_root().join("journals"));
        let repos = Repos::new(data_dir, &device_id);
        doc_host.set_repos(repos.clone());
        let change_requests = CheckoutChangeRequests::start(repos.clone(), &device_id);
        let terminals = Terminals::new();
        let uploads = Uploads::from_root_with_fallback(
            profile.uploads_root(),
            legacy_uploads_root.as_deref(),
        );
        // A recorded local→synced import grants this account the local
        // profile's uploads root read-only — transcripts imported earlier
        // embed absolute paths under it (same shape as the legacy adoption).
        if profile.scope() != ProfileScope::Local
            && let Some(root) = local_import::marker_grants_read_root(
                data_dir,
                profile.organization_id(),
                profile.user_id(),
            )
        {
            uploads.add_read_only_root(&root);
        }
        // Queued-attachment support: the doc host resolves `pending://` refs
        // against this store and pushes staged bytes to remote hosts.
        doc_host.set_uploads(uploads.clone());
        let local_import = (profile.scope() == ProfileScope::Synced).then(|| {
            local_import::LocalImporter::new(
                data_dir,
                &device_id,
                profile.organization_id(),
                profile.user_id(),
                store_for_import.clone(),
                profile.store_root().join("journals"),
                registry.clone(),
                uploads.clone(),
            )
        });
        let agent_accounts = AgentAccounts::new(AgentAccountsConfig::detect(data_dir));
        sessions.set_titles(TitleGenerator::new(
            registry.clone(),
            harness_registry.clone(),
            repos.clone(),
        ));
        let diff_sync = CheckoutDiffSync::start(repos.clone(), registry.clone(), &device_id);
        // Turn starts snapshot the checkout tree — the "Latest turn" diff base.
        let turn_diff = diff_sync.clone();
        sessions.set_turn_listener(Arc::new(move |chat_id, cwd| {
            turn_diff.note_turn_start(chat_id, cwd);
        }));
        let spaces_sync = SpacesSync::start(repos.clone(), registry.clone(), &device_id);
        Ok(Self {
            sessions,
            doc_host,
            registry,
            harness_registry,
            repos,
            terminals,
            change_requests,
            diff_sync,
            spaces_sync,
            uploads,
            agent_accounts,
            device_id,
            local_import,
            profile_scope: profile.scope(),
            profile_identity,
            auth: std::sync::Mutex::new(None),
            links: std::sync::Mutex::new(None),
            updater: std::sync::Mutex::new(None),
            updater_wake: std::sync::Mutex::new(None),
            _instance_lock: lock,
        })
    }

    pub fn profile_scope(&self) -> ProfileScope {
        self.profile_scope
    }

    /// Attach the auth service (before building the RPC service / relays).
    pub fn set_auth(&self, auth: Auth) {
        *self
            .auth
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(auth);
    }

    /// The attached auth service, or a lazily-created dev-mode one (in-process embeds
    /// that never wired WorkOS still answer AuthStatus honestly).
    pub fn auth(&self) -> Auth {
        let mut slot = self
            .auth
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.get_or_insert_with(|| {
            let dev_user = std::env::var("ZERON_EDGE_TOKEN")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "dev-user".into());
            let mut config = AuthConfig::new("http://localhost:27640", std::env::temp_dir());
            config.dev_user_id = dev_user;
            Auth::new(config)
        })
        .clone()
    }

    /// Attach the peer link cache — enables `targetDeviceId` routing,
    /// [`Self::dial_device`], and the doc host's queued-attachment transfers.
    pub fn set_links(&self, links: Arc<zeron_rpc::LinkCache>) {
        self.doc_host.set_links(links.clone());
        *self
            .links
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(links);
    }

    pub fn links(&self) -> Option<Arc<zeron_rpc::LinkCache>> {
        self.links
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Attach the release checker (before building the RPC service).
    pub fn set_updater_wake(&self, handle: tokio::task::JoinHandle<()>) {
        *self
            .updater_wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
    }

    pub fn set_updater(&self, updater: zeron_update::Updater) {
        *self
            .updater
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(updater);
    }

    pub fn updater(&self) -> Option<zeron_update::Updater> {
        self.updater
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// A live RPC client to another device's engine through its relay DO (the router's
    /// dial seam). Cached per device; invalidated + re-dialed on failure.
    pub async fn dial_device(
        &self,
        device_id: &str,
    ) -> Result<Arc<zeron_rpc::RpcClient>, EngineError> {
        let links = self
            .links()
            .ok_or_else(|| EngineError::Other("peer links unavailable (offline)".into()))?;
        links
            .client(device_id)
            .await
            .map_err(|e| EngineError::Other(e.to_string()))
    }

    /// Start hosting our device room: serve the full RPC surface to relay clients and
    /// warm-open chat docs on nudges (§7 cold-chat command delivery). The token source
    /// re-reads auth on every (re)dial, so token refreshes take effect at reconnect.
    pub fn start_host_relay(
        &self,
        edge_url: &str,
        token: Arc<dyn zeron_rpc::TokenSource>,
    ) -> zeron_rpc::HostRelay {
        let config = zeron_rpc::HostRelayConfig::new(edge_url, self.device_id.clone(), token);
        let doc_host = self.doc_host.clone();
        let on_nudge: zeron_rpc::NudgeHandler = Arc::new(move |chat_id: String| {
            // Opening the doc joins its room + syncs; drain fires on the change
            // subscription — the command executes with no standing per-chat socket.
            match doc_host.open(&chat_id) {
                Ok(_) => tracing::info!(chat = %chat_id, "nudge: chat doc opened"),
                Err(err) => {
                    tracing::warn!(chat = %chat_id, error = %err, "nudge: open failed")
                }
            }
        });
        zeron_rpc::HostRelay::spawn(config, self.rpc_service(), on_nudge)
    }

    pub fn rpc_service(&self) -> Arc<EngineRpc> {
        let mut rpc = EngineRpc::new(
            self.sessions.clone(),
            self.doc_host.clone(),
            self.registry.clone(),
            self.harness_registry.clone(),
            self.repos.clone(),
            self.terminals.clone(),
            self.change_requests.clone(),
            self.diff_sync.clone(),
            self.uploads.clone(),
            self.agent_accounts.clone(),
            self.profile_scope,
        )
        .with_profile_identity(self.profile_identity.clone())
        .with_auth(self.auth());
        if let Some(links) = self.links() {
            rpc = rpc.with_links(links);
        }
        if let Some(updater) = self.updater() {
            rpc = rpc.with_updater(updater);
        }
        if let Some(importer) = self.local_import.clone() {
            rpc = rpc.with_local_import(importer);
        }
        Arc::new(rpc)
    }

    /// Revoke every account-scoped transport before any slower graceful
    /// draining. Connected sockets remain authorized by their handshake, so
    /// clearing credentials alone is not a security boundary.
    pub fn disconnect_edge(&self) {
        if let Some(links) = self.links() {
            links.disconnect_all();
        }
        self.doc_host.disconnect_edge();
        self.registry.disconnect_edge();
    }

    /// Graceful teardown: settle live runs (streaming entries stamped `aborted`),
    /// kill live PTYs, stamp our registry `lastSeenAt`, and flush every open doc
    /// snapshot.
    pub async fn shutdown(&self) {
        self.sessions.shutdown().await;
        self.terminals.shutdown();
        self.agent_accounts.shutdown();
        self.change_requests.shutdown();
        // Cancel + await every worker that can reach Edge before flushing: a
        // replaced synced runtime must not keep polling releases or draining
        // the attachment outbox under the old identity after Local boots.
        let wake = self
            .updater_wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(wake) = wake {
            wake.abort();
            let _ = wake.await;
        }
        let updater = self
            .updater
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(updater) = updater {
            updater.shutdown().await;
        }
        self.diff_sync.shutdown().await;
        self.spaces_sync.shutdown().await;
        self.doc_host.shutdown_workers().await;
        self.doc_host.flush_all();
        self.registry.shutdown();
        // Break the sessions ⇄ doc-host retain cycle so the replaced graph can
        // actually be freed once the last handle drops.
        self.sessions.clear_doc_host();
    }
}

pub struct Engine {
    pub config: EngineConfig,
}

/// A fully assembled identity-scoped engine plus the relay handle whose lifetime
/// keeps this device reachable. Used by both the headless server and the headed
/// in-process engine so their production authentication paths cannot diverge.
pub struct EngineRuntime {
    core: EngineCore,
    host_relay: std::sync::Mutex<Option<zeron_rpc::HostRelay>>,
}

/// IPC-only lifecycle control owned by `zeron headless`. The regular
/// [`EngineRpc`] deliberately does not expose this method, so a viewport
/// attached to another headed process cannot shut down that process's engine.
struct HeadlessRpc {
    inner: Arc<dyn RpcService>,
    stop_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait]
impl RpcService for HeadlessRpc {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        if method != methods::STOP_ENGINE {
            return self.inner.handle(method, params).await;
        }

        let stop_tx = self.stop_tx.clone();
        // Let the unary success frame reach the client before `Engine::run`
        // aborts the IPC server and drains the runtime.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let _ = stop_tx.send(());
        });
        RpcReply::value(&serde_json::json!({ "ok": true }))
    }
}

impl EngineRuntime {
    pub fn core(&self) -> &EngineCore {
        &self.core
    }

    pub fn profile_scope(&self) -> ProfileScope {
        self.core.profile_scope()
    }

    pub fn disconnect_edge(&self) {
        // Revoke remote reachability before graceful draining. Sessions may
        // need time to settle; no authenticated relay RPC may enter during
        // that window after sign-out.
        self.host_relay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.core.disconnect_edge();
    }

    pub async fn shutdown(&self) {
        self.disconnect_edge();
        self.core.shutdown().await;
    }
}

impl Drop for EngineRuntime {
    fn drop(&mut self) {
        self.host_relay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self { config }
    }

    /// Resolve the shared dev/WorkOS auth configuration for headed and headless
    /// modes. A clean WorkOS boot deliberately avoids probing Edge: signed-out
    /// installations must be able to start locally without network access.
    pub async fn build_auth(config: &EngineConfig) -> Auth {
        let mut auth_config = AuthConfig::new(config.edge_url.clone(), config.data_dir.clone());
        auth_config.workos_client_id = config.workos_client_id.clone();
        if let Ok(base) = std::env::var("ZERON_WORKOS_API_BASE")
            && !base.trim().is_empty()
        {
            auth_config.workos_api_base = base;
        }
        auth_config.callback_port = Some(
            std::env::var("ZERON_CALLBACK_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(27641),
        );
        if let Some(token) = &config.edge_token {
            auth_config.dev_user_id = token.clone();
        }
        Auth::new(auth_config)
    }

    /// Capture the registry boundary once, before refresh or sign-in can mutate auth.
    pub fn initial_profile_scope(auth: &Auth) -> ProfileScope {
        if !auth.workos_enabled() {
            ProfileScope::Development
        } else if auth.loaded_workos_session() {
            ProfileScope::Synced
        } else {
            ProfileScope::Local
        }
    }

    /// Resolve a profile for the captured scope. A synced session without an
    /// organization returns `None` until onboarding selects one; it never falls
    /// through to the local or development profile.
    pub fn resolve_profile(
        config: &EngineConfig,
        auth: &Auth,
        scope: ProfileScope,
    ) -> Result<Option<EngineProfile>, EngineError> {
        match scope {
            ProfileScope::Local => EngineProfile::local(&config.data_dir).map(Some),
            ProfileScope::Development => {
                let dev_token_organization = config
                    .edge_token
                    .as_deref()
                    .and_then(|token| token.split_once('@'))
                    .map(|(_, organization)| organization.to_string())
                    .filter(|organization| !organization.is_empty());
                let organization_id = dev_token_organization
                    .or(config.organization_id.clone())
                    .unwrap_or_else(|| organization_env_or(DEFAULT_ORGANIZATION_ID));
                let user_id = auth
                    .user_id()
                    .unwrap_or_else(|| env_or("ZERON_USER_ID", DEFAULT_USER_ID));
                Ok(Some(EngineProfile::development(
                    &config.data_dir,
                    &organization_id,
                    &user_id,
                )))
            }
            ProfileScope::Synced => {
                let state = auth.state();
                let Some(user) = state.user() else {
                    return Err(EngineError::Other(
                        "captured synced session no longer exposes its user identity".into(),
                    ));
                };
                let Some(organization_id) = state.organization_id() else {
                    return Ok(None);
                };
                Ok(Some(EngineProfile::synced(
                    &config.data_dir,
                    organization_id,
                    &user.id,
                )))
            }
        }
    }

    /// Resolve the one-shot identity served before profile stores are available.
    pub fn engine_info(
        config: &EngineConfig,
        profile_scope: ProfileScope,
    ) -> Result<EngineInfo, EngineError> {
        Self::engine_info_for_profile(config, profile_scope, None)
    }

    /// Resolve engine identity with the immutable profile, when bootstrap has
    /// already captured one. Kept separate so existing EngineInfo callers that
    /// run before profile onboarding remain source-compatible and fail closed.
    pub fn engine_info_for_profile(
        config: &EngineConfig,
        profile_scope: ProfileScope,
        profile: Option<&EngineProfile>,
    ) -> Result<EngineInfo, EngineError> {
        std::fs::create_dir_all(&config.data_dir)?;
        Ok(EngineInfo {
            device_id: load_or_create_device_id(&config.data_dir)?,
            profile_scope,
            profile: profile.map(|profile| ProfileIdentity {
                user_id: profile.user_id().to_string(),
                organization_id: profile.organization_id().to_string(),
            }),
        })
    }

    /// Open one already-resolved profile. Synced profiles always keep their
    /// Edge supervisors alive; temporary token or network failures are runtime
    /// states, not a reason to permanently assemble an offline engine.
    pub async fn assemble_runtime(
        config: &EngineConfig,
        auth: Auth,
        profile: EngineProfile,
    ) -> anyhow::Result<EngineRuntime> {
        Self::assemble_runtime_inner(config, auth, profile, None).await
    }

    /// Like [`Self::assemble_runtime`], but against an [`InstanceLock`] the
    /// caller already holds on the profile's device root (headed bootstrap
    /// acquires it before binding IPC).
    pub async fn assemble_runtime_with_lock(
        config: &EngineConfig,
        auth: Auth,
        profile: EngineProfile,
        lock: InstanceLock,
    ) -> anyhow::Result<EngineRuntime> {
        Self::assemble_runtime_inner(config, auth, profile, Some(lock)).await
    }

    async fn assemble_runtime_inner(
        config: &EngineConfig,
        auth: Auth,
        profile: EngineProfile,
        lock: Option<InstanceLock>,
    ) -> anyhow::Result<EngineRuntime> {
        let edge_enabled = match profile.scope() {
            ProfileScope::Local => false,
            ProfileScope::Synced => {
                // Validate the persisted session in the BACKGROUND: the probe
                // still transitions auth to SignedOut on definitive revocation
                // (and warms the single-flight refresh every first dial waits
                // on), but assembly — and the viewport blocked on it — no
                // longer stalls on a WorkOS round trip that can take seconds
                // on a bad link. Everything shown at boot is local anyway.
                let auth_probe = auth.clone();
                tokio::spawn(async move {
                    let _ = auth_probe.access_token().await;
                });
                true
            }
            // Dev Auth always exposes `dev_user_id` as its synthetic access
            // token, including when WorkOS was merely disabled with
            // ZERON_WORKOS_CLIENT_ID="". Only an explicitly configured,
            // non-empty bearer opts this runtime into Edge rooms and relays.
            ProfileScope::Development => config
                .edge_token
                .as_deref()
                .is_some_and(|token| !token.trim().is_empty()),
        };
        let fixed_profile = (profile.scope() == ProfileScope::Synced).then(|| ProfileIdentity {
            organization_id: profile.organization_id().to_string(),
            user_id: profile.user_id().to_string(),
        });
        let profile_token: Arc<dyn zeron_rpc::TokenSource> = Arc::new(ProfileTokenSource::new(
            Arc::new(auth.clone()),
            fixed_profile,
        ));
        if edge_enabled {
            // OS network-path events (macOS NWPathMonitor): the instant the
            // path returns every parked reconnect backoff redials, and while
            // the OS says there is no path the dial loops park instead of
            // burning attempts. No-op on platforms without a monitor.
            zeron_sync::net_path::spawn_path_monitor();
        }
        let device_id = load_or_create_device_id(profile.device_root())?;
        let edge = edge_enabled.then(|| {
            EdgeConfig::new(config.edge_url.clone(), profile_token.clone()).with_device(device_id)
        });

        let core = match lock {
            Some(lock) => EngineCore::assemble_with_profile_locked(
                profile,
                Arc::new(default_registry()),
                config.default_harness,
                edge.clone(),
                lock,
            )?,
            None => EngineCore::assemble_with_profile(
                profile,
                Arc::new(default_registry()),
                config.default_harness,
                edge.clone(),
            )?,
        };
        core.set_auth(auth.clone());
        if edge_enabled {
            // Release checker: polls {edge}/releases on a 6h cadence; headless
            // installs with ZERON_AUTO_UPDATE=1 apply + restart themselves — gated
            // on quiescence so a restart never lands under a live run or open PTY.
            let quiescent: zeron_update::QuiescentCheck = {
                let sessions = core.sessions.clone();
                let terminals = core.terminals.clone();
                Arc::new(move || !sessions.any_active() && !terminals.any_open())
            };
            let updater = zeron_update::Updater::spawn(config.edge_url.clone(), Some(quiescent));
            if let Some(mut token_changes) = edge.as_ref().and_then(EdgeConfig::token_changes) {
                let updater_for_tokens = updater.clone();
                let wake = tokio::spawn(async move {
                    while token_changes.changed().await.is_ok() {
                        updater_for_tokens.check_now();
                    }
                });
                core.set_updater_wake(wake);
            }
            core.set_updater(updater);
        }
        tracing::info!(device_id = %core.device_id, "engine core assembled");
        // Managed ACP adapters install in the background at boot (agents
        // whose CLI is present but whose adapter isn't yet), so a first chat
        // never waits on — or dies inside — an npm run.
        zeron_harness::acp::prewarm_managed_adapters();

        let host_relay = edge.as_ref().map(|edge| {
            // Keep the fork's profile-scoped token source: a synced profile
            // must dial peers as its own (Organization, user), not as whatever identity
            // the shared auth handle currently holds.
            let mut link_config =
                zeron_rpc::LinkCacheConfig::new(edge.url.clone(), profile_token.clone());
            // Registry-dark dial gate: devices with no recent presence fail
            // fast with zero dials; presence returning un-parks them (the
            // peer-alive hook below clears any cooldown at the same moment).
            let registry_for_liveness = core.registry.clone();
            link_config.liveness = Some(Arc::new(move |device_id: &str| {
                registry_for_liveness.peer_liveness(device_id)
            }));
            let links = zeron_rpc::LinkCache::new(link_config);
            let links_for_presence = links.clone();
            core.registry
                .set_peer_alive_hook(Arc::new(move |device_id: &str| {
                    links_for_presence.reset_cooldown(device_id);
                }));
            core.set_links(links);
            core.start_host_relay(&edge.url, profile_token.clone())
        });

        Ok(EngineRuntime {
            core,
            host_relay: std::sync::Mutex::new(host_relay),
        })
    }

    /// Run until ctrl-c: auth (dev or WorkOS), sessions engine + doc host + command
    /// executor, IPC server, and — when edge+auth are ready — the device-room host
    /// relay + peer link cache (targetDeviceId routing).
    pub async fn run(self) -> anyhow::Result<()> {
        let config = self.config;
        tracing::info!(data_dir = %config.data_dir.display(), "engine starting");

        std::fs::create_dir_all(&config.data_dir)?;
        let auth = Self::build_auth(&config).await;
        let mut auth_state = auth.watch_state();
        let profile_scope = Self::initial_profile_scope(&auth);
        let mut profile = Self::resolve_profile(&config, &auth, profile_scope)?;
        let _refresh_loop = auth.spawn_refresh_loop();

        // A captured cloud session without an organization must finish onboarding
        // before its profile can open. A clean signed-out install is local and never
        // enters the terminal sign-in flow.
        if profile_scope == ProfileScope::Synced && profile.is_none() {
            terminal_sign_in(&auth).await?;
            profile = Self::resolve_profile(&config, &auth, profile_scope)?;
        }
        let profile = profile
            .ok_or_else(|| EngineError::Other("synced registry profile is not ready".into()))?;

        let runtime = Self::assemble_runtime(&config, auth, profile).await?;

        // A daemon exists to serve this port, so a bind failure is fatal here —
        // unlike the headed app, which can still work over its in-process
        // transport (see `serve_ipc`).
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::unbounded_channel();
        let service: Arc<dyn RpcService> = Arc::new(HeadlessRpc {
            inner: runtime.core().rpc_service(),
            stop_tx,
        });
        let server = serve_ipc(config.ipc_port, service).await?;
        // The listener is bound: agent sessions may now get the MCP bridge
        // that dials back to it.
        runtime.core().sessions.set_mcp_port(config.ipc_port);

        tokio::select! {
            result = shutdown_signal() => result?,
            requested = stop_rx.recv() => {
                if requested.is_some() {
                    tracing::info!("headless shutdown requested over IPC");
                }
            }
            _ = wait_for_signed_out(&mut auth_state), if profile_scope == ProfileScope::Synced => {
                // Edge transports observe the same auth signal and close at
                // once. Leave a brief reply window for a SignOut RPC before
                // the localhost server itself is aborted.
                runtime.disconnect_edge();
                tracing::info!("headless authentication revoked; stopping synced runtime");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        tracing::info!("shutting down");
        server.abort();
        runtime.shutdown().await;
        Ok(())
    }
}

async fn wait_for_signed_out(state: &mut tokio::sync::watch::Receiver<AuthState>) {
    loop {
        if matches!(&*state.borrow(), AuthState::SignedOut) {
            return;
        }
        if state.changed().await.is_err() {
            return;
        }
    }
}

/// Ctrl-C or SIGTERM. systemd/launchd stop (and the auto-updater's service
/// restart) deliver SIGTERM — without catching it the daemon dies mid-write
/// and every stop takes the crash-recovery path instead of the graceful drain.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// Serve the typed RPC on the localhost IPC port.
///
/// Both engines call this: the headless daemon, and the headed app's embedded
/// engine. That second case is the point — an embedded engine that keeps the
/// port to itself forces anyone wanting a second viewport (the terminal app) to
/// stop the desktop app, start a daemon, and start it again in the right order.
/// Serving here means any viewport can just attach.
///
/// Localhost only, exactly as before: this widens *which process* can serve the
/// port, not who can reach it.
pub async fn serve_ipc(
    port: u16,
    service: std::sync::Arc<dyn zeron_rpc::RpcService>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!(port, "IPC server listening");
    Ok(tokio::spawn(zeron_rpc::serve_ws_listener(
        listener, service,
    )))
}

/// Block until the WorkOS session is signed in and Organization-scoped. On a TTY, print the
/// headless (paste-code) sign-in URL, read the pasted `state.code` from stdin, and
/// run Organization onboarding (create / auto-join / numbered picker). Off a TTY this
/// errors immediately — a daemon under systemd/launchd must load the session that
/// `zeron login` persisted, never wait on a prompt nobody can see.
pub async fn terminal_sign_in(auth: &Auth) -> Result<(), EngineError> {
    use std::io::IsTerminal;
    let interactive = std::io::stdin().is_terminal();
    let mut state_rx = auth.watch_state();
    let mut stdin_reader: Option<tokio::task::JoinHandle<()>> = None;
    let mut organization_reader: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        let state = state_rx.borrow().clone();
        match state {
            AuthState::SignedIn {
                user,
                organization_id,
            } => {
                tracing::info!(email = %user.email, organization = organization_id.as_deref().unwrap_or("<none>"),
                    "auth: session ready");
                break;
            }
            AuthState::NeedsOrganization { user } => {
                if !interactive {
                    // No reader tasks have been spawned on this path (both spawns
                    // are TTY-gated), so an early return leaks nothing.
                    return Err(EngineError::Other(format!(
                        "signed in as {} but no Organization is selected — run `zeron login` on this machine to pick one",
                        user.email
                    )));
                }
                if organization_reader.is_none() {
                    // Organization onboarding on the TTY (old zeron's
                    // `backend login` flow): create if none, auto-join a
                    // single membership, numbered picker otherwise.
                    println!("Signed in as {}.", user.email);
                    organization_reader =
                        Some(tokio::spawn(run_organization_onboarding(auth.clone())));
                }
            }
            AuthState::SignedOut => {
                if !interactive {
                    return Err(EngineError::Other(
                        "not signed in — run `zeron login` on this machine first".into(),
                    ));
                }
                if stdin_reader.is_none() {
                    let url = auth.start_headless_sign_in();
                    println!("Sign in to Zeron:\n\n  {url}\n");
                    println!("Then paste the code shown in the browser here and press enter.");
                    let auth = auth.clone();
                    stdin_reader = Some(tokio::spawn(async move {
                        loop {
                            let Some(line) = read_stdin_line().await else {
                                return;
                            };
                            let pasted = line.trim();
                            if pasted.is_empty() {
                                continue;
                            }
                            match auth.complete_sign_in(pasted).await {
                                Ok(()) => return,
                                Err(err) => println!("Sign-in failed: {err}"),
                            }
                        }
                    }));
                }
            }
        }
        if state_rx.changed().await.is_err() {
            break;
        }
    }
    if let Some(reader) = stdin_reader {
        reader.abort();
    }
    if let Some(reader) = organization_reader {
        reader.abort();
    }
    Ok(())
}

/// One line from stdin (blocking read off the runtime). `None` = stdin closed.
async fn read_stdin_line() -> Option<String> {
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None, // EOF / error
            Ok(_) => Some(line),
        }
    })
    .await
    .ok()
    .flatten()
}

/// TTY Organization onboarding for a session without an Organization (ports old zeron's
/// `backend login` flow): no memberships → prompt a name and create; exactly
/// one → auto-join; several → numbered picker. Success flips the auth state to
/// `SignedIn`, which ends [`wait_for_sign_in`]'s wait (and aborts this task).
async fn run_organization_onboarding(auth: Auth) {
    // Zero memberships → a personal Organization is minted automatically; the
    // prompts below then only handle the pick-one / create-another cases.
    if let Err(err) = auth.ensure_default_organization().await {
        tracing::warn!(error = %err, "default Organization creation failed; falling back to prompt");
    }
    if auth.state().is_signed_in() {
        return; // ensure_default_organization created and selected the personal Organization
    }
    let organizations = match auth.list_organizations().await {
        Ok(organizations) => organizations,
        Err(err) => {
            println!(
                "Could not list organizations ({err}) — create or select one from the Zeron UI to continue."
            );
            return;
        }
    };
    match organizations.len() {
        0 => {
            println!("No organizations yet — name your new organization and press enter:");
            loop {
                let Some(line) = read_stdin_line().await else {
                    return;
                };
                let name = line.trim();
                if name.is_empty() {
                    continue;
                }
                match auth.create_organization(name).await {
                    Ok(_) => return,
                    Err(err) => println!("Creating organization failed: {err}"),
                }
            }
        }
        1 => {
            let only = &organizations[0];
            println!("Joining organization \"{}\"…", only.name);
            if let Err(err) = auth.select_organization(&only.organization_id).await {
                println!("Joining organization failed: {err}");
            }
        }
        _ => {
            println!("\nYour organizations:");
            for (index, organization) in organizations.iter().enumerate() {
                println!("  {}. {}", index + 1, organization.name);
            }
            println!("Pick an organization [1-{}]:", organizations.len());
            loop {
                let Some(line) = read_stdin_line().await else {
                    return;
                };
                let choice = line
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|index| organizations.get(index));
                let Some(organization) = choice else {
                    println!("Pick an organization [1-{}]:", organizations.len());
                    continue;
                };
                match auth
                    .select_organization(&organization.organization_id)
                    .await
                {
                    Ok(()) => return,
                    Err(err) => println!("Joining organization failed: {err}"),
                }
            }
        }
    }
}

/// Best-effort human name for this device's registry row.
fn local_device_name(device_id: &str) -> String {
    select_local_device_name(
        [
            std::env::var("ZERON_DEVICE_NAME").ok(),
            native_friendly_device_name(),
            std::env::var("HOSTNAME").ok(),
            gethostname::gethostname().into_string().ok(),
            std::fs::read_to_string("/etc/hostname").ok(),
        ],
        device_id,
        std::env::consts::OS,
    )
}

fn select_local_device_name(
    candidates: impl IntoIterator<Item = Option<String>>,
    device_id: &str,
    platform: &str,
) -> String {
    candidates
        .into_iter()
        .flatten()
        .map(|name| name.trim().to_string())
        .find(|name| !name.is_empty())
        .unwrap_or_else(|| {
            let platform = match platform {
                "macos" => "macOS",
                "windows" => "Windows",
                "linux" => "Linux",
                _ => "Local",
            };
            let short_id: String = device_id.chars().take(8).collect();
            format!("{platform} device {short_id}")
        })
}

#[cfg(target_os = "macos")]
fn native_friendly_device_name() -> Option<String> {
    let output = std::process::Command::new("/usr/sbin/scutil")
        .args(["--get", "ComputerName"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(not(target_os = "macos"))]
fn native_friendly_device_name() -> Option<String> {
    #[cfg(target_os = "windows")]
    return std::env::var("COMPUTERNAME").ok();

    #[cfg(not(target_os = "windows"))]
    None
}

#[cfg(test)]
mod profile_token_tests {
    use super::{ProfileIdentity, ProfileTokenSource, jwt_profile};
    use base64::Engine as _;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn jwt_with_user(organization_id: &str, user_id: Option<&str>) -> String {
        let mut claims = serde_json::json!({ "org_id": organization_id });
        if let Some(user_id) = user_id {
            claims["sub"] = serde_json::json!(user_id);
        }
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).expect("JWT payload"));
        format!("header.{payload}.signature")
    }

    fn jwt(organization_id: &str, user_id: &str) -> String {
        jwt_with_user(organization_id, Some(user_id))
    }

    fn profile(organization_id: &str, user_id: &str) -> ProfileIdentity {
        ProfileIdentity {
            organization_id: organization_id.into(),
            user_id: user_id.into(),
        }
    }

    struct MutableToken {
        value: Mutex<String>,
        changes: tokio::sync::watch::Sender<u64>,
    }

    impl MutableToken {
        fn new(value: String) -> Self {
            let (changes, _) = tokio::sync::watch::channel(0);
            Self {
                value: Mutex::new(value),
                changes,
            }
        }

        fn set(&self, value: String) {
            *self.value.lock().expect("token lock") = value;
        }

        fn notify(&self) {
            self.changes
                .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        }
    }

    #[async_trait::async_trait]
    impl zeron_rpc::TokenSource for MutableToken {
        async fn token(&self) -> Option<String> {
            Some(self.value.lock().expect("token lock").clone())
        }

        fn subscribe(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
            Some(self.changes.subscribe())
        }
    }

    #[tokio::test]
    async fn fixed_profile_rechecks_organization_and_user_on_one_mutable_source() {
        let original = jwt("org-a", "user-a");
        let backing = Arc::new(MutableToken::new(original.clone()));
        let source = ProfileTokenSource::new(backing.clone(), Some(profile("org-a", "user-a")));
        let mut changes = zeron_rpc::TokenSource::subscribe(&source)
            .expect("profile token must forward the auth epoch");

        assert_eq!(
            zeron_rpc::TokenSource::token(&source).await,
            Some(original.clone())
        );

        backing.set(jwt("org-b", "user-a"));
        // The fence is independent of the async shutdown notification: no
        // caller can obtain the new Organization bearer during that handoff window.
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, None);
        backing.notify();
        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .expect("Organization switch notification")
            .expect("token epoch stays open");
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, None);

        backing.set(jwt("org-a", "user-b"));
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, None);
        backing.notify();
        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .expect("user switch notification")
            .expect("token epoch stays open");
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, None);

        backing.set(original.clone());
        backing.notify();
        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .expect("profile restore notification")
            .expect("token epoch stays open");
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, Some(original));
    }

    #[tokio::test]
    async fn synced_profile_fails_closed_on_malformed_or_incomplete_bearers() {
        let source = ProfileTokenSource::new(
            Arc::new(zeron_rpc::StaticToken("not-a-jwt".into())),
            Some(profile("org-a", "user-a")),
        );
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, None);
        assert_eq!(jwt_profile("not-a-jwt"), None);

        let source = ProfileTokenSource::new(
            Arc::new(zeron_rpc::StaticToken(jwt_with_user("org-a", None))),
            Some(profile("org-a", "user-a")),
        );
        assert_eq!(zeron_rpc::TokenSource::token(&source).await, None);
    }

    #[tokio::test]
    async fn unscoped_development_token_passes_through() {
        let source = ProfileTokenSource::new(
            Arc::new(zeron_rpc::StaticToken("dev-user@org".into())),
            None,
        );
        assert_eq!(
            zeron_rpc::TokenSource::token(&source).await.as_deref(),
            Some("dev-user@org")
        );
    }
}

#[cfg(test)]
mod device_name_tests {
    use super::select_local_device_name;

    fn name(candidates: &[Option<&str>], device_id: &str, platform: &str) -> String {
        select_local_device_name(
            candidates
                .iter()
                .map(|candidate| candidate.map(str::to_string)),
            device_id,
            platform,
        )
    }

    #[test]
    fn explicit_override_wins_and_is_trimmed() {
        assert_eq!(
            name(
                &[Some("  Studio Mac  "), Some("system-host")],
                "17bc0aa2-rest",
                "macos"
            ),
            "Studio Mac"
        );
    }

    #[test]
    fn native_friendly_name_wins_over_hostnames() {
        assert_eq!(
            name(
                &[
                    None,
                    Some("MacBook Pro de Jose"),
                    None,
                    Some("MacBook-Pro.local"),
                ],
                "17bc0aa2-rest",
                "macos"
            ),
            "MacBook Pro de Jose"
        );
    }

    #[test]
    fn windows_computer_name_is_used_when_present() {
        assert_eq!(
            name(
                &[None, Some("DESKTOP-123"), Some("shell-host")],
                "17bc0aa2-rest",
                "windows"
            ),
            "DESKTOP-123"
        );
    }

    #[test]
    fn blank_candidates_are_ignored() {
        assert_eq!(
            name(
                &[Some("  "), None, Some("\n"), Some("linux-box")],
                "17bc0aa2-rest",
                "linux"
            ),
            "linux-box"
        );
    }

    #[test]
    fn final_fallback_is_platform_specific_and_distinct() {
        assert_eq!(
            name(&[None, Some(" ")], "17bc0aa2-rest", "linux"),
            "Linux device 17bc0aa2"
        );
    }
}

/// Trimmed env var or the given default.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Canonical Organization env lookup with the historical abbreviation as a
/// read-only compatibility fallback.
pub(crate) fn organization_env_or(default: &str) -> String {
    for key in ["ZERON_ORGANIZATION_ID", "ZERON_ORG_ID"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    default.to_string()
}

/// Stable per-installation device id, persisted at `{data_dir}/device-id`.
fn load_or_create_device_id(data_dir: &Path) -> Result<String, EngineError> {
    std::fs::create_dir_all(data_dir)?;
    // EngineInfo is resolved before the lifetime InstanceLock is acquired, so
    // identity creation and legacy repair need their own short critical section.
    // The OS releases this lock after a crash; unlike a create_new lockfile it
    // cannot strand an installation permanently.
    let _identity_lock = DeviceIdentityLock::acquire(data_dir)?;
    let path = data_dir.join("device-id");
    let recovering_empty = match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => return Ok(id.trim().to_string()),
        // Older releases used truncate+write. A crash between those operations
        // left a zero-byte file that is safe to replace with a fresh identity.
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => return Err(err.into()),
    };

    let id = new_id();
    let temp_path = data_dir.join(format!(
        ".device-id.tmp-{}-{}",
        std::process::id(),
        new_id()
    ));
    let write_result = (|| -> Result<(), EngineError> {
        let mut temp = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        temp.write_all(id.as_bytes())?;
        temp.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }

    // Fresh installs use create-if-absent. Legacy empty files need an atomic
    // same-directory replacement on Unix; the Windows fallback runs under the
    // identity lock and remains recoverable if interrupted.
    let publish_result = if recovering_empty {
        match std::fs::read_to_string(&path) {
            Ok(id) if !id.trim().is_empty() => {
                let _ = std::fs::remove_file(&temp_path);
                return Ok(id.trim().to_string());
            }
            Ok(_) => replace_empty_device_id(&temp_path, &path),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::hard_link(&temp_path, &path)
            }
            Err(err) => Err(err),
        }
    } else {
        std::fs::hard_link(&temp_path, &path)
    };
    let _ = std::fs::remove_file(&temp_path);
    match publish_result {
        Ok(()) => Ok(id),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let winner = std::fs::read_to_string(&path)?;
            if winner.trim().is_empty() {
                Err(EngineError::Other(format!(
                    "invalid device identity {}: file is empty",
                    path.display()
                )))
            } else {
                Ok(winner.trim().to_string())
            }
        }
        Err(err) => Err(err.into()),
    }
}

struct DeviceIdentityLock {
    _file: std::fs::File,
}

impl DeviceIdentityLock {
    fn acquire(data_dir: &Path) -> Result<Self, EngineError> {
        let path = data_dir.join("device-id.lock");
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
            let mut retries = 200;
            let file = loop {
                match options.open(&path) {
                    Ok(file) => break file,
                    Err(err)
                        if err.kind() == std::io::ErrorKind::PermissionDenied && retries > 0 =>
                    {
                        retries -= 1;
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(err) => return Err(err.into()),
                }
            };
            return Ok(Self { _file: file });
        }

        #[cfg(not(windows))]
        let file = options.open(&path)?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            loop {
                if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                    break;
                }
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EINTR) {
                    return Err(err.into());
                }
            }
        }

        #[cfg(not(windows))]
        Ok(Self { _file: file })
    }
}

fn replace_empty_device_id(temp_path: &Path, path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::rename(temp_path, path)
    }
    #[cfg(not(unix))]
    {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        std::fs::hard_link(temp_path, path)
    }
}
