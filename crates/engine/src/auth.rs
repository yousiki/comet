//! Auth — the engine owns the WorkOS session for its device (feature-inventory §3.7,
//! ARCHITECTURE §5). Port of zeron's `apps/backend/src/auth.ts`.
//!
//! The engine is a public client: it builds the AuthKit authorize URL itself but
//! delegates the secret-bearing **code exchange** and **refresh** to the edge Worker
//! (`/auth/exchange`, `/auth/refresh` — the WorkOS API key lives only there).
//!
//! Two modes:
//! - **Dev** (no WorkOS client id configured, or the edge reports `auth: "dev"`): always
//!   signed in; the bearer IS the configured user id (current M2/M3 behavior).
//! - **WorkOS**: authorization-code flow. Headed devices use a loopback callback server
//!   on an ephemeral port; headless devices use the paste-code flow (the redirect is the
//!   edge's hosted `/auth/cli/callback` page, which shows `state.code` to paste back via
//!   stdin or the `CompleteSignIn` RPC). The refresh token is persisted 0600 in the data
//!   dir; access tokens are cached with dual-clock expiry (monotonic AND wall, whichever
//!   aged more — see [`AccessEntry`]) and refreshed on demand plus by a background loop,
//!   so the device-room relay and room clients always dial with a live `?token=`, even
//!   on the first redial after a laptop wakes from sleep. Org onboarding: an org-less session is `NeedsOrganization`; `SelectOrg`
//!   runs an org-scoped refresh and the state follows the returned token's `org_id`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

use crate::EngineError;

const SIGN_IN_TTL: Duration = Duration::from_secs(15 * 60);
/// Refresh when the cached token has less than this much life left.
const TOKEN_SLACK: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const SESSION_FILE: &str = "session.json";
/// Durable session manifest. `signedOut` fences any stale `session.json`; `active`
/// publishes a newly fsynced session. Missing means a legacy pre-manifest session.
const SESSION_STATE_FILE: &str = "session.state";
const ACTIVE_SESSION_MANIFEST: &[u8] = br#"{"state":"active","version":1}"#;
const SIGNED_OUT_SESSION_MANIFEST: &[u8] = br#"{"state":"signedOut","version":1}"#;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Wire types (feature-inventory §2 AuthRpc)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthUser {
    pub id: String,
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrgMembership {
    pub id: String,
    pub organization_id: String,
    pub name: String,
    /// The caller's role in this org ("admin" | "member"). Additive: absent
    /// from pre-role edges defaults to member.
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "member".into()
}

/// One member of an organization (the Team settings roster).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrgMember {
    pub membership_id: String,
    pub user_id: String,
    pub email: String,
    #[serde(default)]
    pub name: Option<String>,
    pub role: String,
}

/// The stable auth state after deleting a workspace. Callers must replace any
/// identity-scoped runtime after either outcome: it now belongs to the selected
/// fallback workspace, or the WorkOS session has been cleared completely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum DeleteOrgOutcome {
    Switched {
        #[serde(rename = "organizationId")]
        organization_id: String,
    },
    SignedOut,
}

/// AuthStatus stream payload (`SignedOut | NeedsOrganization{user} |
/// SignedIn{user, orgId?}`). Serializes as the canonical [`zeron_proto::AuthState`]
/// wire shape (`{"state": "signedIn", …}`) so every client parses one form.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthState {
    SignedOut,
    NeedsOrganization {
        user: AuthUser,
    },
    SignedIn {
        user: AuthUser,
        org_id: Option<String>,
    },
}

impl AuthState {
    pub fn is_signed_in(&self) -> bool {
        matches!(self, AuthState::SignedIn { .. })
    }

    pub fn org_id(&self) -> Option<&str> {
        match self {
            AuthState::SignedIn { org_id, .. } => org_id.as_deref(),
            _ => None,
        }
    }

    pub fn user(&self) -> Option<&AuthUser> {
        match self {
            AuthState::SignedIn { user, .. } | AuthState::NeedsOrganization { user } => Some(user),
            AuthState::SignedOut => None,
        }
    }

    /// The proto wire twin — the one shape the engine emits over AuthStatus.
    pub fn to_proto(&self) -> zeron_proto::AuthState {
        let profile = |user: &AuthUser| zeron_proto::UserProfile {
            id: user.id.clone(),
            email: user.email.clone(),
            name: user.name.clone(),
        };
        match self {
            AuthState::SignedOut => zeron_proto::AuthState::SignedOut,
            AuthState::NeedsOrganization { user } => zeron_proto::AuthState::NeedsOrganization {
                user: profile(user),
            },
            AuthState::SignedIn { user, org_id } => zeron_proto::AuthState::SignedIn {
                user: profile(user),
                org_id: org_id.clone(),
            },
        }
    }
}

impl Serialize for AuthState {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_proto().serialize(serializer)
    }
}

// ---------------------------------------------------------------------------
// Config + construction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Edge base URL (`/auth/*` routes).
    pub edge_url: String,
    /// Data dir for the persisted session (`session.json`, 0600).
    pub data_dir: PathBuf,
    /// WorkOS client id; `None` = dev mode.
    pub workos_client_id: Option<String>,
    /// WorkOS API base (authorize URL host).
    pub workos_api_base: String,
    /// Dev-mode bearer/user id (mirrors the old `ZERON_EDGE_TOKEN` behavior).
    pub dev_user_id: String,
    /// Loopback callback port; `None` = ephemeral.
    pub callback_port: Option<u16>,
}

impl AuthConfig {
    pub fn new(edge_url: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            edge_url: edge_url.into(),
            data_dir: data_dir.into(),
            workos_client_id: None,
            workos_api_base: "https://api.workos.com".into(),
            dev_user_id: "dev-user".into(),
            callback_port: None,
        }
    }
}

/// The persisted session (refresh token + user + last org scope).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredSession {
    refresh_token: String,
    user: AuthUser,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    org_id: Option<String>,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum PersistedSessionState {
    Active,
    SignedOut,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionManifest {
    state: PersistedSessionState,
    version: u8,
}

/// Whether an authenticated Edge request has a definitive server outcome.
/// A complete non-success response proves the operation was rejected; transport,
/// body-read, and success-decoding failures leave a mutating request's outcome
/// unknown to the caller.
enum AuthedRequestError {
    Definite(EngineError),
    Ambiguous(EngineError),
}

impl AuthedRequestError {
    fn into_engine_error(self) -> EngineError {
        match self {
            Self::Definite(error) | Self::Ambiguous(error) => error,
        }
    }
}

#[derive(Clone, Copy)]
enum RefreshFailurePolicy {
    /// Preserve the established offline retry behavior for ordinary background
    /// refreshes, which do not attempt an organization transition.
    Routine,
    /// A complete Edge rejection proves the refresh token was not rotated, so
    /// the pre-request session may be republished.
    RestoreOnDefiniteRejection,
    /// The caller already completed an irreversible operation (workspace delete),
    /// so the old profile must remain fenced even after a definite rejection.
    KeepInvalidated,
}

enum RefreshCommitError {
    /// The routine refresh could not install its post-response fence. The old
    /// active session is still authoritative and remains retryable.
    Unfenced(EngineError),
    /// A durable signedOut fence exists, so the matching in-memory session must
    /// be cleared rather than allowed to outlive disk state.
    Fenced(EngineError),
}

/// Access-token cache. Expiry ages the token's own lifetime (`exp - iat`) by
/// BOTH clocks, pessimistically. Monotonic alone (`Instant`) freezes across
/// system sleep (macOS `mach_absolute_time` and Linux `CLOCK_MONOTONIC` both
/// exclude suspend), so a laptop waking from hours of sleep presented a
/// wall-expired token that still read "fresh" — every room/relay redial got a
/// 401 with the same stale bearer and sync never recovered (user report).
/// Wall clock alone breaks under skewed device clocks (`exp` vs local time);
/// the elapsed-since-issue reading is skew-immune, and a BACKWARD wall step
/// (NTP correction) degrades harmlessly to the monotonic reading.
struct AccessEntry {
    token: String,
    ttl: Duration,
    got_at: Instant,
    got_wall: std::time::SystemTime,
}

impl AccessEntry {
    fn fresh(token: String) -> Self {
        let ttl = jwt_claims(&token)
            .and_then(|c| match (c.exp, c.iat) {
                (Some(exp), Some(iat)) if exp > iat => {
                    Some(Duration::from_secs((exp - iat) as u64))
                }
                _ => None,
            })
            .unwrap_or(Duration::from_secs(240));
        Self {
            token,
            ttl,
            got_at: Instant::now(),
            got_wall: std::time::SystemTime::now(),
        }
    }

    fn remaining(&self) -> Duration {
        let monotonic = self.got_at.elapsed();
        let wall = std::time::SystemTime::now()
            .duration_since(self.got_wall)
            .unwrap_or(Duration::ZERO);
        self.ttl.saturating_sub(monotonic.max(wall))
    }
}

struct AuthInner {
    config: AuthConfig,
    /// `Some(client_id)` = WorkOS mode; `None` = dev mode.
    workos: Option<String>,
    /// Whether construction loaded a parseable WorkOS session. This is an
    /// immutable startup fact: refresh or sign-out must not rewrite it.
    loaded_workos_session: bool,
    http: reqwest::Client,
    state_tx: watch::Sender<AuthState>,
    token_tx: watch::Sender<u64>,
    stored: Mutex<Option<StoredSession>>,
    access: Mutex<Option<AccessEntry>>,
    /// Pending OAuth states plus the cancellation generation that fences code
    /// exchanges already in flight when sign-out occurs.
    sign_in: Mutex<SignInLifecycle>,
    /// Single-flight refresh: WorkOS refresh tokens are single-use (rotated per
    /// exchange); two concurrent refreshes would race and could revoke the session.
    refresh_gate: tokio::sync::Mutex<()>,
    /// Serialize organization changes. In particular, deleting the selected
    /// workspace and choosing its fallback must appear as one operation to other
    /// org switches; otherwise a concurrent switch can persist a deleted org again.
    org_gate: tokio::sync::Mutex<()>,
    /// Serialize disk persistence with the corresponding in-memory commit.
    /// This is synchronous and is never held across an `.await`.
    session_gate: Mutex<()>,
    /// Loopback callback listener port, bound lazily on the first headed sign-in.
    loopback: tokio::sync::Mutex<Option<u16>>,
}

#[derive(Default)]
struct SignInLifecycle {
    generation: u64,
    pending: HashMap<String, Instant>,
    profile_transition_active: bool,
}

struct ProfileTransitionGuard {
    inner: Arc<AuthInner>,
}

impl Drop for ProfileTransitionGuard {
    fn drop(&mut self) {
        let mut sign_in = lock(&self.inner.sign_in);
        sign_in.profile_transition_active = false;
        sign_in.generation = sign_in.generation.wrapping_add(1);
        sign_in.pending.clear();
    }
}

/// The auth service — cheap to clone by `Arc`.
#[derive(Clone)]
pub struct Auth {
    inner: Arc<AuthInner>,
}

impl Auth {
    /// Build from config: dev mode unless a WorkOS client id is configured.
    pub fn new(config: AuthConfig) -> Self {
        let workos = config
            .workos_client_id
            .clone()
            .filter(|s| !s.trim().is_empty());
        let session_file = config.data_dir.join(SESSION_FILE);
        let session_allowed = match std::fs::read(config.data_dir.join(SESSION_STATE_FILE)) {
            Ok(raw) => matches!(
                serde_json::from_slice::<SessionManifest>(&raw),
                Ok(SessionManifest {
                    state: PersistedSessionState::Active,
                    version: 1,
                })
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
            Err(_) => false,
        };
        let stored: Option<StoredSession> = if workos.is_some() && session_allowed {
            std::fs::read_to_string(&session_file)
                .ok()
                .and_then(|raw| serde_json::from_str(&raw).ok())
        } else {
            None
        };
        let initial = match (&workos, &stored) {
            (None, _) => AuthState::SignedIn {
                user: AuthUser {
                    id: config.dev_user_id.clone(),
                    email: config.dev_user_id.clone(),
                    name: None,
                },
                org_id: None,
            },
            (Some(_), Some(session)) => state_for(session.user.clone(), session.org_id.clone()),
            (Some(_), None) => AuthState::SignedOut,
        };
        let loaded_workos_session = workos.is_some() && stored.is_some();
        let (state_tx, _) = watch::channel(initial);
        let (token_tx, _) = watch::channel(0);
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            inner: Arc::new(AuthInner {
                config,
                workos,
                loaded_workos_session,
                http,
                state_tx,
                token_tx,
                stored: Mutex::new(stored),
                access: Mutex::new(None),
                sign_in: Mutex::new(SignInLifecycle::default()),
                refresh_gate: tokio::sync::Mutex::new(()),
                org_gate: tokio::sync::Mutex::new(()),
                session_gate: Mutex::new(()),
                loopback: tokio::sync::Mutex::new(None),
            }),
        }
    }

    /// Like [`Auth::new`], but additionally probes `{edge}/health`: an edge running in
    /// dev auth mode forces dev mode even when a client id is configured (matching the
    /// edge's "bearer = user id" verification).
    pub async fn detect(mut config: AuthConfig) -> Self {
        if config.workos_client_id.is_some() {
            #[derive(Deserialize)]
            struct Health {
                auth: Option<String>,
            }
            let url = format!("{}/health", config.edge_url.trim_end_matches('/'));
            let probe = async {
                reqwest::Client::new()
                    .get(&url)
                    .timeout(Duration::from_secs(3))
                    .send()
                    .await
                    .ok()?
                    .json::<Health>()
                    .await
                    .ok()
            };
            if let Some(health) = probe.await
                && health.auth.as_deref() == Some("dev")
            {
                tracing::info!("auth: edge is in dev mode — using dev bearer");
                config.workos_client_id = None;
            }
        }
        Self::new(config)
    }

    pub fn workos_enabled(&self) -> bool {
        self.inner.workos.is_some()
    }

    /// True when construction loaded a parseable persisted WorkOS session.
    /// The value stays true even if a later refresh revokes that session.
    pub fn loaded_workos_session(&self) -> bool {
        self.inner.loaded_workos_session
    }

    /// Live auth status (current value + changes).
    pub fn watch_state(&self) -> watch::Receiver<AuthState> {
        self.inner.state_tx.subscribe()
    }

    pub fn state(&self) -> AuthState {
        self.inner.state_tx.borrow().clone()
    }

    /// The signed-in user id — the identity that scopes workspace rooms
    /// (`ws3/{orgId}/{userId}`) and local storage (`orgs/{org}/{user}/`).
    /// Dev mode mirrors the edge's dev-bearer parsing (`user@org` → `user`,
    /// a bare token IS the user id). `None` = signed out (WorkOS only).
    pub fn user_id(&self) -> Option<String> {
        if self.inner.workos.is_none() {
            let dev = &self.inner.config.dev_user_id;
            return Some(dev.split('@').next().unwrap_or(dev).to_string());
        }
        self.state().user().map(|u| u.id.clone())
    }

    /// Current bearer for edge rooms / the device relay — `None` when signed out.
    /// Dev mode: the configured user id. WorkOS: cached access token, refreshed when
    /// it has under 30s left.
    pub async fn access_token(&self) -> Option<String> {
        if self.inner.workos.is_none() {
            return Some(self.inner.config.dev_user_id.clone());
        }
        if let Some(entry) = &*lock(&self.inner.access)
            && entry.remaining() > TOKEN_SLACK
        {
            return Some(entry.token.clone());
        }
        match self.refresh(None).await {
            Ok(token) => token,
            Err(err) => {
                tracing::warn!(error = %err, "auth: refresh failed");
                None
            }
        }
    }

    /// Sleep-until-near-expiry refresh loop so long-lived dials (relay, rooms) always
    /// have a live token to present on reconnect. No-op task in dev mode.
    pub fn spawn_refresh_loop(&self) -> tokio::task::JoinHandle<()> {
        let auth = self.clone();
        tokio::spawn(async move {
            if auth.inner.workos.is_none() {
                return;
            }
            let mut state_rx = auth.watch_state();
            let mut wake = zeron_sync::wake::subscribe();
            loop {
                if !state_rx.borrow().is_signed_in() {
                    if state_rx.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                let remaining = lock(&auth.inner.access)
                    .as_ref()
                    .map(AccessEntry::remaining)
                    .unwrap_or(Duration::ZERO);
                let wait = remaining.saturating_sub(Duration::from_secs(60));
                if wait > Duration::ZERO {
                    // Re-evaluate at least once a minute rather than parking
                    // on one long timer: tokio timers ride the monotonic
                    // clock, which excludes system suspend — a laptop waking
                    // from sleep would otherwise wait the WHOLE original
                    // duration again before noticing the (wall-expired) token.
                    let wait = wait.min(Duration::from_secs(60));
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => { continue; }
                        changed = state_rx.changed() => {
                            if changed.is_err() { return; }
                            continue;
                        }
                        // Wake: the cached token is almost certainly
                        // wall-expired — refresh NOW so the reconnecting
                        // rooms/relays dial with live credentials instead of
                        // discovering staleness one 401 at a time.
                        _ = wake.recv() => {}
                    }
                }
                if let Err(err) = auth.refresh(None).await {
                    tracing::warn!(error = %err, "auth: background refresh failed");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        })
    }

    // -- sign-in flows ------------------------------------------------------

    /// Begin a headed sign-in: returns the AuthKit authorize URL redirecting to our
    /// loopback callback server (bound lazily on an ephemeral port).
    pub async fn start_sign_in(&self) -> Result<String, EngineError> {
        if self.inner.workos.is_none() {
            return Ok(String::new()); // dev mode: nothing to do (TS parity)
        }
        let port = self.ensure_loopback().await?;
        Ok(self.begin_sign_in(&format!("http://127.0.0.1:{port}/callback")))
    }

    /// Begin a headless sign-in: the redirect is the edge's hosted paste-code page —
    /// nothing ever redirects to this machine, so the browser can be anywhere.
    pub fn start_headless_sign_in(&self) -> String {
        if self.inner.workos.is_none() {
            return String::new();
        }
        let edge = self.inner.config.edge_url.trim_end_matches('/');
        self.begin_sign_in(&format!("{edge}/auth/cli/callback"))
    }

    /// Finish a headless sign-in with the pasted `state.code` string. The state half
    /// must match a sign-in started HERE (same CSRF discipline as the loopback flow).
    pub async fn complete_sign_in(&self, pasted: &str) -> Result<(), EngineError> {
        if self.inner.workos.is_none() {
            return Ok(());
        }
        let trimmed = pasted.trim();
        let (state, code) = trimmed.split_once('.').unwrap_or(("", ""));
        if state.is_empty() || code.is_empty() {
            return Err(EngineError::Other(
                "invalid or expired sign-in code — start sign-in again and paste the full code"
                    .into(),
            ));
        }
        let Some(generation) = self.take_pending(state) else {
            return Err(EngineError::Other(
                "invalid or expired sign-in code — start sign-in again and paste the full code"
                    .into(),
            ));
        };
        let result = self.exchange_code(code).await?;
        self.finish_sign_in(result, generation)
    }

    pub fn sign_out(&self) -> Result<(), EngineError> {
        let mut sign_in = lock(&self.inner.sign_in);
        let _session_gate = lock(&self.inner.session_gate);
        self.persist_invalidation()?;
        self.finish_signed_out_locked(&mut sign_in);
        Ok(())
    }

    fn finish_invalidated_prior(&self, prior: &StoredSession) {
        let mut sign_in = lock(&self.inner.sign_in);
        let _session_gate = lock(&self.inner.session_gate);
        if lock(&self.inner.stored).as_ref() == Some(prior) {
            self.finish_signed_out_locked(&mut sign_in);
        }
    }

    fn durably_sign_out_prior(&self, prior: &StoredSession) -> Result<(), EngineError> {
        let mut sign_in = lock(&self.inner.sign_in);
        let _session_gate = lock(&self.inner.session_gate);
        if lock(&self.inner.stored).as_ref() != Some(prior) {
            return Ok(());
        }
        self.persist_invalidation()?;
        self.finish_signed_out_locked(&mut sign_in);
        Ok(())
    }

    /// Cancel OAuth callbacks already in flight and prevent any callback from
    /// committing while a profile-changing operation is using a pinned session.
    fn begin_profile_transition(&self) -> ProfileTransitionGuard {
        let mut sign_in = lock(&self.inner.sign_in);
        debug_assert!(!sign_in.profile_transition_active);
        sign_in.profile_transition_active = true;
        sign_in.generation = sign_in.generation.wrapping_add(1);
        sign_in.pending.clear();
        ProfileTransitionGuard {
            inner: self.inner.clone(),
        }
    }

    fn finish_signed_out_locked(&self, sign_in: &mut SignInLifecycle) {
        sign_in.generation = sign_in.generation.wrapping_add(1);
        sign_in.pending.clear();
        *lock(&self.inner.stored) = None;
        *lock(&self.inner.access) = None;
        self.inner.state_tx.send_replace(AuthState::SignedOut);
        self.inner
            .token_tx
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    /// Fence the currently loaded session on disk while retaining its credentials
    /// in memory long enough to attempt one serialized profile transition.
    fn begin_session_invalidation(&self) -> Result<Option<StoredSession>, EngineError> {
        let _session_gate = lock(&self.inner.session_gate);
        let Some(prior) = lock(&self.inner.stored).clone() else {
            return Ok(None);
        };
        self.persist_invalidation()?;
        Ok(Some(prior))
    }

    /// Restore the pre-transition session after a reversible profile request
    /// fails. A concurrent sign-out/sign-in wins and is never overwritten.
    fn restore_profile_session(&self, prior: &StoredSession) -> Result<(), EngineError> {
        let _session_gate = lock(&self.inner.session_gate);
        if lock(&self.inner.stored).as_ref() != Some(prior) {
            return Ok(());
        }
        self.persist_active_session(prior)
    }

    fn recover_profile_failure(
        &self,
        prior: &StoredSession,
        operation_error: EngineError,
    ) -> EngineError {
        match self.restore_profile_session(prior) {
            Ok(()) => operation_error,
            Err(persist_error) => {
                // The durable invalidation marker remains authoritative. Clear
                // the matching old session too, but never erase a concurrent login.
                self.finish_invalidated_prior(prior);
                EngineError::Other(format!(
                    "{operation_error}; the previous session could not be durably restored, so auth was signed out: {persist_error}"
                ))
            }
        }
    }

    // -- organizations ------------------------------------------------------

    pub async fn list_orgs(&self) -> Result<Vec<OrgMembership>, EngineError> {
        if self.inner.workos.is_none() {
            return Ok(Vec::new());
        }
        #[derive(Deserialize)]
        struct Orgs {
            #[serde(default)]
            orgs: Vec<OrgMembership>,
        }
        let body: Orgs = self
            .authed_json(reqwest::Method::GET, "/auth/orgs", None)
            .await?;
        Ok(body.orgs)
    }

    /// Create an org (the edge makes us its first admin member), durably scope to
    /// it, and return the minted id as proof for runtime replacement.
    pub async fn create_org(&self, name: &str) -> Result<String, EngineError> {
        if self.inner.workos.is_none() {
            return Ok(String::new());
        }
        let _org_gate = self.inner.org_gate.lock().await;
        let _transition = self.begin_profile_transition();
        let token = self
            .access_token()
            .await
            .ok_or_else(|| EngineError::Other("not signed in".into()))?;
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Created {
            organization_id: String,
        }
        let created: Created = self
            .authed_json_with_token(
                &token,
                reqwest::Method::POST,
                "/auth/orgs",
                Some(serde_json::json!({ "name": name })),
            )
            .await?;
        if let Err(select_error) = self
            .select_org_profile_inner(&created.organization_id)
            .await
        {
            let rollback: Result<serde_json::Value, EngineError> = self
                .authed_json_with_token(
                    &token,
                    reqwest::Method::DELETE,
                    &format!("/auth/orgs/{}", created.organization_id),
                    None,
                )
                .await;
            return match rollback {
                Ok(_) => Err(EngineError::Other(format!(
                    "workspace {} was created but could not be selected: {select_error}; the new workspace was rolled back and creation may be retried",
                    created.organization_id
                ))),
                Err(rollback_error) => Err(EngineError::Other(format!(
                    "workspace {} was created but could not be selected: {select_error}; rollback also failed: {rollback_error}. Refresh the workspace list and select organizationId={} if it exists; do not retry creation blindly",
                    created.organization_id, created.organization_id
                ))),
            };
        }
        Ok(created.organization_id)
    }

    /// First-sign-in default workspace: a user with ZERO memberships gets a
    /// personal org minted automatically ("{name}'s Space") and is scoped to
    /// it — the org gate then never appears for the solo path. Idempotent:
    /// any existing membership short-circuits. Errors are soft (the gate UI
    /// remains the fallback).
    pub async fn ensure_default_org(&self) -> Result<(), EngineError> {
        if self.inner.workos.is_none() {
            return Ok(());
        }
        if !matches!(self.state(), AuthState::NeedsOrganization { .. }) {
            return Ok(());
        }
        if !self.list_orgs().await?.is_empty() {
            return Ok(());
        }
        let name = match self.state() {
            AuthState::NeedsOrganization { user } => user
                .name
                .clone()
                .filter(|n| !n.trim().is_empty())
                .map(|n| format!("{n}'s Space"))
                .unwrap_or_else(|| {
                    let stem = user.email.split('@').next().unwrap_or("My");
                    format!("{stem}'s Space")
                }),
            _ => return Ok(()),
        };
        tracing::info!(org = %name, "first sign-in: creating default personal workspace");
        self.create_org(&name).await.map(|_| ())
    }

    /// The current org's team roster.
    pub async fn list_members(&self, org_id: &str) -> Result<Vec<OrgMember>, EngineError> {
        #[derive(Deserialize)]
        struct Members {
            #[serde(default)]
            members: Vec<OrgMember>,
        }
        let body: Members = self
            .authed_json(
                reqwest::Method::GET,
                &format!("/auth/orgs/{org_id}/members"),
                None,
            )
            .await?;
        Ok(body.members)
    }

    /// Invite/add a member by email (admin only, enforced edge-side).
    /// Returns `(added, invited)`: an already-registered user is added
    /// immediately; an unknown email gets a WorkOS invitation.
    pub async fn invite_member(
        &self,
        org_id: &str,
        email: &str,
        role: &str,
    ) -> Result<(bool, bool), EngineError> {
        #[derive(Deserialize)]
        struct Outcome {
            #[serde(default)]
            added: bool,
            #[serde(default)]
            invited: bool,
        }
        let body: Outcome = self
            .authed_json(
                reqwest::Method::POST,
                &format!("/auth/orgs/{org_id}/members"),
                Some(serde_json::json!({ "email": email, "role": role })),
            )
            .await?;
        Ok((body.added, body.invited))
    }

    /// Set a member's role ("admin" | "member"); admin only, edge-enforced,
    /// last-admin protected.
    pub async fn set_member_role(
        &self,
        org_id: &str,
        membership_id: &str,
        role: &str,
    ) -> Result<(), EngineError> {
        let _: serde_json::Value = self
            .authed_json(
                reqwest::Method::POST,
                &format!("/auth/orgs/{org_id}/members/{membership_id}"),
                Some(serde_json::json!({ "role": role })),
            )
            .await?;
        Ok(())
    }

    /// Remove a member (admin only, edge-enforced, last-admin protected).
    pub async fn remove_member(
        &self,
        org_id: &str,
        membership_id: &str,
    ) -> Result<(), EngineError> {
        let _: serde_json::Value = self
            .authed_json(
                reqwest::Method::DELETE,
                &format!("/auth/orgs/{org_id}/members/{membership_id}"),
                None,
            )
            .await?;
        Ok(())
    }

    /// Delete the org (admin only, edge-enforced), then leave auth in a stable
    /// state before returning. A surviving membership is selected and persisted;
    /// otherwise the stale org-scoped session is cleared completely.
    ///
    /// Once Edge accepts the delete, every recovery failure degrades safely to
    /// [`DeleteOrgOutcome::SignedOut`]. It must never leave the deleted `org_id`
    /// in memory or on disk for the next runtime bootstrap to reuse.
    pub async fn delete_org(&self, org_id: &str) -> Result<DeleteOrgOutcome, EngineError> {
        let _org_gate = self.inner.org_gate.lock().await;
        let _transition = self.begin_profile_transition();
        let token = self
            .access_token()
            .await
            .ok_or_else(|| EngineError::Other("not signed in".into()))?;
        let _refresh_gate = self.inner.refresh_gate.lock().await;
        let prior = self
            .begin_session_invalidation()?
            .ok_or_else(|| EngineError::Other("not signed in".into()))?;
        let deleted: Result<serde_json::Value, AuthedRequestError> = self
            .authed_json_with_token_classified(
                &token,
                reqwest::Method::DELETE,
                &format!("/auth/orgs/{org_id}"),
                None,
            )
            .await;
        match deleted {
            Ok(_) => {}
            Err(AuthedRequestError::Definite(error)) => {
                // Edge returned a complete rejection, so no deletion occurred and
                // it is safe to republish the pre-operation session.
                return Err(self.recover_profile_failure(&prior, error));
            }
            Err(AuthedRequestError::Ambiguous(error)) => {
                // Edge may have committed the irreversible deletion before the
                // connection failed. Keep the durable fence rather than reviving a
                // possibly-deleted profile.
                self.finish_invalidated_prior(&prior);
                return Err(EngineError::Other(format!(
                    "{error}; workspace deletion outcome is uncertain, so auth was safely signed out"
                )));
            }
        }

        let current_org = self.state().org_id().map(str::to_owned);
        #[derive(Deserialize)]
        struct Orgs {
            #[serde(default)]
            orgs: Vec<OrgMembership>,
        }
        let listed: Result<Orgs, EngineError> = self
            .authed_json_with_token(&token, reqwest::Method::GET, "/auth/orgs", None)
            .await;
        let mut remaining = match listed {
            Ok(body) => body.orgs,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    deleted_org = org_id,
                    "auth: workspace deleted but fallback discovery failed; signing out"
                );
                self.finish_invalidated_prior(&prior);
                return Ok(DeleteOrgOutcome::SignedOut);
            }
        };
        // WorkOS deletion can be briefly eventually consistent. Never pick the
        // deleted membership even if it is still present in the first list response.
        remaining.retain(|membership| membership.organization_id != org_id);
        let fallback = current_org
            .as_deref()
            .filter(|current| *current != org_id)
            .and_then(|current| {
                remaining
                    .iter()
                    .find(|membership| membership.organization_id == current)
            })
            .or_else(|| remaining.first())
            .map(|membership| membership.organization_id.clone());

        let Some(organization_id) = fallback else {
            self.finish_invalidated_prior(&prior);
            return Ok(DeleteOrgOutcome::SignedOut);
        };
        match self
            .refresh_inner(
                Some(&organization_id),
                RefreshFailurePolicy::KeepInvalidated,
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::warn!(
                    deleted_org = org_id,
                    fallback_org = %organization_id,
                    "auth: workspace deleted but auth changed before fallback selection; signing out"
                );
                self.finish_invalidated_prior(&prior);
                return Ok(DeleteOrgOutcome::SignedOut);
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    deleted_org = org_id,
                    fallback_org = %organization_id,
                    "auth: workspace deleted but fallback selection failed; signing out"
                );
                self.finish_invalidated_prior(&prior);
                return Ok(DeleteOrgOutcome::SignedOut);
            }
        }
        Ok(DeleteOrgOutcome::Switched { organization_id })
    }

    /// Scope the session to an org: one refresh with `organizationId`; the state follows
    /// the returned token's `org_id` claim.
    pub async fn select_org(&self, organization_id: &str) -> Result<(), EngineError> {
        let _org_gate = self.inner.org_gate.lock().await;
        let _transition = self.begin_profile_transition();
        self.select_org_profile_inner(organization_id).await
    }

    async fn select_org_profile_inner(&self, organization_id: &str) -> Result<(), EngineError> {
        if self.inner.workos.is_none() {
            return Ok(());
        }
        let _refresh_gate = self.inner.refresh_gate.lock().await;
        match self
            .refresh_inner(
                Some(organization_id),
                RefreshFailurePolicy::RestoreOnDefiniteRejection,
            )
            .await
        {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(EngineError::Other(
                "could not switch workspace because auth was signed out".into(),
            )),
            Err(err) => Err(err),
        }
    }

    // -- internals ----------------------------------------------------------

    fn begin_sign_in(&self, redirect_uri: &str) -> String {
        let state = uuid::Uuid::new_v4().to_string();
        {
            let mut sign_in = lock(&self.inner.sign_in);
            let cutoff = Instant::now();
            sign_in
                .pending
                .retain(|_, at| cutoff.duration_since(*at) < SIGN_IN_TTL);
            sign_in.pending.insert(state.clone(), cutoff);
        }
        let client_id = self.inner.workos.clone().unwrap_or_default();
        format!(
            "{}/user_management/authorize?response_type=code&client_id={}&redirect_uri={}&provider=authkit&state={}",
            self.inner.config.workos_api_base.trim_end_matches('/'),
            url_encode(&client_id),
            url_encode(redirect_uri),
            state
        )
    }

    /// Consume a pending sign-in state and capture its cancellation generation.
    /// `None` means unknown/expired (CSRF check).
    fn take_pending(&self, state: &str) -> Option<u64> {
        let mut sign_in = lock(&self.inner.sign_in);
        let now = Instant::now();
        sign_in
            .pending
            .retain(|_, at| now.duration_since(*at) < SIGN_IN_TTL);
        sign_in.pending.remove(state)?;
        Some(sign_in.generation)
    }

    async fn exchange_code(&self, code: &str) -> Result<SignInResult, EngineError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct WireUser {
            id: String,
            email: String,
            #[serde(default)]
            first_name: Option<String>,
            #[serde(default)]
            last_name: Option<String>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Exchange {
            user: WireUser,
            access_token: String,
            refresh_token: String,
        }
        let url = format!(
            "{}/auth/exchange",
            self.inner.config.edge_url.trim_end_matches('/')
        );
        let res = self
            .inner
            .http
            .post(&url)
            .json(&serde_json::json!({ "code": code }))
            .send()
            .await
            .map_err(|e| EngineError::Other(format!("the edge is unreachable: {e}")))?;
        if !res.status().is_success() {
            return Err(EngineError::Other(format!(
                "sign-in failed during token exchange ({}) — the code may have expired; start again",
                res.status().as_u16()
            )));
        }
        let body: Exchange = res
            .json()
            .await
            .map_err(|e| EngineError::Other(format!("malformed exchange response: {e}")))?;
        let name = [body.user.first_name, body.user.last_name]
            .into_iter()
            .flatten()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        Ok(SignInResult {
            user: AuthUser {
                id: body.user.id,
                email: body.user.email,
                name: (!name.is_empty()).then_some(name),
            },
            access_token: body.access_token,
            refresh_token: body.refresh_token,
        })
    }

    fn finish_sign_in(&self, result: SignInResult, generation: u64) -> Result<(), EngineError> {
        // Serialize the final commit with sign-out. A callback can consume its
        // OAuth state and spend time exchanging the code; if cancellation wins
        // during that await, its old generation must never restore credentials.
        let mut sign_in = lock(&self.inner.sign_in);
        if sign_in.generation != generation || sign_in.profile_transition_active {
            return Err(EngineError::Other(
                "sign-in was canceled — start again from Zeron".into(),
            ));
        }
        let org_id = jwt_claims(&result.access_token).and_then(|c| c.org_id);
        let access = AccessEntry::fresh(result.access_token);
        let session = StoredSession {
            refresh_token: result.refresh_token,
            user: result.user.clone(),
            org_id: org_id.clone(),
        };
        let _session_gate = lock(&self.inner.session_gate);
        self.persist_invalidation()?;
        if let Err(err) = self.persist_active_session(&session) {
            self.finish_signed_out_locked(&mut sign_in);
            return Err(err);
        }
        *lock(&self.inner.stored) = Some(session);
        *lock(&self.inner.access) = Some(access);
        tracing::info!(email = %result.user.email, org = org_id.as_deref().unwrap_or("<none>"),
            "auth: signed in");
        self.inner
            .state_tx
            .send_replace(state_for(result.user, org_id));
        self.inner
            .token_tx
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        Ok(())
    }

    /// Refresh the session (single-flight). `organization_id` migrates the WorkOS
    /// session to that org; routine refreshes keep the current scope. Returns the new
    /// access token, `None` when signed out / the refresh could not run.
    async fn refresh(&self, organization_id: Option<&str>) -> Result<Option<String>, EngineError> {
        let _gate = self.inner.refresh_gate.lock().await;
        self.refresh_inner(
            organization_id,
            if organization_id.is_none() {
                RefreshFailurePolicy::Routine
            } else {
                RefreshFailurePolicy::RestoreOnDefiniteRejection
            },
        )
        .await
    }

    /// Refresh with the single-flight gate already held. The durable session is
    /// committed before access/state become visible in memory.
    async fn refresh_inner(
        &self,
        organization_id: Option<&str>,
        failure_policy: RefreshFailurePolicy,
    ) -> Result<Option<String>, EngineError> {
        // Re-check under the gate: the refresh we queued behind may have done the work.
        if organization_id.is_none()
            && let Some(entry) = &*lock(&self.inner.access)
            && entry.remaining() > TOKEN_SLACK
        {
            return Ok(Some(entry.token.clone()));
        }
        let prior = match failure_policy {
            RefreshFailurePolicy::Routine => lock(&self.inner.stored).clone(),
            RefreshFailurePolicy::RestoreOnDefiniteRejection => {
                self.begin_session_invalidation()?
            }
            RefreshFailurePolicy::KeepInvalidated => {
                let _session_gate = lock(&self.inner.session_gate);
                let fenced = std::fs::read(self.session_state_file())
                    .ok()
                    .and_then(|raw| serde_json::from_slice::<SessionManifest>(&raw).ok())
                    .is_some_and(|manifest| {
                        manifest.version == 1 && manifest.state == PersistedSessionState::SignedOut
                    });
                if !fenced {
                    return Err(EngineError::Other(
                        "auth session changed while completing workspace deletion".into(),
                    ));
                }
                lock(&self.inner.stored).clone()
            }
        };
        let Some(prior) = prior else { return Ok(None) };
        let refresh_token = prior.refresh_token.clone();
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RefreshBody<'a> {
            refresh_token: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            organization_id: Option<&'a str>,
        }
        let url = format!(
            "{}/auth/refresh",
            self.inner.config.edge_url.trim_end_matches('/')
        );
        let res = self
            .inner
            .http
            .post(&url)
            .json(&RefreshBody {
                refresh_token: &refresh_token,
                organization_id,
            })
            .send()
            .await;
        let res = match res {
            Ok(res) => res,
            Err(err) => {
                let error =
                    EngineError::Other(format!("could not reach the edge during refresh: {err}"));
                if matches!(failure_policy, RefreshFailurePolicy::Routine) {
                    return Err(error);
                }
                self.finish_invalidated_prior(&prior);
                return Err(EngineError::Other(format!(
                    "{error}; refresh outcome is uncertain, so the previous session was safely signed out"
                )));
            }
        };
        let status = res.status();
        let bytes = match res.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                let error = EngineError::Other(format!(
                    "refresh failed ({}): could not read the response: {e}",
                    status.as_u16(),
                    e = error
                ));
                if matches!(failure_policy, RefreshFailurePolicy::Routine) {
                    return Err(error);
                }
                self.finish_invalidated_prior(&prior);
                return Err(EngineError::Other(format!(
                    "{error}; refresh outcome is uncertain, so the previous session was safely signed out"
                )));
            }
        };
        let rejection = (!status.is_success())
            .then(|| edge_response_error_message("refresh failed", status.as_u16(), &bytes));
        if rejection.is_some() && edge_response_outcome_unknown(&bytes) {
            let error = EngineError::Other(rejection.expect("checked above"));
            return match failure_policy {
                // Routine refresh deliberately preserves the prior session on
                // transient uncertainty and lets a later explicit 4xx revoke it.
                RefreshFailurePolicy::Routine => Err(error),
                RefreshFailurePolicy::RestoreOnDefiniteRejection
                | RefreshFailurePolicy::KeepInvalidated => {
                    self.finish_invalidated_prior(&prior);
                    Err(EngineError::Other(format!(
                        "{error}; WorkOS reported that the refresh outcome is unknown, so the previous session was safely signed out"
                    )))
                }
            };
        }
        if status.is_client_error() && organization_id.is_none() {
            // A definitive 4xx means the refresh token itself is dead (revoked session,
            // deleted user) — it can NEVER succeed again. Degrade to SignedOut so every
            // downstream retry loop quiets down. (Org-switch refreshes are exempt: a 4xx
            // there means "not a member", not a dead session.)
            tracing::warn!(
                status = status.as_u16(),
                "auth: refresh rejected — session revoked; signing out"
            );
            if let Err(persist_error) = self.durably_sign_out_prior(&prior) {
                return Err(EngineError::Other(format!(
                    "{}; could not durably sign out: {persist_error}",
                    rejection.unwrap_or_else(|| format!("refresh failed ({})", status.as_u16()))
                )));
            }
            return Ok(None);
        }
        if let Some(rejection) = rejection {
            let error = EngineError::Other(rejection);
            return match failure_policy {
                RefreshFailurePolicy::Routine => Err(error),
                RefreshFailurePolicy::RestoreOnDefiniteRejection => {
                    Err(self.recover_profile_failure(&prior, error))
                }
                RefreshFailurePolicy::KeepInvalidated => {
                    self.finish_invalidated_prior(&prior);
                    Err(error)
                }
            };
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Tokens {
            access_token: String,
            refresh_token: String,
        }
        let tokens: Tokens = match serde_json::from_slice(&bytes) {
            Ok(tokens) => tokens,
            Err(error) => {
                let error = EngineError::Other(format!("malformed refresh response: {error}"));
                if matches!(failure_policy, RefreshFailurePolicy::Routine) {
                    return Err(error);
                }
                self.finish_invalidated_prior(&prior);
                return Err(EngineError::Other(format!(
                    "{error}; refresh outcome is uncertain, so the previous session was safely signed out"
                )));
            }
        };
        let org_id = jwt_claims(&tokens.access_token).and_then(|c| c.org_id);
        if let Some(expected) = organization_id
            && org_id.as_deref() != Some(expected)
        {
            let error = EngineError::Other(format!(
                "could not switch to workspace {expected}: the refreshed token was scoped to {}",
                org_id.as_deref().unwrap_or("no workspace")
            ));
            self.finish_invalidated_prior(&prior);
            return Err(EngineError::Other(format!(
                "{error}; the refresh token may have rotated, so the previous session was safely signed out"
            )));
        }
        let entry = AccessEntry::fresh(tokens.access_token.clone());
        tracing::info!(ttl_s = entry.ttl.as_secs(), "auth: access token refreshed");
        let persist_error = {
            let _session_gate = lock(&self.inner.session_gate);
            let Some(current) = lock(&self.inner.stored).clone() else {
                return Ok(None); // signed out while the refresh request was in flight
            };
            if current.refresh_token != refresh_token {
                // A sign-in committed a different session while this request was in
                // flight. Never overwrite it with credentials derived from the old one.
                if let Some(expected) = organization_id
                    && current.org_id.as_deref() != Some(expected)
                {
                    return Err(EngineError::Other(format!(
                        "could not switch to workspace {expected}: another sign-in selected {} while the switch was in flight",
                        current.org_id.as_deref().unwrap_or("no workspace")
                    )));
                }
                return Ok(lock(&self.inner.access)
                    .as_ref()
                    .map(|entry| entry.token.clone()));
            }
            let org_changed = current.org_id != org_id;
            let user = current.user.clone();
            let candidate = StoredSession {
                refresh_token: tokens.refresh_token,
                user: user.clone(),
                org_id: org_id.clone(),
            };
            let fence_error = matches!(failure_policy, RefreshFailurePolicy::Routine)
                .then(|| self.persist_invalidation().err())
                .flatten();
            if let Some(error) = fence_error {
                Some(RefreshCommitError::Unfenced(error))
            } else {
                match self.persist_active_session(&candidate) {
                    Ok(()) => {
                        *lock(&self.inner.stored) = Some(candidate);
                        *lock(&self.inner.access) = Some(entry);
                        if org_changed {
                            self.inner.state_tx.send_replace(state_for(user, org_id));
                        }
                        self.inner
                            .token_tx
                            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
                        None
                    }
                    Err(error) => Some(RefreshCommitError::Fenced(error)),
                }
            }
        };
        if let Some(error) = persist_error {
            match error {
                RefreshCommitError::Unfenced(error) => return Err(error),
                RefreshCommitError::Fenced(error) => {
                    self.finish_invalidated_prior(&prior);
                    return Err(EngineError::Other(format!(
                        "{error}; the refresh token rotated but could not be durably committed, so auth was safely signed out"
                    )));
                }
            }
        }
        Ok(Some(tokens.access_token))
    }

    fn session_file(&self) -> PathBuf {
        self.inner.config.data_dir.join(SESSION_FILE)
    }

    fn session_state_file(&self) -> PathBuf {
        self.inner.config.data_dir.join(SESSION_STATE_FILE)
    }

    /// Atomically persist the credentials, then publish the active manifest.
    /// A caller may update in-memory auth state only after this returns `Ok`.
    fn persist_active_session(&self, session: &StoredSession) -> Result<(), EngineError> {
        self.persist_active_session_with(session, atomic_write_private)
    }

    fn persist_active_session_with<F>(
        &self,
        session: &StoredSession,
        mut write: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&std::path::Path, &[u8]) -> std::io::Result<()>,
    {
        let bytes = serde_json::to_vec(session)
            .map_err(|e| EngineError::Other(format!("could not serialize auth session: {e}")))?;

        // Two-phase publication: revoke the old active marker before replacing
        // session.json. If either the credential write or final publication fails,
        // startup must ignore whichever session file is left behind.
        write(&self.session_state_file(), SIGNED_OUT_SESSION_MANIFEST).map_err(|e| {
            EngineError::Other(format!(
                "could not durably fence auth session before update: {e}"
            ))
        })?;

        write(&self.session_file(), &bytes).map_err(|e| {
            EngineError::Other(format!("could not durably persist auth session: {e}"))
        })?;

        write(&self.session_state_file(), ACTIVE_SESSION_MANIFEST).map_err(|e| {
            EngineError::Other(format!(
                "auth session was written but could not be durably published: {e}"
            ))
        })
    }

    /// Durably fence any old session before clearing it. The marker is the
    /// security boundary: failure to unlink a stale `session.json` is safe because
    /// construction always ignores it while this marker exists.
    fn persist_invalidation(&self) -> Result<(), EngineError> {
        atomic_write_private(&self.session_state_file(), SIGNED_OUT_SESSION_MANIFEST).map_err(
            |e| EngineError::Other(format!("could not durably invalidate auth session: {e}")),
        )?;

        match std::fs::remove_file(self.session_file()) {
            Ok(()) => {
                if let Err(err) = sync_parent_directory(&self.session_file()) {
                    // The marker's own rename + directory fsync completed first,
                    // so even an undurable cleanup unlink cannot revive the session.
                    tracing::warn!(
                        error = %err,
                        "auth: invalidation is durable but stale-session cleanup fsync failed"
                    );
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                // This cleanup error is intentionally non-fatal: the durable
                // invalidation marker already guarantees the stale file cannot load.
                tracing::warn!(
                    error = %err,
                    path = %self.session_file().display(),
                    "auth: stale session file remains behind a durable invalidation marker"
                );
            }
        }
        Ok(())
    }

    async fn authed_json<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, EngineError> {
        let token = self
            .access_token()
            .await
            .ok_or_else(|| EngineError::Other("not signed in".into()))?;
        self.authed_json_with_token(&token, method, path, body)
            .await
    }

    /// Authenticated request with a caller-pinned fresh bearer. Profile deletion
    /// uses this while holding the refresh gate, so it cannot recursively refresh
    /// and deadlock after the durable invalidation fence is installed.
    async fn authed_json_with_token<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, EngineError> {
        self.authed_json_with_token_classified(token, method, path, body)
            .await
            .map_err(AuthedRequestError::into_engine_error)
    }

    async fn authed_json_with_token_classified<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, AuthedRequestError> {
        let url = format!(
            "{}{}",
            self.inner.config.edge_url.trim_end_matches('/'),
            path
        );
        let mut req = self.inner.http.request(method, &url).bearer_auth(token);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let res = req.send().await.map_err(|e| {
            AuthedRequestError::Ambiguous(EngineError::Other(format!(
                "the edge is unreachable: {e}"
            )))
        })?;
        let status = res.status();
        let bytes = res.bytes().await.map_err(|e| {
            AuthedRequestError::Ambiguous(EngineError::Other(format!(
                "workspace request failed ({}): could not read the response: {e}",
                status.as_u16()
            )))
        })?;
        if !status.is_success() {
            let error = EngineError::Other(workspace_error_message(status.as_u16(), &bytes));
            return Err(if edge_response_outcome_unknown(&bytes) {
                AuthedRequestError::Ambiguous(error)
            } else {
                AuthedRequestError::Definite(error)
            });
        }
        serde_json::from_slice::<T>(&bytes).map_err(|e| {
            AuthedRequestError::Ambiguous(EngineError::Other(format!("malformed response: {e}")))
        })
    }

    // -- loopback callback server ------------------------------------------

    /// Bind the loopback callback listener (idempotent); returns its port.
    async fn ensure_loopback(&self) -> Result<u16, EngineError> {
        let mut slot = self.inner.loopback.lock().await;
        if let Some(port) = *slot {
            return Ok(port);
        }
        let requested = self.inner.config.callback_port.unwrap_or(0);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", requested))
            .await
            .map_err(|e| EngineError::Other(format!("sign-in callback bind failed: {e}")))?;
        let port = listener
            .local_addr()
            .map_err(|e| EngineError::Other(format!("sign-in callback addr: {e}")))?
            .port();
        *slot = Some(port);
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(loopback_loop(listener, weak));
        tracing::info!(port, "auth: sign-in callback listening");
        Ok(port)
    }
}

/// Preserve Edge's actionable JSON errors instead of collapsing every rejected
/// team operation to a status code. Bodies are untrusted and can be unexpectedly
/// large, so the UI-facing detail is whitespace-normalized and bounded.
fn workspace_error_message(status: u16, body: &[u8]) -> String {
    edge_response_error_message("workspace request failed", status, body)
}

fn edge_response_error_message(prefix: &str, status: u16, body: &[u8]) -> String {
    const MAX_DETAIL_CHARS: usize = 600;

    let mut fragments = Vec::new();
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) {
        if let Some(object) = value.as_object() {
            for key in ["error", "message", "details"] {
                if let Some(value) = object.get(key) {
                    collect_error_fragments(value, &mut fragments);
                }
            }
        } else {
            collect_error_fragments(&value, &mut fragments);
        }
    }
    if fragments.is_empty() {
        let raw = String::from_utf8_lossy(body);
        push_error_fragment(&mut fragments, &raw);
    }
    let detail = truncate_chars(&fragments.join("; "), MAX_DETAIL_CHARS);
    if detail.is_empty() {
        format!("{prefix} ({status})")
    } else {
        format!("{prefix} ({status}): {detail}")
    }
}

fn edge_response_outcome_unknown(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("outcomeUnknown")?.as_bool())
        .unwrap_or(false)
}

fn collect_error_fragments(value: &serde_json::Value, fragments: &mut Vec<String>) {
    match value {
        serde_json::Value::Null => {}
        serde_json::Value::String(message) => push_error_fragment(fragments, message),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_error_fragments(value, fragments);
            }
        }
        serde_json::Value::Object(object) => {
            let before = fragments.len();
            for key in ["error", "message", "details", "detail", "reason"] {
                if let Some(value) = object.get(key) {
                    collect_error_fragments(value, fragments);
                }
            }
            if fragments.len() == before
                && let Ok(compact) = serde_json::to_string(value)
            {
                push_error_fragment(fragments, &compact);
            }
        }
        scalar => push_error_fragment(fragments, &scalar.to_string()),
    }
}

fn push_error_fragment(fragments: &mut Vec<String>, message: &str) {
    let normalized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if !normalized.is_empty() && !fragments.iter().any(|part| part == &normalized) {
        fragments.push(normalized);
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let mut truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        truncated.push('…');
    }
    truncated
}

struct SignInResult {
    user: AuthUser,
    access_token: String,
    refresh_token: String,
}

fn state_for(user: AuthUser, org_id: Option<String>) -> AuthState {
    // Every user must belong to an organization before the product opens up; an org-less
    // session is `NeedsOrganization`, which the UI gates on.
    match org_id {
        Some(org_id) => AuthState::SignedIn {
            user,
            org_id: Some(org_id),
        },
        None => AuthState::NeedsOrganization { user },
    }
}

/// The relay/room token seam: `Auth` IS a [`zeron_rpc::TokenSource`], so the host relay
/// and link cache always dial with a fresh bearer after refreshes.
#[async_trait::async_trait]
impl zeron_rpc::TokenSource for Auth {
    async fn token(&self) -> Option<String> {
        if self.inner.workos.is_some() && !self.state().is_signed_in() {
            return None;
        }
        self.access_token().await
    }

    fn subscribe(&self) -> Option<watch::Receiver<u64>> {
        Some(self.inner.token_tx.subscribe())
    }
}

// ---------------------------------------------------------------------------
// Loopback HTTP (hand-rolled: no HTTP server dependency in the engine)
// ---------------------------------------------------------------------------

async fn loopback_loop(listener: tokio::net::TcpListener, inner: Weak<AuthInner>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let Some(inner) = inner.upgrade() else { break };
        tokio::spawn(async move {
            if let Err(err) = handle_loopback_conn(stream, Auth { inner }).await {
                tracing::debug!(error = %err, "auth: callback connection failed");
            }
        });
    }
}

async fn handle_loopback_conn(
    mut stream: tokio::net::TcpStream,
    auth: Auth,
) -> Result<(), std::io::Error> {
    // Read the request head (bounded; we only need the request line).
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "header read"))??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 16 * 1024 {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let request_line = head.lines().next().unwrap_or_default();
    let target = request_line.split_whitespace().nth(1).unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));

    let (status, body) = if path != "/callback" {
        ("404 Not Found", page("Not found."))
    } else {
        let params: HashMap<String, String> = query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.to_string(), url_decode(v)))
            .collect();
        let code = params.get("code");
        let state = params.get("state");
        let invalid_callback = || {
            (
                "400 Bad Request",
                page("Invalid or expired sign-in link. Start again from Zeron."),
            )
        };
        match (code, state) {
            (Some(code), Some(state)) => match auth.take_pending(state) {
                Some(generation) => match auth.exchange_code(code).await {
                    Ok(result) => match auth.finish_sign_in(result, generation) {
                        Ok(()) => (
                            "200 OK",
                            page("Signed in. You can close this tab and return to Zeron."),
                        ),
                        Err(err) => {
                            tracing::info!(error = %err, "auth: discarded canceled callback exchange");
                            (
                                "409 Conflict",
                                page(
                                    "This sign-in was canceled. Start again from Zeron if you still want to enable sync.",
                                ),
                            )
                        }
                    },
                    Err(err) => {
                        tracing::warn!(error = %err, "auth: loopback code exchange failed");
                        (
                            "502 Bad Gateway",
                            page("Sign-in failed during token exchange — check the Zeron logs."),
                        )
                    }
                },
                None => invalid_callback(),
            },
            _ => invalid_callback(),
        }
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

fn page(message: &str) -> String {
    format!("<html><body style='font-family:sans-serif;padding:2rem'>{message}</body></html>")
}

// ---------------------------------------------------------------------------
// Small utilities (JWT claims, base64url, URL encoding, 0600 writes)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct JwtClaims {
    #[serde(default)]
    exp: Option<i64>,
    #[serde(default)]
    iat: Option<i64>,
    #[serde(default)]
    org_id: Option<String>,
}

/// Decode (without verifying — the edge verifies) the JWT payload claims. Total: a
/// malformed token yields `None`, never a panic.
fn jwt_claims(token: &str) -> Option<JwtClaims> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64url_decode(payload)?;
    serde_json::from_slice(&bytes).ok()
}

fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4 + 3);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            b'=' => continue,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn url_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Atomically replace a private file: sibling temp (0600), file fsync, rename,
/// then parent-directory fsync. A crash can expose only the old or new record.
fn atomic_write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("auth session path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("session");
    let temp = parent.join(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));

    let outcome = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        #[cfg(unix)]
        file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp, path)?;
        sync_parent_directory(path)
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    outcome
}

fn sync_parent_directory(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::other("auth session path has no parent"))?;
        std::fs::File::open(parent)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use base64::Engine as _;
    use zeron_rpc::{RpcReply, RpcService, methods};

    use crate::rpc::AuthRpc;

    struct MockResponse {
        status: u16,
        body: serde_json::Value,
    }

    async fn mock_edge(
        responses: Vec<MockResponse>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind mock edge");
        let addr = listener.local_addr().expect("mock edge addr");
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.expect("accept mock request");
                let mut request = Vec::new();
                let mut chunk = [0u8; 2048];
                let (head_end, content_length) = loop {
                    let n = stream.read(&mut chunk).await.expect("read mock request");
                    assert!(n > 0, "request ended before its headers");
                    request.extend_from_slice(&chunk[..n]);
                    if let Some(head_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head_end = head_end + 4;
                        let head = String::from_utf8_lossy(&request[..head_end]);
                        let content_length = head
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .unwrap_or(0);
                        break (head_end, content_length);
                    }
                };
                while request.len() < head_end + content_length {
                    let n = stream
                        .read(&mut chunk)
                        .await
                        .expect("read mock request body");
                    assert!(n > 0, "request ended before its body");
                    request.extend_from_slice(&chunk[..n]);
                }
                requests.push(
                    String::from_utf8_lossy(&request)
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_string(),
                );

                let body = serde_json::to_vec(&response.body).expect("mock response json");
                let wire = format!(
                    "HTTP/1.1 {} Mock\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    response.status,
                    body.len()
                );
                stream.write_all(wire.as_bytes()).await.expect("write head");
                stream.write_all(&body).await.expect("write body");
                stream.shutdown().await.expect("close mock response");
            }
            requests
        });
        (format!("http://{addr}"), handle)
    }

    async fn mock_truncated_edge() -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind truncated mock edge");
        let addr = listener.local_addr().expect("truncated mock edge addr");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept truncated request");
            let mut request = Vec::new();
            let mut chunk = [0u8; 2048];
            loop {
                let n = stream.read(&mut chunk).await.expect("read request");
                assert!(n > 0, "request ended before its headers");
                request.extend_from_slice(&chunk[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request_line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 128\r\nconnection: close\r\n\r\n{\"ok\":",
                )
                .await
                .expect("write truncated response");
            stream.shutdown().await.expect("close truncated response");
            request_line
        });
        (format!("http://{addr}"), handle)
    }

    async fn pausing_refresh_edge(
        response: serde_json::Value,
    ) -> (
        String,
        tokio::sync::oneshot::Receiver<()>,
        Arc<tokio::sync::Notify>,
        tokio::task::JoinHandle<String>,
    ) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind pausing mock edge");
        let addr = listener.local_addr().expect("pausing mock edge addr");
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let server_release = release.clone();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept refresh request");
            let mut request = Vec::new();
            let mut chunk = [0u8; 2048];
            let (head_end, content_length) = loop {
                let n = stream.read(&mut chunk).await.expect("read refresh request");
                assert!(n > 0, "refresh request ended before its headers");
                request.extend_from_slice(&chunk[..n]);
                if let Some(head_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head_end = head_end + 4;
                    let head = String::from_utf8_lossy(&request[..head_end]);
                    let content_length = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    break (head_end, content_length);
                }
            };
            while request.len() < head_end + content_length {
                let n = stream
                    .read(&mut chunk)
                    .await
                    .expect("read refresh request body");
                assert!(n > 0, "refresh request ended before its body");
                request.extend_from_slice(&chunk[..n]);
            }
            let raw_request = String::from_utf8_lossy(&request).into_owned();
            received_tx.send(()).expect("signal refresh request");
            server_release.notified().await;
            let body = serde_json::to_vec(&response).expect("refresh response json");
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.expect("write head");
            stream.write_all(&body).await.expect("write body");
            stream.shutdown().await.expect("close refresh response");
            raw_request
        });
        (format!("http://{addr}"), received_rx, release, handle)
    }

    async fn read_mock_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut chunk = [0u8; 2048];
        let (head_end, content_length) = loop {
            let n = stream.read(&mut chunk).await.expect("read mock request");
            assert!(n > 0, "request ended before its headers");
            request.extend_from_slice(&chunk[..n]);
            if let Some(head_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                let head_end = head_end + 4;
                let head = String::from_utf8_lossy(&request[..head_end]);
                let content_length = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                break (head_end, content_length);
            }
        };
        while request.len() < head_end + content_length {
            let n = stream
                .read(&mut chunk)
                .await
                .expect("read mock request body");
            assert!(n > 0, "request ended before its body");
            request.extend_from_slice(&chunk[..n]);
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    async fn pausing_delete_edge() -> (
        String,
        tokio::sync::oneshot::Receiver<()>,
        Arc<tokio::sync::Notify>,
        tokio::task::JoinHandle<Vec<String>>,
    ) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind pausing delete edge");
        let addr = listener.local_addr().expect("pausing delete edge addr");
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let server_release = release.clone();
        let handle = tokio::spawn(async move {
            let (mut delete_stream, _) = listener.accept().await.expect("accept delete request");
            let delete_request = read_mock_request(&mut delete_stream).await;
            received_tx.send(()).expect("signal delete request");
            server_release.notified().await;
            let delete_body = br#"{"ok":true}"#;
            let delete_head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                delete_body.len()
            );
            delete_stream
                .write_all(delete_head.as_bytes())
                .await
                .expect("write delete head");
            delete_stream
                .write_all(delete_body)
                .await
                .expect("write delete body");
            delete_stream
                .shutdown()
                .await
                .expect("close delete response");

            let (mut list_stream, _) = listener.accept().await.expect("accept org list request");
            let list_request = read_mock_request(&mut list_stream).await;
            let list_body = br#"{"orgs":[]}"#;
            let list_head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                list_body.len()
            );
            list_stream
                .write_all(list_head.as_bytes())
                .await
                .expect("write org list head");
            list_stream
                .write_all(list_body)
                .await
                .expect("write org list body");
            list_stream
                .shutdown()
                .await
                .expect("close org list response");

            [delete_request, list_request]
                .into_iter()
                .map(|request| request.lines().next().unwrap_or_default().to_string())
                .collect()
        });
        (format!("http://{addr}"), received_rx, release, handle)
    }

    fn jwt_for_org(org_id: &str) -> String {
        let payload = serde_json::to_vec(&serde_json::json!({
            "iat": 1_700_000_000_i64,
            "exp": 4_100_000_000_i64,
            "org_id": org_id,
        }))
        .expect("jwt payload");
        format!(
            "header.{}.signature",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
        )
    }

    fn workos_test_config(edge_url: String, data_dir: PathBuf) -> AuthConfig {
        let mut config = AuthConfig::new(edge_url, data_dir);
        config.workos_client_id = Some("client_1".into());
        config
    }

    fn workos_auth(edge_url: String, data_dir: PathBuf, org_id: &str) -> Auth {
        let auth = Auth::new(workos_test_config(edge_url, data_dir));
        let user = AuthUser {
            id: "user_1".into(),
            email: "user@example.com".into(),
            name: Some("Example User".into()),
        };
        let session = StoredSession {
            refresh_token: "refresh_old".into(),
            user: user.clone(),
            org_id: Some(org_id.into()),
        };
        auth.persist_active_session(&session)
            .expect("persist test session");
        *lock(&auth.inner.stored) = Some(session);
        *lock(&auth.inner.access) = Some(AccessEntry::fresh(jwt_for_org(org_id)));
        auth.inner.state_tx.send_replace(AuthState::SignedIn {
            user,
            org_id: Some(org_id.into()),
        });
        auth
    }

    #[test]
    fn base64url_round_trips_jwt_payload() {
        let payload = br#"{"exp":100,"iat":40,"org_id":"org_1"}"#;
        // Standard base64url without padding (as JWTs use).
        let encoded = {
            const ALPHABET: &[u8] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            let mut out = String::new();
            for chunk in payload.chunks(3) {
                let b = [
                    chunk[0],
                    *chunk.get(1).unwrap_or(&0),
                    *chunk.get(2).unwrap_or(&0),
                ];
                let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
                out.push(ALPHABET[(n >> 18) as usize & 63] as char);
                out.push(ALPHABET[(n >> 12) as usize & 63] as char);
                if chunk.len() > 1 {
                    out.push(ALPHABET[(n >> 6) as usize & 63] as char);
                }
                if chunk.len() > 2 {
                    out.push(ALPHABET[n as usize & 63] as char);
                }
            }
            out
        };
        assert_eq!(
            base64url_decode(&encoded).as_deref(),
            Some(payload.as_slice())
        );
        let token = format!("h.{encoded}.sig");
        let claims = jwt_claims(&token).expect("claims decode");
        assert_eq!(claims.exp, Some(100));
        assert_eq!(claims.iat, Some(40));
        assert_eq!(claims.org_id.as_deref(), Some("org_1"));
    }

    #[test]
    fn url_coding_round_trips() {
        let raw = "http://127.0.0.1:1234/callback?x=a b&y=%";
        assert_eq!(url_decode(&url_encode(raw)), raw);
        assert_eq!(url_encode("a b"), "a%20b");
    }

    #[test]
    fn auth_state_serializes_as_proto_shape() {
        let user = AuthUser {
            id: "u1".into(),
            email: "u@x".into(),
            name: None,
        };
        let signed_in = AuthState::SignedIn {
            user: user.clone(),
            org_id: Some("org_1".into()),
        };
        let value = serde_json::to_value(&signed_in).expect("json");
        assert_eq!(
            value,
            serde_json::json!({
                "state": "signedIn",
                "user": {"id": "u1", "email": "u@x", "name": null},
                "orgId": "org_1",
            })
        );
        // The proto type itself round-trips the emitted value.
        let parsed: zeron_proto::AuthState = serde_json::from_value(value).expect("proto parse");
        assert!(matches!(parsed, zeron_proto::AuthState::SignedIn { .. }));
        assert_eq!(
            serde_json::to_value(AuthState::SignedOut).expect("json"),
            serde_json::json!({"state": "signedOut"})
        );
        assert_eq!(
            serde_json::to_value(AuthState::NeedsOrganization { user }).expect("json"),
            serde_json::json!({
                "state": "needsOrganization",
                "user": {"id": "u1", "email": "u@x", "name": null},
            })
        );
    }

    #[test]
    fn unpublished_session_is_fenced_when_active_manifest_publication_fails() {
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
            "org_prior",
        );
        let candidate = StoredSession {
            refresh_token: "refresh_unpublished".into(),
            user: lock(&auth.inner.stored)
                .as_ref()
                .expect("prior session")
                .user
                .clone(),
            org_id: Some("org_unpublished".into()),
        };
        let mut writes = 0;
        let error = auth
            .persist_active_session_with(&candidate, |path, bytes| {
                writes += 1;
                if writes == 3 {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "injected active publication failure",
                    ))
                } else {
                    atomic_write_private(path, bytes)
                }
            })
            .expect_err("active publication should fail");
        assert!(
            error.to_string().contains("could not be durably published"),
            "unexpected error: {error}"
        );
        assert_eq!(writes, 3);
        let disk: StoredSession = serde_json::from_slice(
            &std::fs::read(auth.session_file()).expect("unpublished session file"),
        )
        .expect("parse unpublished session");
        assert_eq!(disk, candidate);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("session fence")
            )
            .expect("parse session fence"),
            serde_json::json!({"state": "signedOut", "version": 1})
        );

        let restarted = Auth::new(workos_test_config(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
        ));
        assert_eq!(restarted.state(), AuthState::SignedOut);
    }

    #[test]
    fn invalid_or_unknown_session_manifest_fails_closed() {
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
            "org_prior",
        );
        atomic_write_private(
            &auth.session_state_file(),
            br#"{"state":"active","version":2}"#,
        )
        .expect("write unknown manifest version");

        let restarted = Auth::new(workos_test_config(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
        ));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert!(!restarted.loaded_workos_session());
    }

    #[cfg(unix)]
    #[test]
    fn atomic_session_files_are_private_and_leave_no_temp_files() {
        use std::os::unix::fs::PermissionsExt;

        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
            "org_prior",
        );
        for path in [auth.session_file(), auth.session_state_file()] {
            let mode = std::fs::metadata(&path)
                .expect("session file metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "{} must be owner-only", path.display());
        }
        let names = std::fs::read_dir(data_dir.path())
            .expect("read auth directory")
            .map(|entry| {
                entry
                    .expect("auth directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert!(
            names.iter().all(|name| !name.ends_with(".tmp")),
            "temporary files leaked: {names:?}"
        );
    }

    #[tokio::test]
    async fn authed_json_surfaces_edge_error_message_and_details() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 409,
            body: serde_json::json!({
                "error": "operation rejected",
                "message": "member update failed",
                "details": {"reason": "cannot remove the last admin"},
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_1");

        let error = auth
            .list_members("org_1")
            .await
            .expect_err("edge rejection should surface");
        assert_eq!(
            error.to_string(),
            "workspace request failed (409): operation rejected; member update failed; cannot remove the last admin"
        );
        let requests = server.await.expect("mock edge completed");
        assert_eq!(requests, ["GET /auth/orgs/org_1/members HTTP/1.1"]);
    }

    #[tokio::test]
    async fn create_org_returns_and_durably_selects_the_minted_id() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"organizationId": "org_minted"}),
            },
            MockResponse {
                status: 200,
                body: serde_json::json!({
                    "accessToken": jwt_for_org("org_minted"),
                    "refreshToken": "refresh_minted"
                }),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_prior");

        let organization_id = auth
            .create_org("Minted Workspace")
            .await
            .expect("create and select workspace");
        assert_eq!(organization_id, "org_minted");
        assert_eq!(auth.state().org_id(), Some("org_minted"));
        let disk: StoredSession = serde_json::from_slice(
            &std::fs::read(auth.session_file()).expect("minted session persisted"),
        )
        .expect("parse minted session");
        assert_eq!(disk.org_id.as_deref(), Some("org_minted"));
        assert_eq!(disk.refresh_token, "refresh_minted");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("active session manifest")
            )
            .expect("parse active manifest"),
            serde_json::json!({"state": "active", "version": 1})
        );
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/orgs HTTP/1.1", "POST /auth/refresh HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn create_org_rolls_back_after_a_definite_select_rejection_without_deadlock() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"organizationId": "org_rolled_back"}),
            },
            MockResponse {
                status: 403,
                body: serde_json::json!({"error": "selection forbidden"}),
            },
            MockResponse {
                status: 200,
                body: serde_json::json!({"ok": true}),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_prior");

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            auth.create_org("Rollback Workspace"),
        )
        .await
        .expect("create rollback must not deadlock")
        .expect_err("selection should fail");
        let message = error.to_string();
        assert!(
            message.contains("org_rolled_back"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("was rolled back and creation may be retried"),
            "unexpected error: {message}"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        assert_eq!(
            lock(&auth.inner.stored)
                .as_ref()
                .and_then(|session| session.org_id.as_deref()),
            Some("org_prior")
        );
        assert_eq!(
            server.await.expect("mock edge completed"),
            [
                "POST /auth/orgs HTTP/1.1",
                "POST /auth/refresh HTTP/1.1",
                "DELETE /auth/orgs/org_rolled_back HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn create_org_uses_the_pinned_bearer_to_rollback_an_ambiguous_select() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"organizationId": "org_ambiguous"}),
            },
            MockResponse {
                status: 200,
                body: serde_json::json!({"unexpected": "rotated response"}),
            },
            MockResponse {
                status: 200,
                body: serde_json::json!({"ok": true}),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .create_org("Ambiguous Workspace")
            .await
            .expect_err("ambiguous selection must fail after compensation");
        let message = error.to_string();
        assert!(
            message.contains("org_ambiguous"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("was rolled back and creation may be retried"),
            "unexpected error: {message}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("mock edge completed"),
            [
                "POST /auth/orgs HTTP/1.1",
                "POST /auth/refresh HTTP/1.1",
                "DELETE /auth/orgs/org_ambiguous HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn create_org_failed_rollback_identifies_the_existing_org_and_forbids_blind_retry() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"organizationId": "org_maybe_exists"}),
            },
            MockResponse {
                status: 403,
                body: serde_json::json!({"error": "selection forbidden"}),
            },
            MockResponse {
                status: 503,
                body: serde_json::json!({"error": "rollback unavailable"}),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .create_org("Uncertain Workspace")
            .await
            .expect_err("rollback should fail");
        let message = error.to_string();
        assert!(
            message.contains("organizationId=org_maybe_exists"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("do not retry creation blindly"),
            "unexpected error: {message}"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        assert_eq!(
            server.await.expect("mock edge completed"),
            [
                "POST /auth/orgs HTTP/1.1",
                "POST /auth/refresh HTTP/1.1",
                "DELETE /auth/orgs/org_maybe_exists HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn select_org_rejection_restores_the_prior_durable_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 403,
            body: serde_json::json!({
                "error": "forbidden",
                "message": "workspace selection rejected",
                "details": {"reason": "membership was removed"},
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .select_org("org_denied")
            .await
            .expect_err("selection should be rejected");
        assert_eq!(
            error.to_string(),
            "refresh failed (403): forbidden; workspace selection rejected; membership was removed"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        let stored = lock(&auth.inner.stored).clone().expect("prior session");
        assert_eq!(stored.org_id.as_deref(), Some("org_prior"));
        assert_eq!(stored.refresh_token, "refresh_old");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("active session manifest")
            )
            .expect("parse session manifest"),
            serde_json::json!({"state": "active", "version": 1})
        );

        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
        let requests = server.await.expect("mock edge completed");
        assert_eq!(requests, ["POST /auth/refresh HTTP/1.1"]);
    }

    #[tokio::test]
    async fn routine_refresh_transient_response_preserves_the_prior_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 503,
            body: serde_json::json!({
                "error": "workos rate limited or unavailable",
                "transient": true,
                "outcomeUnknown": false,
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");
        *lock(&auth.inner.access) = None;

        let error = auth
            .refresh(None)
            .await
            .expect_err("transient routine refresh should be retryable");
        assert_eq!(
            error.to_string(),
            "refresh failed (503): workos rate limited or unavailable"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        assert_eq!(
            lock(&auth.inner.stored)
                .as_ref()
                .map(|session| session.refresh_token.as_str()),
            Some("refresh_old")
        );
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/refresh HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn routine_refresh_unknown_upstream_outcome_preserves_the_prior_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 502,
            body: serde_json::json!({
                "error": "failed after token rotation",
                "transient": true,
                "outcomeUnknown": true,
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");
        *lock(&auth.inner.access) = None;

        let error = auth
            .refresh(None)
            .await
            .expect_err("unknown routine refresh should remain retryable");
        assert_eq!(
            error.to_string(),
            "refresh failed (502): failed after token rotation"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        assert_eq!(
            lock(&auth.inner.stored)
                .as_ref()
                .map(|session| session.refresh_token.as_str()),
            Some("refresh_old")
        );
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/refresh HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn select_org_transient_rejection_restores_the_prior_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 503,
            body: serde_json::json!({
                "error": "workos rate limited",
                "transient": true,
                "outcomeUnknown": false,
                "upstreamStatus": 429,
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .select_org("org_target")
            .await
            .expect_err("rate-limited profile refresh should be rejected");
        assert_eq!(
            error.to_string(),
            "refresh failed (503): workos rate limited"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/refresh HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn select_org_unknown_upstream_outcome_keeps_the_signed_out_fence() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 502,
            body: serde_json::json!({
                "error": "failed after token rotation",
                "transient": true,
                "outcomeUnknown": true,
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .select_org("org_target")
            .await
            .expect_err("unknown profile-refresh outcome must fail closed");
        assert!(
            error
                .to_string()
                .contains("WorkOS reported that the refresh outcome is unknown"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/refresh HTTP/1.1"]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn select_org_stops_before_refresh_when_the_durable_fence_cannot_be_written() {
        use std::os::unix::fs::PermissionsExt;

        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
            "org_prior",
        );
        std::fs::set_permissions(data_dir.path(), std::fs::Permissions::from_mode(0o500))
            .expect("make auth directory read-only");
        let result = auth.select_org("org_target").await;
        std::fs::set_permissions(data_dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("restore auth directory permissions");

        let error = result.expect_err("selection must fail before network refresh");
        assert!(
            error
                .to_string()
                .contains("could not durably invalidate auth session"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("active session manifest")
            )
            .expect("parse active manifest"),
            serde_json::json!({"state": "active", "version": 1})
        );
        let restarted = Auth::new(workos_test_config(
            "http://127.0.0.1:9".into(),
            data_dir.path().to_path_buf(),
        ));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
    }

    #[tokio::test]
    async fn malformed_select_success_invalidates_the_possibly_rotated_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 200,
            body: serde_json::json!({"unexpected": true}),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .select_org("org_target")
            .await
            .expect_err("malformed success is ambiguous");
        assert!(
            error.to_string().contains("refresh outcome is uncertain"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/refresh HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn truncated_select_response_keeps_the_signed_out_fence() {
        let (edge_url, server) = mock_truncated_edge().await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .select_org("org_target")
            .await
            .expect_err("truncated response leaves profile refresh uncertain");
        assert!(
            error.to_string().contains("refresh outcome is uncertain"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("signed-out fence")
            )
            .expect("parse signed-out fence"),
            serde_json::json!({"state": "signedOut", "version": 1})
        );
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("truncated mock edge completed"),
            "POST /auth/refresh HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn mismatched_select_token_invalidates_the_possibly_rotated_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 200,
            body: serde_json::json!({
                "accessToken": jwt_for_org("org_wrong"),
                "refreshToken": "refresh_rotated"
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .select_org("org_target")
            .await
            .expect_err("wrong org claim must fail");
        assert!(
            error
                .to_string()
                .contains("the refresh token may have rotated"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["POST /auth/refresh HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn select_org_does_not_report_success_over_a_concurrent_sign_in() {
        let (edge_url, received, release, server) = pausing_refresh_edge(serde_json::json!({
            "accessToken": jwt_for_org("org_target"),
            "refreshToken": "refresh_target"
        }))
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_prior");
        let switching_auth = auth.clone();
        let switching = tokio::spawn(async move { switching_auth.select_org("org_target").await });
        received.await.expect("refresh request reached edge");

        let generation = lock(&auth.inner.sign_in).generation;
        let sign_in_error = auth
            .finish_sign_in(
                SignInResult {
                    user: AuthUser {
                        id: "user_concurrent".into(),
                        email: "concurrent@example.com".into(),
                        name: Some("Concurrent User".into()),
                    },
                    access_token: jwt_for_org("org_concurrent"),
                    refresh_token: "refresh_concurrent".into(),
                },
                generation,
            )
            .expect_err("profile transition must cancel a concurrent sign-in callback");
        assert!(
            sign_in_error.to_string().contains("sign-in was canceled"),
            "unexpected error: {sign_in_error}"
        );
        release.notify_one();

        switching
            .await
            .expect("switch task completed")
            .expect("profile transition should commit after the callback is rejected");
        assert_eq!(auth.state().org_id(), Some("org_target"));
        let stored = lock(&auth.inner.stored)
            .clone()
            .expect("selected session preserved");
        assert_eq!(stored.org_id.as_deref(), Some("org_target"));
        assert_eq!(stored.refresh_token, "refresh_target");
        let disk: StoredSession = serde_json::from_slice(
            &std::fs::read(auth.session_file()).expect("selected session persisted"),
        )
        .expect("parse selected session");
        assert_eq!(disk, stored);
        let request = server.await.expect("pausing mock edge completed");
        assert!(request.starts_with("POST /auth/refresh HTTP/1.1\r\n"));
        let body: serde_json::Value = serde_json::from_str(
            request
                .split_once("\r\n\r\n")
                .expect("refresh request has body")
                .1,
        )
        .expect("refresh request json");
        assert_eq!(
            body,
            serde_json::json!({
                "refreshToken": "refresh_old",
                "organizationId": "org_target"
            })
        );
    }

    #[tokio::test]
    async fn sign_out_wins_over_an_in_flight_routine_refresh() {
        let (edge_url, received, release, server) = pausing_refresh_edge(serde_json::json!({
            "accessToken": jwt_for_org("org_prior"),
            "refreshToken": "refresh_rotated"
        }))
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");
        *lock(&auth.inner.access) = None;
        let refreshing_auth = auth.clone();
        let refreshing = tokio::spawn(async move { refreshing_auth.refresh(None).await });
        received.await.expect("refresh request reached edge");

        auth.sign_out().expect("durably sign out during refresh");
        release.notify_one();
        assert_eq!(
            refreshing
                .await
                .expect("refresh task completed")
                .expect("refresh result"),
            None
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        assert!(lock(&auth.inner.access).is_none());
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        let request = server.await.expect("pausing mock edge completed");
        let body: serde_json::Value = serde_json::from_str(
            request
                .split_once("\r\n\r\n")
                .expect("refresh request has body")
                .1,
        )
        .expect("refresh request json");
        assert_eq!(body, serde_json::json!({"refreshToken": "refresh_old"}));
    }

    #[tokio::test]
    async fn refresh_persistence_failure_keeps_fence_and_clears_memory() {
        let (edge_url, received, release, server) = pausing_refresh_edge(serde_json::json!({
            "accessToken": jwt_for_org("org_prior"),
            "refreshToken": "refresh_rotated"
        }))
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");
        *lock(&auth.inner.access) = None;
        let refreshing_auth = auth.clone();
        let refreshing = tokio::spawn(async move { refreshing_auth.refresh(None).await });
        received.await.expect("refresh request reached edge");

        std::fs::remove_file(auth.session_file()).expect("remove old session file");
        std::fs::create_dir(auth.session_file()).expect("block session replacement with directory");
        release.notify_one();
        let error = refreshing
            .await
            .expect("refresh task completed")
            .expect_err("refresh persistence should fail");
        assert!(
            error.to_string().contains("could not be durably committed"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        assert!(lock(&auth.inner.access).is_none());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("signed-out fence")
            )
            .expect("parse signed-out fence"),
            serde_json::json!({"state": "signedOut", "version": 1})
        );
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        let names = std::fs::read_dir(data_dir.path())
            .expect("read auth directory")
            .map(|entry| {
                entry
                    .expect("auth directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert!(
            names.iter().all(|name| !name.ends_with(".tmp")),
            "failed atomic write leaked a temporary file: {names:?}"
        );
        let _ = server.await.expect("pausing mock edge completed");
    }

    #[tokio::test]
    async fn delete_org_rejection_restores_the_prior_durable_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 403,
            body: serde_json::json!({
                "error": "forbidden",
                "message": "only an admin can delete this workspace",
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .delete_org("org_prior")
            .await
            .expect_err("delete should be rejected");
        assert_eq!(
            error.to_string(),
            "workspace request failed (403): forbidden; only an admin can delete this workspace"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        let stored = lock(&auth.inner.stored).clone().expect("prior session");
        assert_eq!(stored.org_id.as_deref(), Some("org_prior"));
        assert_eq!(stored.refresh_token, "refresh_old");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("active session manifest")
            )
            .expect("parse session manifest"),
            serde_json::json!({"state": "active", "version": 1})
        );

        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
        let requests = server.await.expect("mock edge completed");
        assert_eq!(requests, ["DELETE /auth/orgs/org_prior HTTP/1.1"]);
    }

    #[tokio::test]
    async fn delete_org_transient_rejection_restores_the_prior_session() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 503,
            body: serde_json::json!({
                "error": "workos rate limited",
                "transient": true,
                "outcomeUnknown": false,
                "upstreamStatus": 429,
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .delete_org("org_prior")
            .await
            .expect_err("rate-limited delete should be rejected");
        assert_eq!(
            error.to_string(),
            "workspace request failed (503): workos rate limited"
        );
        assert_eq!(auth.state().org_id(), Some("org_prior"));
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state().org_id(), Some("org_prior"));
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["DELETE /auth/orgs/org_prior HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn delete_org_unknown_upstream_outcome_keeps_the_signed_out_fence() {
        let (edge_url, server) = mock_edge(vec![MockResponse {
            status: 502,
            body: serde_json::json!({
                "error": "failed after workspace deletion",
                "transient": true,
                "outcomeUnknown": true,
            }),
        }])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .delete_org("org_prior")
            .await
            .expect_err("unknown delete outcome must fail closed");
        assert!(
            error.to_string().contains("deletion outcome is uncertain"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("mock edge completed"),
            ["DELETE /auth/orgs/org_prior HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn ambiguous_delete_keeps_the_signed_out_fence_across_restart() {
        let (edge_url, server) = mock_truncated_edge().await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url.clone(), data_dir.path().to_path_buf(), "org_prior");

        let error = auth
            .delete_org("org_prior")
            .await
            .expect_err("truncated response leaves delete outcome uncertain");
        assert!(
            error.to_string().contains("deletion outcome is uncertain"),
            "unexpected error: {error}"
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(auth.session_state_file()).expect("signed-out session manifest")
            )
            .expect("parse session manifest"),
            serde_json::json!({"state": "signedOut", "version": 1})
        );

        let restarted = Auth::new(workos_test_config(edge_url, data_dir.path().to_path_buf()));
        assert_eq!(restarted.state(), AuthState::SignedOut);
        assert_eq!(
            server.await.expect("truncated mock edge completed"),
            "DELETE /auth/orgs/org_prior HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn delete_org_selects_and_persists_a_remaining_membership() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"ok": true}),
            },
            MockResponse {
                status: 200,
                // Include the just-deleted org to cover WorkOS eventual consistency.
                body: serde_json::json!({"orgs": [
                    {
                        "id": "membership_deleted",
                        "organizationId": "org_deleted",
                        "name": "Deleted",
                        "role": "admin"
                    },
                    {
                        "id": "membership_fallback",
                        "organizationId": "org_fallback",
                        "name": "Fallback",
                        "role": "member"
                    }
                ]}),
            },
            MockResponse {
                status: 200,
                body: serde_json::json!({
                    "accessToken": jwt_for_org("org_fallback"),
                    "refreshToken": "refresh_rotated"
                }),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_deleted");

        let outcome = auth
            .delete_org("org_deleted")
            .await
            .expect("delete and fallback");
        assert_eq!(
            outcome,
            DeleteOrgOutcome::Switched {
                organization_id: "org_fallback".into()
            }
        );
        assert_eq!(
            serde_json::to_value(&outcome).expect("serialize delete outcome"),
            serde_json::json!({
                "action": "switched",
                "organizationId": "org_fallback"
            })
        );
        assert_eq!(auth.state().org_id(), Some("org_fallback"));
        let stored = lock(&auth.inner.stored).clone().expect("stored session");
        assert_eq!(stored.org_id.as_deref(), Some("org_fallback"));
        assert_eq!(stored.refresh_token, "refresh_rotated");
        let disk: StoredSession = serde_json::from_slice(
            &std::fs::read(auth.session_file()).expect("persisted fallback session"),
        )
        .expect("parse persisted session");
        assert_eq!(disk.org_id.as_deref(), Some("org_fallback"));

        let requests = server.await.expect("mock edge completed");
        assert_eq!(
            requests,
            [
                "DELETE /auth/orgs/org_deleted HTTP/1.1",
                "GET /auth/orgs HTTP/1.1",
                "POST /auth/refresh HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn delete_last_org_signs_out_and_removes_the_stale_session() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"ok": true}),
            },
            MockResponse {
                status: 200,
                body: serde_json::json!({"orgs": []}),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_deleted");
        assert!(auth.session_file().exists());

        let outcome = auth
            .delete_org("org_deleted")
            .await
            .expect("delete last org");
        assert_eq!(outcome, DeleteOrgOutcome::SignedOut);
        assert_eq!(
            serde_json::to_value(&outcome).expect("serialize delete outcome"),
            serde_json::json!({"action": "signedOut"})
        );
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        assert!(lock(&auth.inner.access).is_none());
        assert!(!auth.session_file().exists());

        let requests = server.await.expect("mock edge completed");
        assert_eq!(
            requests,
            [
                "DELETE /auth/orgs/org_deleted HTTP/1.1",
                "GET /auth/orgs HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn delete_org_rpc_rejects_an_in_flight_sign_in_and_reports_its_real_state() {
        let (edge_url, received, release, server) = pausing_delete_edge().await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_deleted");
        // Model a callback that consumed its OAuth state before deletion began,
        // then finished exchanging its code while DELETE was in flight.
        let callback_generation = lock(&auth.inner.sign_in).generation;

        let rpc = AuthRpc::new(auth.clone());
        let deleting = tokio::spawn(async move {
            rpc.handle(
                methods::DELETE_ORG,
                serde_json::json!({"organizationId": "org_deleted"}),
            )
            .await
        });
        received.await.expect("delete request reached edge");

        let sign_in_error = auth
            .finish_sign_in(
                SignInResult {
                    user: AuthUser {
                        id: "user_concurrent".into(),
                        email: "concurrent@example.com".into(),
                        name: Some("Concurrent User".into()),
                    },
                    access_token: jwt_for_org("org_concurrent"),
                    refresh_token: "refresh_concurrent".into(),
                },
                callback_generation,
            )
            .expect_err("profile deletion must cancel the old sign-in callback");
        assert!(
            sign_in_error.to_string().contains("sign-in was canceled"),
            "unexpected error: {sign_in_error}"
        );
        release.notify_one();

        let reply = deleting
            .await
            .expect("delete RPC task completed")
            .expect("delete RPC succeeded");
        let RpcReply::Value(value) = reply else {
            panic!("delete RPC must return one stable lifecycle outcome");
        };
        assert_eq!(value, serde_json::json!({"action": "signedOut"}));
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(lock(&auth.inner.stored).is_none());
        assert_eq!(
            server.await.expect("pausing delete edge completed"),
            [
                "DELETE /auth/orgs/org_deleted HTTP/1.1",
                "GET /auth/orgs HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn delete_org_clears_stale_session_when_fallback_discovery_fails() {
        let (edge_url, server) = mock_edge(vec![
            MockResponse {
                status: 200,
                body: serde_json::json!({"ok": true}),
            },
            MockResponse {
                status: 503,
                body: serde_json::json!({"error": "membership service unavailable"}),
            },
        ])
        .await;
        let data_dir = tempfile::tempdir().expect("temp auth dir");
        let auth = workos_auth(edge_url, data_dir.path().to_path_buf(), "org_deleted");

        let outcome = auth
            .delete_org("org_deleted")
            .await
            .expect("successful delete has a safe outcome");
        assert_eq!(outcome, DeleteOrgOutcome::SignedOut);
        assert_eq!(auth.state(), AuthState::SignedOut);
        assert!(!auth.session_file().exists());

        let requests = server.await.expect("mock edge completed");
        assert_eq!(
            requests,
            [
                "DELETE /auth/orgs/org_deleted HTTP/1.1",
                "GET /auth/orgs HTTP/1.1",
            ]
        );
    }
}
