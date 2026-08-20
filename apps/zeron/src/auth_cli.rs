//! `zeron login` / `zeron logout` / `zeron status` — the standalone auth surface.
//!
//! Sign-in used to live only inside `zeron headless`, coupling authentication to
//! the long-running daemon. These commands work on the persisted session
//! (`{data_dir}/session.json`) and exit, so a service-managed `zeron headless`
//! only ever *loads* credentials. While an engine is running it owns the session
//! (WorkOS refresh tokens are single-use and rotate on every refresh), so login
//! and logout take the same data-dir lock the engine holds and refuse politely
//! when it is busy.

use std::io::IsTerminal;

use zeron_engine::{AuthState, Engine, EngineConfig, InstanceLock, ProfileScope};

#[derive(Debug, PartialEq, Eq)]
struct AccountStatus {
    mode: &'static str,
    auth: String,
    healthy: bool,
}

fn account_status(scope: ProfileScope, auth: &AuthState) -> AccountStatus {
    match scope {
        ProfileScope::Local => match auth {
            AuthState::SignedOut => AccountStatus {
                mode: "local only",
                auth: "signed out (optional in local-only mode)".into(),
                healthy: true,
            },
            AuthState::NeedsOrganization { user } => AccountStatus {
                mode: "local only",
                auth: format!(
                    "signed in as {}; finish organization setup to enable sync",
                    user.email
                ),
                healthy: true,
            },
            AuthState::SignedIn { user, .. } => AccountStatus {
                mode: "local only",
                auth: format!("signed in as {}; sync is ready after restart", user.email),
                healthy: true,
            },
        },
        ProfileScope::Development => AccountStatus {
            mode: "development",
            auth: "dev mode (bearer = user id)".into(),
            healthy: true,
        },
        ProfileScope::Synced => match auth {
            AuthState::SignedIn {
                user,
                organization_id,
            } => AccountStatus {
                mode: "synced",
                auth: format!(
                    "signed in as {}{}",
                    user.email,
                    organization_id
                        .as_ref()
                        .map(|organization| format!(" (organization {organization})"))
                        .unwrap_or_default()
                ),
                healthy: true,
            },
            AuthState::NeedsOrganization { user } => AccountStatus {
                mode: "synced",
                auth: format!(
                    "signed in as {} but no organization selected — run `zeron login`",
                    user.email
                ),
                healthy: false,
            },
            AuthState::SignedOut => AccountStatus {
                mode: "synced",
                auth: "saved session is no longer valid — run `zeron login`".into(),
                healthy: false,
            },
        },
    }
}

/// `zeron login`: authenticate via the paste-code flow (and organization
/// onboarding), persist `session.json`, and exit.
pub async fn login(config: EngineConfig) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.data_dir)?;
    let _lock = engine_lock(&config, "sign in")?;
    let auth = Engine::build_auth(&config).await;
    if !auth.workos_enabled() {
        println!("Auth is in dev mode (no WorkOS client id) — there is nothing to sign in to.");
        return Ok(());
    }
    if let AuthState::SignedIn {
        user,
        organization_id,
    } = auth.state()
    {
        println!(
            "Already signed in as {}{}.",
            user.email,
            organization_id
                .map(|organization| format!(" (organization {organization})"))
                .unwrap_or_default()
        );
        println!("Run `zeron logout` first to switch accounts.");
        println!("The next engine start will use the synced profile.");
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("zeron login needs an interactive terminal");
    }
    zeron_engine::terminal_sign_in(&auth).await?;
    match auth.state() {
        AuthState::SignedIn {
            user,
            organization_id,
        } => {
            println!(
                "\nSigned in as {}{}.",
                user.email,
                organization_id
                    .map(|organization| format!(" (organization {organization})"))
                    .unwrap_or_default()
            );
            println!(
                "Sync is ready. Start or restart Zeron to open the organization's synced data; existing local sessions will stay local."
            );
        }
        // terminal_sign_in only returns Ok once signed in; keep an honest fallback.
        _ => println!("Sign-in did not complete."),
    }
    Ok(())
}

/// `zeron logout`: remove the persisted session.
pub async fn logout(config: EngineConfig) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.data_dir)?;
    let _lock = engine_lock(&config, "sign out")?;
    let auth = Engine::build_auth(&config).await;
    if !auth.workos_enabled() {
        // Dev mode has no live session, but clear any stale session.json from a
        // previous WorkOS-mode run so the next real run starts signed out.
        auth.sign_out().map_err(|error| {
            anyhow::anyhow!("could not durably clear the saved auth session: {error}")
        })?;
        println!("Auth is in dev mode — cleared any stale saved session.");
        println!("The next engine start will remain in development mode.");
        return Ok(());
    }
    match auth.state() {
        AuthState::SignedOut => {
            // Also remove malformed or stale files that could not be loaded.
            auth.sign_out().map_err(|error| {
                anyhow::anyhow!("could not durably clear the stale auth session: {error}")
            })?;
            println!("No valid saved session; cleared any stale session file.");
        }
        state => {
            let email = state
                .user()
                .map(|u| u.email.clone())
                .unwrap_or_else(|| "<unknown>".into());
            auth.sign_out()
                .map_err(|error| anyhow::anyhow!("could not durably sign out {email}: {error}"))?;
            println!(
                "Signed out {email} — removed {}.",
                config.data_dir.join("session.json").display()
            );
        }
    }
    println!("The next engine start will use the local-only profile.");
    Ok(())
}

/// `zeron status`: report the fixed scope a new engine would select, optional
/// auth, and engine liveness. Local-only is a healthy signed-out state.
pub async fn status(config: EngineConfig) -> anyhow::Result<()> {
    let auth = Engine::build_auth(&config).await;
    let next_scope = Engine::initial_profile_scope(&auth);
    let scope = live_engine_profile_scope(config.ipc_port)
        .await
        .unwrap_or(next_scope);
    let account = account_status(scope, &auth.state());
    println!("Data dir: {}", config.data_dir.display());
    println!("Edge:     {}", config.edge_url);
    println!("Mode:     {}", account.mode);
    println!("Auth:     {}", account.auth);
    match InstanceLock::holder(&config.data_dir) {
        Some(pid) => println!("Engine:   running (pid {pid})"),
        None => println!("Engine:   not running"),
    }
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], config.ipc_port));
    let ipc = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500));
    println!(
        "IPC:      {} 127.0.0.1:{}",
        if ipc.is_ok() {
            "listening on"
        } else {
            "not listening on"
        },
        config.ipc_port
    );
    if !account.healthy {
        std::process::exit(1);
    }
    Ok(())
}

/// Prefer the immutable scope of a live runtime. Falling back to the next-boot
/// derivation is correct when no engine is listening and tolerant of old
/// daemons that predate EngineInfo.
async fn live_engine_profile_scope(ipc_port: u16) -> Option<ProfileScope> {
    let client = zeron_rpc::connect_ws(&format!("ws://127.0.0.1:{ipc_port}"))
        .await
        .ok()?;
    let value = client
        .call(zeron_rpc::methods::ENGINE_INFO, serde_json::json!({}))
        .await
        .ok()?;
    serde_json::from_value::<zeron_engine::EngineInfo>(value)
        .ok()
        .map(|info| info.profile_scope)
}

/// The same exclusive data-dir lock the engine holds for its lifetime: taken for
/// the whole login/logout mutation so we never rotate or delete a session out
/// from under a running engine (whose in-memory copy would fight back — the next
/// token refresh re-persists it).
fn engine_lock(config: &EngineConfig, verb: &str) -> anyhow::Result<InstanceLock> {
    InstanceLock::acquire(&config.data_dir).map_err(|err| {
        anyhow::anyhow!(
            "{err}\nCannot {verb} while an engine is running — stop it first \
             (`zeron daemon stop`, or quit the Zeron app), or use the running UI instead."
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeron_engine::{AuthUser, HarnessId};

    fn config(data_dir: &std::path::Path) -> EngineConfig {
        EngineConfig {
            data_dir: data_dir.to_path_buf(),
            edge_url: "http://127.0.0.1:1".into(),
            edge_token: None,
            ipc_port: 0,
            default_harness: HarnessId::Mock,
            organization_id: None,
            workos_client_id: Some("client_test".into()),
        }
    }

    #[test]
    fn signed_out_local_status_is_healthy() {
        let status = account_status(ProfileScope::Local, &AuthState::SignedOut);
        assert_eq!(status.mode, "local only");
        assert_eq!(status.auth, "signed out (optional in local-only mode)");
        assert!(status.healthy);
    }

    #[test]
    fn synced_status_still_requires_a_complete_account() {
        let user = AuthUser {
            id: "user-1".into(),
            email: "user@example.com".into(),
            name: None,
        };
        assert!(
            !account_status(
                ProfileScope::Synced,
                &AuthState::NeedsOrganization { user: user.clone() },
            )
            .healthy
        );
        assert!(
            account_status(
                ProfileScope::Synced,
                &AuthState::SignedIn {
                    user,
                    organization_id: Some("org-1".into()),
                },
            )
            .healthy
        );
    }

    #[cfg(unix)]
    #[test]
    fn login_and_logout_lock_out_a_running_engine() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _engine = InstanceLock::acquire(dir.path()).unwrap();

        for verb in ["sign in", "sign out"] {
            let error = engine_lock(&config, verb).unwrap_err().to_string();
            assert!(error.contains("while an engine is running"));
            assert!(error.contains("stop it first"));
        }
    }

    #[tokio::test]
    async fn logout_makes_the_next_engine_start_local() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let session = dir.path().join("session.json");
        std::fs::write(
            &session,
            r#"{"refreshToken":"refresh-1","user":{"id":"user-1","email":"user@example.com"},"orgId":"org-1"}"#,
        )
        .unwrap();
        let before = Engine::build_auth(&config).await;
        assert_eq!(Engine::initial_profile_scope(&before), ProfileScope::Synced);

        logout(config.clone()).await.unwrap();

        assert!(!session.exists());
        let after = Engine::build_auth(&config).await;
        assert_eq!(Engine::initial_profile_scope(&after), ProfileScope::Local);
    }

    #[tokio::test]
    async fn logout_reports_a_durable_invalidation_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        std::fs::create_dir(dir.path().join("session.state")).unwrap();

        let error = logout(config).await.unwrap_err().to_string();
        assert!(
            error.contains("could not durably clear the stale auth session"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("could not durably invalidate auth session"),
            "unexpected error: {error}"
        );
    }
}
