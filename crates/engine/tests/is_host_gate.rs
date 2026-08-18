//! `WorkspaceHost::is_host` fail-closed matrix (org-shared registry): an
//! engine that has never synced its registry room must NOT believe it hosts a
//! chat it has no row for — in a shared registry that window would
//! double-execute a teammate's commands. Edge-less hosts keep the original
//! claim-anything behavior (local-only chats must still run).

use std::sync::Arc;

use zeron_engine::{WorkspaceHost, WorkspaceHostConfig};
use zeron_sync::DocsStore;

fn open_host(dir: &std::path::Path, edge: Option<zeron_engine::EdgeConfig>) -> WorkspaceHost {
    let store = Arc::new(DocsStore::open(dir).expect("store opens"));
    WorkspaceHost::open(
        store,
        WorkspaceHostConfig {
            device_id: "dev-me".into(),
            device_name: "test".into(),
            platform: "linux".into(),
            org_id: "org-test".into(),
            user_id: "alice".into(),
            edge,
        },
    )
    .expect("workspace host opens")
}

#[tokio::test]
async fn edgeless_host_claims_unknown_chats() {
    let dir = tempfile::tempdir().unwrap();
    let host = open_host(dir.path(), None);
    assert!(host.is_host("never-seen-chat"));
}

#[tokio::test]
async fn edged_host_is_fail_closed_before_first_registry_sync() {
    let dir = tempfile::tempdir().unwrap();
    // Unroutable edge: the registry join keeps failing, synced_once stays false.
    let edge = zeron_engine::EdgeConfig::with_static_token("http://127.0.0.1:1", "alice");
    let host = open_host(dir.path(), Some(edge));
    assert!(
        !host.is_host("never-seen-chat"),
        "unknown chat must not be claimable before the first registry sync"
    );
}

#[tokio::test]
async fn row_ownership_is_authoritative_regardless_of_sync_state() {
    let dir = tempfile::tempdir().unwrap();
    let edge = zeron_engine::EdgeConfig::with_static_token("http://127.0.0.1:1", "alice");
    let host = open_host(dir.path(), Some(edge));
    host.create_space("space-1", "dev-me", "/tmp/repo", None, true)
        .expect("space");
    host.create_chat("chat-own", Some("space-1"), None, None, None)
        .expect("own chat");
    host.create_space("space-2", "dev-bob", "/tmp/other", None, true)
        .expect("space");
    host.create_chat("chat-foreign", Some("space-2"), None, None, None)
        .expect("foreign chat");
    assert!(host.is_host("chat-own"));
    assert!(!host.is_host("chat-foreign"));
}
