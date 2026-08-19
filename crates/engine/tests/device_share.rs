//! Device-share opt-out (guest mode) + retired-device deletion.
//!
//! Off = this device withdraws its registry row, refuses every host path
//! (is_host / claim / createChat / createSpace targeting itself), and the
//! choice persists per profile across reboots. Deleting a device cascades to
//! its spaces and chats; the local device is refused (the toggle is the way).

use std::sync::Arc;

use zeron_engine::{WorkspaceHost, WorkspaceHostConfig};
use zeron_sync::DocsStore;

fn open_host(store: Arc<DocsStore>) -> WorkspaceHost {
    WorkspaceHost::open(
        store,
        WorkspaceHostConfig {
            device_id: "dev-me".into(),
            device_name: "test".into(),
            platform: "linux".into(),
            org_id: "org-test".into(),
            user_id: "alice".into(),
            edge: None,
        },
    )
    .expect("workspace host opens")
}

fn device_ids(host: &WorkspaceHost) -> Vec<String> {
    host.read_devices()
        .unwrap()
        .into_iter()
        .map(|d| d.id)
        .collect()
}

#[tokio::test]
async fn unshare_withdraws_row_and_persists_across_reboots() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(DocsStore::open(dir.path()).expect("store opens"));

    let host = open_host(store.clone());
    assert!(host.device_shared());
    assert_eq!(device_ids(&host), vec!["dev-me"]);

    host.set_device_shared(false).unwrap();
    assert!(!host.device_shared());
    assert!(device_ids(&host).is_empty());
    drop(host);

    // Reboot: the opt-out is per profile and survives; no row is re-upserted.
    let host = open_host(store.clone());
    assert!(!host.device_shared());
    assert!(device_ids(&host).is_empty());

    // Flip back on: the row returns and the next boot keeps it.
    host.set_device_shared(true).unwrap();
    assert_eq!(device_ids(&host), vec!["dev-me"]);
    drop(host);
    let host = open_host(store);
    assert!(host.device_shared());
    assert_eq!(device_ids(&host), vec!["dev-me"]);
}

#[tokio::test]
async fn unshared_device_refuses_every_host_path() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(DocsStore::open(dir.path()).expect("store opens"));
    let host = open_host(store);

    // Shared: set up a chat we host, then withdraw.
    host.create_space("space-1", "dev-me", "/tmp/repo", None, false)
        .expect("space");
    host.create_chat("chat-own", Some("space-1"), None, None, None)
        .expect("own chat");
    assert!(host.is_host("chat-own"));

    host.set_device_shared(false).unwrap();
    // Even chats whose row still names us must not execute here: an org
    // member can point any chat at our device id.
    assert!(!host.is_host("chat-own"));
    // Edgeless hosts normally claim unknown chats — not in guest mode.
    assert!(!host.is_host("chat-unknown"));
    assert!(host.claim_chat("chat-new", Some("/tmp/repo")).is_err());
    assert!(
        host.create_chat("chat-here", Some("space-1"), None, None, None)
            .is_err(),
        "createChat targeting the unshared device must refuse"
    );
    assert!(
        host.create_space("space-2", "dev-me", "/tmp/other", None, false)
            .is_err(),
        "createSpace for the unshared device must refuse"
    );
    // Foreign-hosted rows are fine — guest mode still participates.
    host.create_chat("chat-remote", None, Some("dev-bob"), None, None)
        .expect("chat hosted elsewhere");
}

#[tokio::test]
async fn delete_device_cascades_but_refuses_self() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(DocsStore::open(dir.path()).expect("store opens"));
    let host = open_host(store);

    assert!(host.delete_device("dev-me").is_err(), "self-delete refused");

    host.create_space("space-b", "dev-bob", "/tmp/bob", None, false)
        .expect("space");
    host.create_chat("chat-b", Some("space-b"), None, None, None)
        .expect("chat");
    let deleted = host.delete_device("dev-bob").unwrap();
    assert_eq!(deleted.chat_ids, vec!["chat-b"]);
    assert!(host.read_spaces().unwrap().is_empty());
    assert!(host.chat("chat-b").unwrap().is_none());
}
