//! Registry row shapes shared by the typed [`crate::registry::RegistryDoc`]
//! API: the doc-resident raw rows (epoch-millis timestamps, serde-decoded from
//! row JSON) and the materialized read/delete result types they surface as
//! `zeron_proto` entities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use zeron_proto::{Chat, ChatConfig, Device, Session, SessionStatus, Space};

/// Everything in the registry, materialized (`read_all`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryRows {
    pub devices: Vec<Device>,
    pub spaces: Vec<Space>,
    pub chats: Vec<Chat>,
    pub sessions: Vec<Session>,
}

/// Result of a `delete_space` cascade — the chat ids removed alongside the
/// space so the engine can drop local run state / doc-host handles.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletedSpace {
    pub existed: bool,
    pub chat_ids: Vec<String>,
}

/// Result of a `delete_device` cascade — same contract as [`DeletedSpace`].
#[derive(Debug, Clone, PartialEq)]
pub struct DeletedDevice {
    pub existed: bool,
    pub chat_ids: Vec<String>,
}

fn dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::UNIX_EPOCH)
}

// ── doc-resident row shapes (epoch-millis timestamps) ───────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawDevice {
    id: String,
    name: String,
    platform: String,
    #[serde(default)]
    last_seen_at: Option<i64>,
    #[serde(default)]
    created_at: Option<i64>,
    #[serde(default)]
    version: Option<String>,
}

impl From<RawDevice> for Device {
    fn from(raw: RawDevice) -> Self {
        Device {
            id: raw.id,
            name: raw.name,
            platform: raw.platform,
            last_seen_at: raw.last_seen_at.map(dt),
            created_at: raw.created_at.map(dt),
            version: raw.version,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawSpace {
    id: String,
    device_id: String,
    path: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    git_detected: bool,
    #[serde(default)]
    git_checked_at: Option<i64>,
    #[serde(default)]
    checkout_id: Option<String>,
    #[serde(default)]
    created_at: i64,
}

impl From<RawSpace> for Space {
    fn from(raw: RawSpace) -> Self {
        Space {
            id: raw.id,
            device_id: raw.device_id,
            path: raw.path,
            name: raw.name,
            git_detected: raw.git_detected,
            git_checked_at: raw.git_checked_at.map(dt),
            checkout_id: raw.checkout_id,
            created_at: dt(raw.created_at),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawChat {
    id: String,
    device_id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    checkout_id: Option<String>,
    #[serde(default)]
    config: Option<ChatConfig>,
    #[serde(default)]
    last_message_preview: Option<String>,
    #[serde(default)]
    last_message_at: Option<i64>,
    #[serde(default)]
    created_at: i64,
    #[serde(default)]
    harness_session_id: Option<String>,
    #[serde(default)]
    harness_session_cwd: Option<String>,
    #[serde(default)]
    space_id: Option<String>,
    #[serde(default)]
    last_seen_at: Option<i64>,
    #[serde(default)]
    room_gen: Option<u32>,
    #[serde(default)]
    user_id: Option<String>,
}

impl From<RawChat> for Chat {
    fn from(raw: RawChat) -> Self {
        Chat {
            id: raw.id,
            device_id: raw.device_id,
            title: raw.title,
            archived: raw.archived,
            cwd: raw.cwd,
            branch: raw.branch,
            checkout_id: raw.checkout_id,
            config: raw.config,
            last_message_preview: raw.last_message_preview,
            last_message_at: raw.last_message_at.map(dt),
            created_at: dt(raw.created_at),
            harness_session_id: raw.harness_session_id,
            harness_session_cwd: raw.harness_session_cwd,
            space_id: raw.space_id,
            last_seen_at: raw.last_seen_at.map(dt),
            room_gen: raw.room_gen,
            user_id: raw.user_id,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawSession {
    chat_id: String,
    device_id: String,
    status: SessionStatus,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    updated_at: i64,
}

impl From<RawSession> for Session {
    fn from(raw: RawSession) -> Self {
        Session {
            chat_id: raw.chat_id,
            device_id: raw.device_id,
            status: raw.status,
            started_at: raw.started_at.map(dt),
            updated_at: dt(raw.updated_at),
        }
    }
}
