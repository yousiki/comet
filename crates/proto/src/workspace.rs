//! Workspace lifecycle types shared by the engine and its clients.

use serde::{Deserialize, Serialize};

/// The fixed data boundary selected when an engine runtime is assembled.
///
/// Authentication can change while a runtime is alive, but its workspace scope
/// cannot. Switching scopes requires assembling a new runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceScope {
    Local,
    Synced,
    Development,
}

/// The immutable account boundary captured when an engine runtime opens its
/// stores. This deliberately differs from the latest authentication state:
/// selecting another Team may update auth before the old runtime has stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineProfileIdentity {
    pub user_id: String,
    pub organization_id: String,
}

/// Stable information about the engine runtime reached by a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineInfo {
    pub device_id: String,
    pub workspace_scope: WorkspaceScope,
    /// Absent for legacy daemons and a synced runtime that is still waiting for
    /// its first organization. Consumers must fail closed instead of deriving
    /// this identity from mutable auth in either case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<EngineProfileIdentity>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_scope_uses_wire_safe_names() {
        for (scope, encoded) in [
            (WorkspaceScope::Local, "\"local\""),
            (WorkspaceScope::Synced, "\"synced\""),
            (WorkspaceScope::Development, "\"development\""),
        ] {
            assert_eq!(serde_json::to_string(&scope).unwrap(), encoded);
            assert_eq!(
                serde_json::from_str::<WorkspaceScope>(encoded).unwrap(),
                scope
            );
        }
    }

    #[test]
    fn engine_info_uses_camel_case_fields() {
        let info = EngineInfo {
            device_id: "device-1".into(),
            workspace_scope: WorkspaceScope::Local,
            profile: None,
        };
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            serde_json::json!({
                "deviceId": "device-1",
                "workspaceScope": "local",
            })
        );
    }

    #[test]
    fn engine_info_profile_is_backward_compatible_and_camel_case() {
        let legacy = serde_json::json!({
            "deviceId": "device-1",
            "workspaceScope": "synced",
        });
        assert_eq!(
            serde_json::from_value::<EngineInfo>(legacy)
                .unwrap()
                .profile,
            None
        );

        let info = EngineInfo {
            device_id: "device-1".into(),
            workspace_scope: WorkspaceScope::Synced,
            profile: Some(EngineProfileIdentity {
                user_id: "user-1".into(),
                organization_id: "org-1".into(),
            }),
        };
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            serde_json::json!({
                "deviceId": "device-1",
                "workspaceScope": "synced",
                "profile": {
                    "userId": "user-1",
                    "organizationId": "org-1",
                },
            })
        );
    }
}
