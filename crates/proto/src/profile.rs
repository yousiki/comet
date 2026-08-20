//! Profile lifecycle types shared by the engine and its clients.

use serde::{Deserialize, Serialize};

/// The fixed data boundary selected when an engine runtime is assembled.
///
/// Authentication can change while a runtime is alive, but its profile scope
/// cannot. Switching scopes requires assembling a new runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProfileScope {
    Local,
    Synced,
    Development,
}

/// The immutable account boundary captured when an engine runtime opens its
/// stores. This deliberately differs from the latest authentication state:
/// selecting another Organization may update auth before the old runtime has stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileIdentity {
    pub user_id: String,
    pub organization_id: String,
}

/// Stable information about the engine runtime reached by a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineInfo {
    pub device_id: String,
    pub profile_scope: ProfileScope,
    /// Absent for legacy daemons and a synced runtime that is still waiting for
    /// its first organization. Consumers must fail closed instead of deriving
    /// this identity from mutable auth in either case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<ProfileIdentity>,
}

impl<'de> Deserialize<'de> for EngineInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire {
            device_id: String,
            #[serde(default)]
            profile_scope: Option<ProfileScope>,
            #[serde(default)]
            workspace_scope: Option<ProfileScope>,
            #[serde(default)]
            profile: Option<ProfileIdentity>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let profile_scope = match (wire.profile_scope, wire.workspace_scope) {
            (Some(current), Some(previous)) if current != previous => {
                return Err(serde::de::Error::custom(
                    "profileScope and workspaceScope disagree",
                ));
            }
            (Some(scope), _) | (None, Some(scope)) => scope,
            (None, None) => return Err(serde::de::Error::missing_field("profileScope")),
        };
        Ok(Self {
            device_id: wire.device_id,
            profile_scope,
            profile: wire.profile,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_scope_uses_wire_safe_names() {
        for (scope, encoded) in [
            (ProfileScope::Local, "\"local\""),
            (ProfileScope::Synced, "\"synced\""),
            (ProfileScope::Development, "\"development\""),
        ] {
            assert_eq!(serde_json::to_string(&scope).unwrap(), encoded);
            assert_eq!(
                serde_json::from_str::<ProfileScope>(encoded).unwrap(),
                scope
            );
        }
    }

    #[test]
    fn engine_info_uses_camel_case_fields() {
        let info = EngineInfo {
            device_id: "device-1".into(),
            profile_scope: ProfileScope::Local,
            profile: None,
        };
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            serde_json::json!({
                "deviceId": "device-1",
                "profileScope": "local",
            })
        );
    }

    #[test]
    fn engine_info_profile_is_backward_compatible_and_camel_case() {
        let legacy_without_profile = serde_json::json!({
            "deviceId": "device-1",
            "profileScope": "synced",
        });
        assert_eq!(
            serde_json::from_value::<EngineInfo>(legacy_without_profile)
                .unwrap()
                .profile,
            None
        );

        let previous_version = serde_json::json!({
            "deviceId": "device-1",
            "workspaceScope": "synced",
            "profile": {
                "userId": "user-1",
                "organizationId": "org-1",
            },
        });
        assert_eq!(
            serde_json::from_value::<EngineInfo>(previous_version).unwrap(),
            EngineInfo {
                device_id: "device-1".into(),
                profile_scope: ProfileScope::Synced,
                profile: Some(ProfileIdentity {
                    user_id: "user-1".into(),
                    organization_id: "org-1".into(),
                }),
            }
        );

        let info = EngineInfo {
            device_id: "device-1".into(),
            profile_scope: ProfileScope::Synced,
            profile: Some(ProfileIdentity {
                user_id: "user-1".into(),
                organization_id: "org-1".into(),
            }),
        };
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            serde_json::json!({
                "deviceId": "device-1",
                "profileScope": "synced",
                "profile": {
                    "userId": "user-1",
                    "organizationId": "org-1",
                },
            })
        );
    }
}
