//! Native-driver MCP config conversion.
//!
//! [`McpServerConfig`] is deliberately a stdio-only, host-computed type.  The
//! native agents all accept the same logical shape, but under different outer
//! keys.  Keep the conversion here so env values are always JSON data passed
//! directly to the child process/API -- never shell fragments.

use serde_json::{Map, Value, json};
use zeron_proto::McpServerConfig;

/// Convert host-provided stdio servers to the object-map shape used by Claude
/// (`mcpServers`), Codex (`config.mcp_servers`) and Cursor (`mcpServers`).
///
/// Native APIs key servers and env vars by name, so repeated names use the
/// last value just as insertion into those APIs' maps would.
pub(crate) fn stdio_server_map(servers: &[McpServerConfig]) -> Map<String, Value> {
    let mut result = Map::new();
    for server in servers {
        let env = server
            .env
            .iter()
            .map(|(name, value)| (name.clone(), Value::String(value.clone())))
            .collect::<Map<_, _>>();
        result.insert(
            server.name.clone(),
            json!({
                "command": server.command,
                "args": server.args,
                "env": env,
            }),
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdio_config_keeps_argv_and_env_as_literal_json_data() {
        let map = stdio_server_map(&[McpServerConfig {
            name: "zeron".into(),
            command: "/opt/zeron bin/comet".into(),
            args: vec!["mcp-bridge".into(), "--literal=$HOME".into()],
            env: vec![
                ("ZERON_CHAT_ID".into(), "chat-\"a\"".into()),
                ("EMPTY".into(), String::new()),
            ],
        }]);

        assert_eq!(
            Value::Object(map),
            json!({
                "zeron": {
                    "command": "/opt/zeron bin/comet",
                    "args": ["mcp-bridge", "--literal=$HOME"],
                    "env": {
                        "ZERON_CHAT_ID": "chat-\"a\"",
                        "EMPTY": "",
                    },
                },
            })
        );
    }
}
