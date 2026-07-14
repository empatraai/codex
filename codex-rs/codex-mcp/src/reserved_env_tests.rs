use std::collections::HashMap;

use codex_config::McpServerTransportConfig;
use pretty_assertions::assert_eq;

use super::validate_mcp_transport_environment;

#[test]
fn rejects_reserved_host_capability_references_across_mcp_transports() {
    let reserved = "empatra_model_gateway_token";
    let transports = [
        McpServerTransportConfig::Stdio {
            command: "demo".to_string(),
            args: Vec::new(),
            env: Some(HashMap::from([(
                reserved.to_string(),
                "literal".to_string(),
            )])),
            env_vars: Vec::new(),
            cwd: None,
        },
        McpServerTransportConfig::Stdio {
            command: "demo".to_string(),
            args: Vec::new(),
            env: None,
            env_vars: vec![reserved.into()],
            cwd: None,
        },
        McpServerTransportConfig::StreamableHttp {
            url: "https://mcp.example".to_string(),
            bearer_token_env_var: Some(reserved.to_string()),
            http_headers: None,
            env_http_headers: None,
        },
        McpServerTransportConfig::StreamableHttp {
            url: "https://mcp.example".to_string(),
            bearer_token_env_var: None,
            http_headers: None,
            env_http_headers: Some(HashMap::from([(
                "Authorization".to_string(),
                reserved.to_string(),
            )])),
        },
    ];
    let results = transports
        .iter()
        .map(validate_mcp_transport_environment)
        .collect::<Vec<_>>();

    assert_eq!(
        results,
        vec![
            Err("MCP server configuration cannot reference reserved host variable `empatra_model_gateway_token`".to_string());
            4
        ]
    );
}
