use codex_config::McpServerTransportConfig;
use codex_rmcp_client::is_reserved_mcp_env_var;

pub(crate) fn validate_mcp_transport_environment(
    transport: &McpServerTransportConfig,
) -> Result<(), String> {
    let reserved_name = match transport {
        McpServerTransportConfig::Stdio { env, env_vars, .. } => env
            .as_ref()
            .and_then(|env| env.keys().find(|name| is_reserved_mcp_env_var(name)))
            .map(String::as_str)
            .or_else(|| {
                env_vars
                    .iter()
                    .map(codex_config::McpServerEnvVar::name)
                    .find(|name| is_reserved_mcp_env_var(name))
            }),
        McpServerTransportConfig::StreamableHttp {
            bearer_token_env_var,
            env_http_headers,
            ..
        } => bearer_token_env_var
            .as_deref()
            .filter(|name| is_reserved_mcp_env_var(name))
            .or_else(|| {
                env_http_headers.as_ref().and_then(|headers| {
                    headers
                        .values()
                        .map(String::as_str)
                        .find(|name| is_reserved_mcp_env_var(name))
                })
            }),
    };

    match reserved_name {
        Some(name) => Err(format!(
            "MCP server configuration cannot reference reserved host variable `{name}`"
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "reserved_env_tests.rs"]
mod tests;
