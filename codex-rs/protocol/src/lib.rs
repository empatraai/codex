pub mod account;
mod agent_path;
pub mod auth;
mod session_id;
mod thread_id;
mod tool_name;
pub use agent_path::AgentPath;
pub use session_id::SessionId;
pub use thread_id::ThreadId;
pub use tool_name::ToolName;
pub mod approvals;
pub mod capabilities;
mod compacted_item;
pub mod config_types;
pub mod dynamic_tools;
pub mod error;
pub mod exec_output;
pub mod items;
pub mod mcp;
pub mod mcp_approval_meta;
pub mod memory_citation;
pub mod models;
pub mod network_policy;
pub mod num_format;
pub mod openai_models;
pub mod parse_command;
pub mod permissions;
pub mod plan_tool;
pub mod protocol;
pub mod request_permissions;
pub mod request_user_input;
pub mod shell_environment;
pub mod user_input;

/// Directory used for project-local Empatra configuration and metadata.
///
/// This is deliberately separate from `CODEX_HOME`, which remains the global
/// runtime data directory, and from `.codex-plugin`, which is the plugin
/// manifest contract.
pub const PROJECT_CONFIG_DIR_NAME: &str = ".empatra";
