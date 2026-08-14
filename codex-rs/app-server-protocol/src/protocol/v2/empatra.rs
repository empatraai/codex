use super::AskForApproval;
use super::SandboxPolicy;
use super::ThreadForkParams;
use super::ThreadStartParams;
use super::UserInput;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::openai_models::ReasoningEffort;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;
use ts_rs::TS;

/// The bounded initial-turn surface used by Empatra's atomic thread creation RPCs.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct EmpatraInitialTurnParams {
    #[ts(optional = nullable)]
    pub client_user_message_id: Option<String>,
    pub input: Vec<UserInput>,
    #[ts(optional = nullable)]
    pub cwd: Option<PathBuf>,
    #[ts(optional = nullable)]
    pub approval_policy: Option<AskForApproval>,
    #[ts(optional = nullable)]
    pub sandbox_policy: Option<SandboxPolicy>,
    #[ts(optional = nullable)]
    pub permissions: Option<String>,
    #[ts(optional = nullable)]
    pub model: Option<String>,
    #[ts(optional = nullable)]
    pub effort: Option<ReasoningEffort>,
    #[ts(optional = nullable)]
    pub collaboration_mode: Option<CollaborationMode>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct EmpatraThreadCreateAndStartParams {
    pub operation_id: String,
    pub issued_at_ms: i64,
    pub thread: ThreadStartParams,
    pub turn: EmpatraInitialTurnParams,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct EmpatraThreadForkAndStartParams {
    pub operation_id: String,
    pub issued_at_ms: i64,
    pub thread: ThreadForkParams,
    pub turn: EmpatraInitialTurnParams,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct EmpatraThreadCreateAndStartResponse {
    pub operation_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct EmpatraThreadForkAndStartResponse {
    pub operation_id: String,
    pub thread_id: String,
    pub turn_id: String,
}
