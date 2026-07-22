use crate::tools::handlers::multi_agents_spec::WaitAgentTimeoutOptions;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

fn string_schema(description: &str) -> JsonSchema {
    JsonSchema::string(Some(description.to_string()))
}

fn integer_schema(description: &str) -> JsonSchema {
    JsonSchema::integer(Some(description.to_string()))
}

fn boolean_schema(description: &str) -> JsonSchema {
    JsonSchema::boolean(Some(description.to_string()))
}

pub fn create_start_work_swarm_tool() -> ToolSpec {
    let task_schema = JsonSchema::object(
        BTreeMap::from([
            ("id".to_string(), string_schema("Stable task identifier.")),
            (
                "task_title".to_string(),
                string_schema("Short human-readable task title containing at most 5 words."),
            ),
            (
                "kind".to_string(),
                JsonSchema::string_enum(
                    vec!["worker".into(), "reducer".into(), "reviewer".into()],
                    Some("Task kind.".to_string()),
                ),
            ),
            (
                "agent_type".to_string(),
                string_schema("Specialist role name for routing."),
            ),
            (
                "instructions".to_string(),
                string_schema("Task instructions for the specialist."),
            ),
            (
                "dependencies".to_string(),
                JsonSchema::array(
                    string_schema("Task identifiers this task depends on."),
                    Some("Dependency task ids.".to_string()),
                ),
            ),
            (
                "cell".to_string(),
                JsonSchema::string(Some("Optional cell or shard label.".to_string())),
            ),
            (
                "explicit_model".to_string(),
                JsonSchema::string(Some(
                    "Optional exact model override for this task.".to_string(),
                )),
            ),
            (
                "explicit_reasoning".to_string(),
                JsonSchema::string(Some(
                    "Optional exact reasoning effort for this task.".to_string(),
                )),
            ),
            (
                "max_attempts".to_string(),
                integer_schema("Maximum task attempts before permanent failure."),
            ),
            (
                "deadline_at".to_string(),
                string_schema("Optional RFC3339 deadline for this task."),
            ),
        ]),
        Some(vec![
            "id".to_string(),
            "task_title".to_string(),
            "kind".to_string(),
            "agent_type".to_string(),
            "instructions".to_string(),
        ]),
        Some(false.into()),
    );

    let policy_schema = JsonSchema::object(
        BTreeMap::from([
            (
                "max_concurrency".to_string(),
                integer_schema("Maximum number of concurrently running tasks."),
            ),
            (
                "token_budget".to_string(),
                integer_schema("Optional total token budget for the run."),
            ),
            (
                "runtime_budget_seconds".to_string(),
                integer_schema("Optional runtime budget in seconds for the run."),
            ),
            (
                "lease_ttl_seconds".to_string(),
                integer_schema("Lease lifetime in seconds for running tasks."),
            ),
            (
                "deadline_at".to_string(),
                string_schema("Optional RFC3339 deadline for the run."),
            ),
            (
                "fail_fast".to_string(),
                boolean_schema("Fail the run immediately on terminal task failure."),
            ),
        ]),
        Some(vec!["max_concurrency".to_string()]),
        Some(false.into()),
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "start_work_swarm".to_string(),
        description:
            "Start a durable Work-only swarm from a typed DAG when multiple narrow specialists can materially improve speed or quality. Keep simple or tightly sequential work solo. Adapt agent_type and instructions to the task domain; use independent worker nodes for parallel evidence or implementation, reducer nodes to merge multiple outputs, and reviewer nodes for an explicit quality gate on broad, ambiguous, or high-stakes work. Dependencies are authoritative. The scheduler validates the DAG, selects economical specialist models, enforces concurrency/budgets/deadlines, recovers leases after crashes, and escalates only objective execution failures. The tool returns immediately with a server-generated run id. Use wait_work_swarm—not wait_agent—when the final pipeline result is required; use get_work_swarm_status only for a non-blocking progress snapshot."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            BTreeMap::from([
                (
                    "title".to_string(),
                    string_schema("Optional human-readable run title."),
                ),
                ("policy".to_string(), policy_schema),
                (
                    "tasks".to_string(),
                JsonSchema::array(task_schema, Some("Typed task DAG.".to_string())),
            ),
            ]),
            Some(vec!["policy".to_string(), "tasks".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

pub fn create_wait_work_swarm_tool(options: WaitAgentTimeoutOptions) -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: "wait_work_swarm".to_string(),
        description: "Wait for an entire Work Swarm DAG to reach a terminal run status. This waits for the scheduler pipeline identified by run_id, including dependent reducers and reviewers; it must be used instead of wait_agent for Work Swarm runs. The wait ends on completed, failed, or cancelled status, when new user input is steered into the active turn, or at the bounded timeout. On timeout, call wait_work_swarm again with the same run_id. Returns the full run and per-task result snapshot."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            BTreeMap::from([
                (
                    "run_id".to_string(),
                    string_schema("Identifier returned by start_work_swarm."),
                ),
                (
                    "timeout_ms".to_string(),
                    JsonSchema::number(Some(format!(
                        "Bounded wait in milliseconds. Defaults to {}, min {}, max {}.",
                        options.default_timeout_ms, options.min_timeout_ms, options.max_timeout_ms,
                    ))),
                ),
            ]),
            Some(vec!["run_id".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

pub fn create_get_work_swarm_status_tool() -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: "get_work_swarm_status".to_string(),
        description:
            "Return a compact projection of a Work Swarm run, including task counts, budgets, terminal status, and current model routing."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            BTreeMap::from([(
                "run_id".to_string(),
                string_schema("Identifier of the run to inspect."),
            )]),
            Some(vec!["run_id".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

pub fn create_cancel_work_swarm_tool() -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: "cancel_work_swarm".to_string(),
        description:
            "Cancel a Work Swarm run. The operation is idempotent and propagates to active worker threads."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            BTreeMap::from([
                (
                    "run_id".to_string(),
                    string_schema("Identifier of the run to cancel."),
                ),
                (
                    "reason".to_string(),
                    string_schema("Optional cancellation reason."),
                ),
            ]),
            Some(vec!["run_id".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

pub fn create_report_work_swarm_result_tool() -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: "report_work_swarm_result".to_string(),
        description:
            "Worker-only tool for reporting the result of the currently executing Work Swarm task. Main agents should not call this."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            BTreeMap::from([
                (
                    "run_id".to_string(),
                    string_schema("Identifier of the run being reported."),
                ),
                (
                    "task_id".to_string(),
                    string_schema("Identifier of the current task."),
                ),
                (
                    "result_token".to_string(),
                    string_schema("Unpredictable result token from the assigned worker packet."),
                ),
                ("result".to_string(), JsonSchema::object(BTreeMap::new(), None, Some(true.into()))),
                (
                    "status".to_string(),
                    JsonSchema::string_enum(
                        vec![
                            "succeeded".into(),
                            "failed".into(),
                            "retryable".into(),
                            "escalated".into(),
                        ],
                        Some("Terminal disposition for this attempt.".to_string()),
                    ),
                ),
                (
                    "error".to_string(),
                    string_schema("Optional failure detail."),
                ),
            ]),
            Some(vec![
                "run_id".to_string(),
                "task_id".to_string(),
                "result_token".to_string(),
                "result".to_string(),
                "status".to_string(),
            ]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_swarm_tool_specs_have_stable_names() {
        assert_eq!(
            match create_start_work_swarm_tool() {
                ToolSpec::Function(tool) => tool.name,
                _ => panic!("expected function tool"),
            },
            "start_work_swarm"
        );
        assert_eq!(
            match create_wait_work_swarm_tool(WaitAgentTimeoutOptions::default()) {
                ToolSpec::Function(tool) => tool.name,
                _ => panic!("expected function tool"),
            },
            "wait_work_swarm"
        );
        assert_eq!(
            match create_get_work_swarm_status_tool() {
                ToolSpec::Function(tool) => tool.name,
                _ => panic!("expected function tool"),
            },
            "get_work_swarm_status"
        );
        assert_eq!(
            match create_cancel_work_swarm_tool() {
                ToolSpec::Function(tool) => tool.name,
                _ => panic!("expected function tool"),
            },
            "cancel_work_swarm"
        );
        assert_eq!(
            match create_report_work_swarm_result_tool() {
                ToolSpec::Function(tool) => tool.name,
                _ => panic!("expected function tool"),
            },
            "report_work_swarm_result"
        );
    }
}
