use anyhow::Result;
use proto::jobworkerp_conductor::data::{WorkerExecution, WorkflowExecution};

/// Validate oneof execution_target for Create/Update requests.
///
/// Each proto Data type has its own ExecutionTarget enum (different Rust types due to
/// different oneof field numbers), but the variant structure is identical: Workflow(WorkflowExecution)
/// and Worker(WorkerExecution). This macro generates a type-specific validation function
/// to avoid duplicating the same logic across CronScheduler, SlackEventHandler, and
/// WorkerResultHandler gRPC services.
///
/// Usage in gRPC service modules:
/// ```ignore
/// shared::define_validate_execution_target!(
///     CronSchedulerData,
///     proto::jobworkerp_conductor::data::cron_scheduler_data
/// );
/// ```
#[macro_export]
macro_rules! define_validate_execution_target {
    ($data_type:ty, $et_mod:path) => {
        #[allow(dead_code)]
        fn validate_execution_target(data: &$data_type) -> Result<(), tonic::Status> {
            use $et_mod as et;
            shared::validation::validate_execution_target_impl(
                data.execution_target.as_ref().map(|t| match t {
                    et::ExecutionTarget::Workflow(wf) => {
                        shared::validation::ExecutionTargetRef::Workflow(wf)
                    }
                    et::ExecutionTarget::Worker(w) => {
                        shared::validation::ExecutionTargetRef::Worker(w)
                    }
                }),
                &data.workflow_url,
            )
            .map_err(|msg| tonic::Status::invalid_argument(msg))
        }
    };
}

/// App-layer variant: returns anyhow::Result instead of tonic::Status.
/// Used in App layer (CronSchedulerApp, WorkerResultHandlerApp) where tonic is not available.
///
/// Usage in App layer impl blocks:
/// ```ignore
/// shared::define_validate_execution_target_app!(
///     CronSchedulerData,
///     proto::jobworkerp_conductor::data::cron_scheduler_data
/// );
/// ```
#[macro_export]
macro_rules! define_validate_execution_target_app {
    ($data_type:ty, $et_mod:path) => {
        /// Validate execution_target: either worker_name or workflow_url must be specified.
        /// App layer should be independently safe even without gRPC layer validation.
        #[allow(dead_code)]
        fn validate_execution_target(data: &$data_type) -> anyhow::Result<()> {
            use $et_mod as et;
            shared::validation::validate_execution_target_impl(
                data.execution_target.as_ref().map(|t| match t {
                    et::ExecutionTarget::Workflow(wf) => {
                        shared::validation::ExecutionTargetRef::Workflow(wf)
                    }
                    et::ExecutionTarget::Worker(w) => {
                        shared::validation::ExecutionTargetRef::Worker(w)
                    }
                }),
                &data.workflow_url,
            )
            .map_err(|msg| anyhow::anyhow!(msg))
        }
    };
}

/// Normalized execution target reference for validation
pub enum ExecutionTargetRef<'a> {
    Workflow(&'a WorkflowExecution),
    Worker(&'a WorkerExecution),
}

/// Shared validation logic for execution_target oneof.
/// Returns Ok(()) or Err(error_message).
/// Called via the `define_validate_execution_target!` macro.
pub fn validate_execution_target_impl(
    execution_target: Option<ExecutionTargetRef>,
    deprecated_workflow_url: &str,
) -> Result<(), String> {
    match execution_target {
        Some(ExecutionTargetRef::Workflow(wf)) => {
            if wf.workflow_url.is_empty() {
                return Err("WorkflowExecution.workflow_url must not be empty".to_string());
            }
            if !deprecated_workflow_url.is_empty() {
                tracing::warn!(
                    "Both execution_target and deprecated workflow_url are set; execution_target takes precedence"
                );
            }
            Ok(())
        }
        Some(ExecutionTargetRef::Worker(w)) => {
            if w.worker_name.is_empty() {
                return Err("WorkerExecution.worker_name must not be empty".to_string());
            }
            if !deprecated_workflow_url.is_empty() {
                tracing::warn!(
                    "Both execution_target(worker) and deprecated workflow_url are set; execution_target takes precedence"
                );
            }
            Ok(())
        }
        None => {
            if !deprecated_workflow_url.is_empty() {
                Ok(())
            } else {
                Err("execution_target or workflow_url must be specified".to_string())
            }
        }
    }
}

/// 引数文字列のバリデーション
///
/// argsの内容の正否は処理側が判断するため、空であってもエラーではない。
/// 最大容量チェックのみを実施する。
pub fn validate_args(args: &Option<String>) -> Result<()> {
    const MAX_ARGS_SIZE: usize = 65536; // 64KB

    if let Some(args_str) = args {
        if args_str.len() > MAX_ARGS_SIZE {
            return Err(anyhow::anyhow!(
                "Arguments size ({} bytes) exceeds maximum allowed size ({} bytes)",
                args_str.len(),
                MAX_ARGS_SIZE
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_args_none() {
        let result = validate_args(&None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_empty_string() {
        let result = validate_args(&Some(String::new()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_valid_size() {
        let args = Some("a".repeat(1024));
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_boundary_size() {
        let args = Some("a".repeat(65536)); // Exactly 64KB
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_exceeds_size() {
        let args = Some("a".repeat(65537)); // 64KB + 1
        let result = validate_args(&args);
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg
            .contains("Arguments size (65537 bytes) exceeds maximum allowed size (65536 bytes)"));
    }

    #[test]
    fn test_validate_args_json_format() {
        let args = Some(r#"{"key": "value", "number": 42}"#.to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_plain_text() {
        let args = Some("env=production,batch_size=100".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_utf8_content() {
        let args = Some("日本語テキスト😀".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    // validate_execution_target_impl tests

    #[test]
    fn test_execution_target_workflow_ok() {
        let wf = WorkflowExecution {
            workflow_url: "https://example.com/wf.yml".to_string(),
            channel: None,
        };
        let result = validate_execution_target_impl(Some(ExecutionTargetRef::Workflow(&wf)), "");
        assert!(result.is_ok());
    }

    #[test]
    fn test_execution_target_worker_ok() {
        let w = WorkerExecution {
            worker_name: "my-worker".to_string(),
            r#using: Some("run".to_string()),
        };
        let result = validate_execution_target_impl(Some(ExecutionTargetRef::Worker(&w)), "");
        assert!(result.is_ok());
    }

    #[test]
    fn test_execution_target_workflow_empty_url_error() {
        let wf = WorkflowExecution {
            workflow_url: "".to_string(),
            channel: None,
        };
        let result = validate_execution_target_impl(Some(ExecutionTargetRef::Workflow(&wf)), "");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("workflow_url must not be empty"));
    }

    #[test]
    fn test_execution_target_worker_empty_name_error() {
        let w = WorkerExecution {
            worker_name: "".to_string(),
            r#using: None,
        };
        let result = validate_execution_target_impl(Some(ExecutionTargetRef::Worker(&w)), "");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("worker_name must not be empty"));
    }

    #[test]
    fn test_execution_target_none_with_fallback_ok() {
        let result = validate_execution_target_impl(None, "https://example.com/wf.yml");
        assert!(result.is_ok());
    }

    #[test]
    fn test_execution_target_none_without_fallback_error() {
        let result = validate_execution_target_impl(None, "");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("execution_target or workflow_url must be specified"));
    }
}
