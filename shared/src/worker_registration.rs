use anyhow::{bail, Context, Result};
use jobworkerp_client::client::JobworkerpClient;
use jobworkerp_client::jobworkerp::data::WorkerId;
use jobworkerp_client::jobworkerp::function::data::{
    function_id::Id as FunctionIdInner, FunctionId, FunctionSetData, FunctionUsing, WorkerOptions,
};
use jobworkerp_client::jobworkerp::function::service::{
    create_worker_request::Runner as RunnerOneof, CreateWorkerRequest, FindByNameRequest,
};

const FUNCTION_SET_NAME: &str = "conductor-admin";
const FUNCTION_SET_DESCRIPTION: &str = "jobworkerp-conductor admin API. Provides CRUD operations for jobworkerp server info, cron schedulers, Slack event handlers, and worker result handlers.";
#[cfg(test)]
const WORKER_NAME_PREFIX: &str = "conductor.";

struct ConductorWorkerDef {
    name: &'static str,
    grpc_method: &'static str,
    description: &'static str,
    #[allow(dead_code)] // used in tests for definition correctness validation
    is_streaming: bool,
}

fn conductor_worker_definitions() -> Vec<ConductorWorkerDef> {
    // Scope: Create, Update, Delete, FindByName, FindList per service.
    // Excluded RPCs:
    //   - Find (by ID): LLMs work with names, not opaque numeric IDs
    //   - Count: FindList results provide counts
    //   - FindListByCondition (SlackEventHandler): FindCondition is empty, same as FindList
    // JobworkerpServerService is read-only because servers are pre-configured infrastructure;
    // CRUD is tracked as a future extension in the spec (§9).
    vec![
        // JobworkerpServerService (read-only: FindByName + FindList only)
        ConductorWorkerDef {
            name: "conductor.jobworkerp-server.find-by-name",
            grpc_method: "jobworkerp_conductor.service.JobworkerpServerService/FindByName",
            description: "Find a jobworkerp server by name",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.jobworkerp-server.find-list",
            grpc_method: "jobworkerp_conductor.service.JobworkerpServerService/FindList",
            description: "List all jobworkerp servers",
            is_streaming: true,
        },
        // CronSchedulerService
        ConductorWorkerDef {
            name: "conductor.cron-scheduler.create",
            grpc_method: "jobworkerp_conductor.service.CronSchedulerService/Create",
            description: "Create a new cron scheduler in jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.cron-scheduler.update",
            grpc_method: "jobworkerp_conductor.service.CronSchedulerService/Update",
            description: "Update an existing cron scheduler in jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.cron-scheduler.delete",
            grpc_method: "jobworkerp_conductor.service.CronSchedulerService/Delete",
            description: "Delete a cron scheduler from jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.cron-scheduler.find-by-name",
            grpc_method: "jobworkerp_conductor.service.CronSchedulerService/FindByName",
            description: "Find a cron scheduler by name",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.cron-scheduler.find-list",
            grpc_method: "jobworkerp_conductor.service.CronSchedulerService/FindList",
            description: "List all cron schedulers",
            is_streaming: true,
        },
        // WorkerResultHandlerService
        ConductorWorkerDef {
            name: "conductor.worker-result-handler.create",
            grpc_method: "jobworkerp_conductor.service.WorkerResultHandlerService/Create",
            description: "Create a new worker result handler in jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.worker-result-handler.update",
            grpc_method: "jobworkerp_conductor.service.WorkerResultHandlerService/Update",
            description: "Update an existing worker result handler in jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.worker-result-handler.delete",
            grpc_method: "jobworkerp_conductor.service.WorkerResultHandlerService/Delete",
            description: "Delete a worker result handler from jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.worker-result-handler.find-by-name",
            grpc_method: "jobworkerp_conductor.service.WorkerResultHandlerService/FindByName",
            description: "Find a worker result handler by name",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.worker-result-handler.find-list",
            grpc_method: "jobworkerp_conductor.service.WorkerResultHandlerService/FindList",
            description: "List all worker result handlers",
            is_streaming: true,
        },
        // SlackEventHandlerService
        ConductorWorkerDef {
            name: "conductor.slack-event-handler.create",
            grpc_method: "jobworkerp_conductor.service.SlackEventHandlerService/Create",
            description: "Create a new Slack event handler in jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.slack-event-handler.update",
            grpc_method: "jobworkerp_conductor.service.SlackEventHandlerService/Update",
            description: "Update an existing Slack event handler in jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.slack-event-handler.delete",
            grpc_method: "jobworkerp_conductor.service.SlackEventHandlerService/Delete",
            description: "Delete a Slack event handler from jobworkerp-conductor",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.slack-event-handler.find-by-name",
            grpc_method: "jobworkerp_conductor.service.SlackEventHandlerService/FindByName",
            description: "Find a Slack event handler by name",
            is_streaming: false,
        },
        ConductorWorkerDef {
            name: "conductor.slack-event-handler.find-list",
            grpc_method: "jobworkerp_conductor.service.SlackEventHandlerService/FindList",
            description: "List all Slack event handlers",
            is_streaming: true,
        },
    ]
}

#[derive(Debug, Clone, PartialEq)]
struct ConductorGrpcConfig {
    host: String,
    port: u32,
    tls: bool,
}

fn parse_conductor_url(url: &str) -> Result<ConductorGrpcConfig> {
    let parsed = url::Url::parse(url).context("Invalid conductor gRPC URL")?;

    let tls = match parsed.scheme() {
        "http" => false,
        "https" => true,
        scheme => bail!("Unsupported scheme: {scheme}. Use http:// or https://"),
    };

    let host = parsed
        .host_str()
        .context("Missing host in URL")?
        .to_string();

    let port = parsed.port().unwrap_or(if tls { 443 } else { 80 }) as u32;

    Ok(ConductorGrpcConfig { host, port, tls })
}

fn build_settings_json(config: &ConductorGrpcConfig, grpc_method: &str) -> String {
    serde_json::json!({
        "host": config.host,
        "port": config.port,
        "tls": config.tls,
        "use_reflection": true,
        "method": grpc_method,
        "as_json": true,
    })
    .to_string()
}

fn build_create_worker_request(
    def: &ConductorWorkerDef,
    config: &ConductorGrpcConfig,
) -> CreateWorkerRequest {
    CreateWorkerRequest {
        runner: Some(RunnerOneof::RunnerName("GRPC".to_string())),
        name: def.name.to_string(),
        description: Some(def.description.to_string()),
        settings_json: Some(build_settings_json(config, def.grpc_method)),
        worker_options: Some(WorkerOptions {
            use_static: true,
            store_failure: true,
            response_type: Some(jobworkerp_client::jobworkerp::data::ResponseType::Direct as i32),
            queue_type: Some(jobworkerp_client::jobworkerp::data::QueueType::Normal as i32),
            ..Default::default()
        }),
    }
}

/// Register conductor admin workers and FunctionSet on the jobworkerp server.
///
/// Uses find-then-create pattern: checks if each worker already exists by name
/// before attempting creation, since CreateWorker returns a DB error on duplicate names.
pub async fn register_conductor_workers(
    jobworkerp_url: &str,
    conductor_grpc_url: &str,
) -> Result<()> {
    let config = parse_conductor_url(conductor_grpc_url)?;
    let client = JobworkerpClient::new(jobworkerp_url.to_string(), None)
        .await
        .context("Failed to connect to jobworkerp server")?;

    println!("Connected to jobworkerp server: {jobworkerp_url}");

    let defs = conductor_worker_definitions();
    let mut registered_worker_ids: Vec<WorkerId> = Vec::with_capacity(defs.len());

    println!("Registering workers...");
    for def in &defs {
        match find_or_create_worker(&client, def, &config).await {
            Ok((worker_id, existed)) => {
                let status = if existed {
                    "exists, settings unchanged — unregister first to update"
                } else {
                    "created"
                };
                println!(
                    "  \u{2713} {} (worker_id={}, {status})",
                    def.name, worker_id.value
                );
                registered_worker_ids.push(worker_id);
            }
            Err(e) => {
                tracing::error!("Failed to register worker {}: {e}", def.name);
                eprintln!("  \u{2717} {} \u{2014} {e}", def.name);
            }
        }
    }

    let total = defs.len();
    let succeeded = registered_worker_ids.len();
    if succeeded == 0 {
        bail!("No workers were registered (0/{total})");
    }
    if succeeded < total {
        eprintln!(
            "Warning: {succeeded}/{total} workers registered ({} failed)",
            total - succeeded
        );
    }

    let targets: Vec<FunctionUsing> = registered_worker_ids
        .iter()
        .map(|worker_id| FunctionUsing {
            function_id: Some(FunctionId {
                id: Some(FunctionIdInner::WorkerId(*worker_id)),
            }),
            using: None,
        })
        .collect();

    let function_set_data = FunctionSetData {
        name: FUNCTION_SET_NAME.to_string(),
        description: FUNCTION_SET_DESCRIPTION.to_string(),
        category: 0,
        targets,
    };

    println!("Registering FunctionSet: {FUNCTION_SET_NAME}");
    let mut fs_client = client.function_set_client().await;
    let existing = fs_client
        .find_by_name(FindByNameRequest {
            name: FUNCTION_SET_NAME.to_string(),
        })
        .await?
        .into_inner()
        .data;

    match existing {
        Some(existing_fs) => {
            let fs_id = existing_fs.id.context("Existing FunctionSet has no id")?;
            fs_client
                .update(jobworkerp_client::jobworkerp::function::data::FunctionSet {
                    id: Some(fs_id),
                    data: Some(function_set_data),
                })
                .await
                .context("Failed to update FunctionSet")?;
            println!(
                "  \u{2713} FunctionSet updated (id={}, {} workers)",
                fs_id.value,
                registered_worker_ids.len()
            );
        }
        None => {
            let resp = fs_client
                .create(function_set_data)
                .await
                .context("Failed to create FunctionSet")?
                .into_inner();
            let fs_id = resp.id.context("No id in CreateFunctionSetResponse")?;
            println!(
                "  \u{2713} FunctionSet registered (id={}, {} workers)",
                fs_id.value,
                registered_worker_ids.len()
            );
        }
    }

    println!("Done.");
    Ok(())
}

/// Find an existing worker by name, or create a new one.
/// Returns (WorkerId, already_existed).
async fn find_or_create_worker(
    client: &JobworkerpClient,
    def: &ConductorWorkerDef,
    config: &ConductorGrpcConfig,
) -> Result<(WorkerId, bool)> {
    let existing = client
        .worker_client()
        .await
        .find_by_name(jobworkerp_client::jobworkerp::service::WorkerNameRequest {
            name: def.name.to_string(),
        })
        .await?
        .into_inner()
        .data;

    if let Some(worker) = existing {
        let worker_id = worker.id.context("Existing worker has no id")?;
        // NOTE: settings are not updated for existing workers.
        // To update settings, unregister and re-register.
        return Ok((worker_id, true));
    }

    let request = build_create_worker_request(def, config);
    let resp = client
        .function_client()
        .await
        .create_worker(request)
        .await
        .with_context(|| format!("Failed to create worker {}", def.name))?
        .into_inner();
    let worker_id = resp
        .worker_id
        .context("No worker_id in CreateWorkerResponse")?;
    Ok((worker_id, false))
}

/// Unregister conductor admin workers and FunctionSet from the jobworkerp server.
pub async fn unregister_conductor_workers(jobworkerp_url: &str) -> Result<()> {
    let client = JobworkerpClient::new(jobworkerp_url.to_string(), None)
        .await
        .context("Failed to connect to jobworkerp server")?;

    println!("Connected to jobworkerp server: {jobworkerp_url}");

    // Delete FunctionSet first (non-fatal: continue to worker deletion on failure)
    match delete_function_set(&client).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Warning: FunctionSet deletion failed: {e}");
            eprintln!("Continuing with worker deletion...");
        }
    }

    // Delete workers
    let defs = conductor_worker_definitions();
    let mut worker_client = client.worker_client().await;
    for def in &defs {
        let resp = worker_client
            .find_by_name(jobworkerp_client::jobworkerp::service::WorkerNameRequest {
                name: def.name.to_string(),
            })
            .await;
        match resp {
            Ok(resp) => {
                if let Some(worker) = resp.into_inner().data {
                    if let Some(id) = worker.id {
                        match worker_client.delete(id).await {
                            Ok(_) => println!("  \u{2713} Deleted {}", def.name),
                            Err(e) => eprintln!("  \u{2717} Failed to delete {}: {e}", def.name),
                        }
                    }
                } else {
                    println!("  - {} not found, skipping", def.name);
                }
            }
            Err(e) => {
                eprintln!("  \u{2717} Failed to find {}: {e}", def.name);
            }
        }
    }

    println!("Done.");
    Ok(())
}

async fn delete_function_set(client: &JobworkerpClient) -> Result<()> {
    let mut fs_client = client.function_set_client().await;
    let existing = fs_client
        .find_by_name(FindByNameRequest {
            name: FUNCTION_SET_NAME.to_string(),
        })
        .await?
        .into_inner()
        .data;

    if let Some(fs) = existing {
        let fs_id = fs.id.context("FunctionSet has no id")?;
        fs_client
            .delete(fs_id)
            .await
            .context("Failed to delete FunctionSet")?;
        println!(
            "Deleted FunctionSet: {FUNCTION_SET_NAME} (id={})",
            fs_id.value
        );
    } else {
        println!("FunctionSet {FUNCTION_SET_NAME} not found, skipping.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_parse_conductor_url_http() {
        let config = parse_conductor_url("http://conductor:9090").unwrap();
        assert_eq!(config.host, "conductor");
        assert_eq!(config.port, 9090);
        assert!(!config.tls);
    }

    #[test]
    fn test_parse_conductor_url_https() {
        let config = parse_conductor_url("https://conductor:9090").unwrap();
        assert_eq!(config.host, "conductor");
        assert_eq!(config.port, 9090);
        assert!(config.tls);
    }

    #[test]
    fn test_parse_conductor_url_default_port_http() {
        let config = parse_conductor_url("http://host").unwrap();
        assert_eq!(config.port, 80);
        assert!(!config.tls);
    }

    #[test]
    fn test_parse_conductor_url_default_port_https() {
        let config = parse_conductor_url("https://host").unwrap();
        assert_eq!(config.port, 443);
        assert!(config.tls);
    }

    #[test]
    fn test_parse_conductor_url_trailing_slash() {
        let config = parse_conductor_url("http://host:9090/").unwrap();
        assert_eq!(config.host, "host");
        assert_eq!(config.port, 9090);
    }

    #[test]
    fn test_parse_conductor_url_invalid_scheme() {
        let result = parse_conductor_url("ftp://host");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported scheme"));
    }

    #[test]
    fn test_parse_conductor_url_empty() {
        assert!(parse_conductor_url("").is_err());
    }

    #[test]
    fn test_conductor_worker_definitions_count() {
        let defs = conductor_worker_definitions();
        assert_eq!(defs.len(), 17);
    }

    #[test]
    fn test_conductor_worker_definitions_unique_names() {
        let defs = conductor_worker_definitions();
        let names: HashSet<&str> = defs.iter().map(|d| d.name).collect();
        assert_eq!(names.len(), defs.len(), "Worker names must be unique");
    }

    #[test]
    fn test_conductor_worker_definitions_method_format() {
        let defs = conductor_worker_definitions();
        for def in &defs {
            assert!(
                def.grpc_method.starts_with("jobworkerp_conductor.service."),
                "Method path must start with 'jobworkerp_conductor.service.': {}",
                def.grpc_method
            );
            assert!(
                def.grpc_method.contains('/'),
                "Method path must contain '/': {}",
                def.grpc_method
            );
        }
    }

    #[test]
    fn test_conductor_worker_definitions_streaming_flags() {
        let defs = conductor_worker_definitions();
        let streaming_count = defs.iter().filter(|d| d.is_streaming).count();
        // 4 FindList RPCs: JobworkerpServer, CronScheduler, WorkerResultHandler, SlackEventHandler
        assert_eq!(streaming_count, 4);

        for def in &defs {
            if def.name.ends_with(".find-list") {
                assert!(def.is_streaming, "{} should be streaming", def.name);
            } else {
                assert!(!def.is_streaming, "{} should not be streaming", def.name);
            }
        }
    }

    #[test]
    fn test_conductor_worker_definitions_name_prefix() {
        let defs = conductor_worker_definitions();
        for def in &defs {
            assert!(
                def.name.starts_with(WORKER_NAME_PREFIX),
                "Worker name must start with '{}': {}",
                WORKER_NAME_PREFIX,
                def.name
            );
        }
    }

    #[test]
    fn test_build_settings_json() {
        let config = ConductorGrpcConfig {
            host: "conductor".to_string(),
            port: 9090,
            tls: false,
        };
        let json_str = build_settings_json(
            &config,
            "jobworkerp_conductor.service.CronSchedulerService/Create",
        );
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["host"], "conductor");
        assert_eq!(parsed["port"], 9090);
        assert_eq!(parsed["tls"], false);
        assert_eq!(parsed["use_reflection"], true);
        assert_eq!(parsed["as_json"], true);
        assert_eq!(
            parsed["method"],
            "jobworkerp_conductor.service.CronSchedulerService/Create"
        );
    }

    #[test]
    fn test_build_settings_json_tls() {
        let config = ConductorGrpcConfig {
            host: "conductor".to_string(),
            port: 443,
            tls: true,
        };
        let json_str = build_settings_json(
            &config,
            "jobworkerp_conductor.service.CronSchedulerService/Create",
        );
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["tls"], true);
    }

    #[test]
    fn test_build_create_worker_request() {
        let config = ConductorGrpcConfig {
            host: "conductor".to_string(),
            port: 9090,
            tls: false,
        };
        let def = &conductor_worker_definitions()[0];
        let req = build_create_worker_request(def, &config);

        assert_eq!(
            req.runner,
            Some(RunnerOneof::RunnerName("GRPC".to_string()))
        );
        assert_eq!(req.name, def.name);
        assert!(req.description.is_some());
        assert!(req.settings_json.is_some());

        let opts = req.worker_options.unwrap();
        assert!(opts.use_static);
        assert!(opts.store_failure);
    }
}
