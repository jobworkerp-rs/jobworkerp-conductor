//! `conductor-main bootstrap` against a real conductor-main process (and, for
//! the ignored test, a real jobworkerp all-in-one binary).

#[path = "support/conductor_process.rs"]
mod conductor_process;

use std::time::{Duration, Instant};

use conductor_main::bootstrap::{
    connect_jobworkerp_server_service, reconcile_jobworkerp_server, run_bootstrap,
    server_data_from_endpoint, BootstrapConfig, ServerOutcome,
};
use conductor_process::{
    all_in_one_binary, find_server_by_name, free_port, server_data, ConductorProcess,
    JobworkerpProcess, CONNECT_TIMEOUT,
};
use grpc_admin::proto::jobworkerp_conductor::data::{
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
};
use grpc_admin::proto::jobworkerp_conductor::service::{
    jobworkerp_server_service_client::JobworkerpServerServiceClient, FindCondition,
};
use jobworkerp_client::client::JobworkerpClient;
use jobworkerp_client::jobworkerp::data::ResultStatus;
use jobworkerp_client::jobworkerp::function::data::FunctionSet;
use jobworkerp_client::jobworkerp::function::service::{
    function_call_request, FindByNameRequest, FindFunctionSetRequest, FunctionCallRequest,
};
use shared::grpc_endpoint::parse_grpc_endpoint;
use shared::worker_registration::{conductor_worker_count, FUNCTION_SET_NAME};
use tonic::transport::Channel;

/// Short, since nothing listens in the timeout tests and refusals are immediate.
const SHORT_WAIT: Duration = Duration::from_millis(300);

fn desired(name: &str, url: &str) -> JobworkerpServerData {
    server_data_from_endpoint(name, &parse_grpc_endpoint(url).expect("endpoint"))
}

async fn stored(
    client: &mut JobworkerpServerServiceClient<Channel>,
    name: &str,
) -> JobworkerpServerData {
    find_server_by_name(client, name)
        .await
        .and_then(|server| server.data)
        .expect("server exists")
}

async fn count(client: &mut JobworkerpServerServiceClient<Channel>) -> i64 {
    client
        .count(FindCondition {})
        .await
        .expect("count")
        .into_inner()
        .total
}

fn config(conductor: &ConductorProcess, jobworkerp_url: String) -> BootstrapConfig {
    BootstrapConfig {
        jobworkerp_url,
        conductor_grpc_url: conductor.url(),
        server_name: "local".to_string(),
        wait_timeout: CONNECT_TIMEOUT,
    }
}

#[tokio::test]
async fn reconcile_creates_then_reports_existing_server() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let local = desired("local", "http://127.0.0.1:9000");

    let first = reconcile_jobworkerp_server(&mut client, &local)
        .await
        .expect("first reconcile")
        .outcome;
    let second = reconcile_jobworkerp_server(&mut client, &local)
        .await
        .expect("second reconcile")
        .outcome;

    let ServerOutcome::Created(id) = first else {
        panic!("first reconcile must create, got {first:?}");
    };
    assert_eq!(second, ServerOutcome::Exists(id));
    assert_eq!(count(&mut client).await, 1);
    let stored = stored(&mut client, "local").await;
    assert_eq!(
        (
            stored.host.as_str(),
            stored.port.as_str(),
            stored.ssl_enabled
        ),
        ("127.0.0.1", "9000", false)
    );
    assert!(stored.enabled);
}

#[tokio::test]
async fn reconcile_updates_a_managed_server_whose_endpoint_changed() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let old = desired("local", "http://127.0.0.1:9000");
    let new = desired("local", "http://127.0.0.1:9100");

    let ServerOutcome::Created(id) = reconcile_jobworkerp_server(&mut client, &old)
        .await
        .expect("create")
        .outcome
    else {
        panic!("first reconcile must create");
    };
    // Looks the record up by name once more so a stale name cache would show.
    reconcile_jobworkerp_server(&mut client, &old)
        .await
        .expect("exists");

    let updated = reconcile_jobworkerp_server(&mut client, &new)
        .await
        .expect("update")
        .outcome;
    let again = reconcile_jobworkerp_server(&mut client, &new)
        .await
        .expect("after update")
        .outcome;

    assert_eq!(updated, ServerOutcome::Updated(id));
    assert_eq!(stored(&mut client, "local").await.port, "9100");
    assert_eq!(again, ServerOutcome::Exists(id));
    assert_eq!(count(&mut client).await, 1);
}

#[tokio::test]
async fn reconcile_reports_a_disabled_managed_server_without_enabling_it() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let local = desired("local", "http://127.0.0.1:9000");
    let ServerOutcome::Created(id) = reconcile_jobworkerp_server(&mut client, &local)
        .await
        .expect("create")
        .outcome
    else {
        panic!("first reconcile must create");
    };
    let disabled = JobworkerpServerData {
        enabled: false,
        ..stored(&mut client, "local").await
    };
    client
        .update(JobworkerpServer {
            id: Some(JobworkerpServerId { value: id }),
            data: Some(disabled),
        })
        .await
        .expect("disable");

    let reconciled = reconcile_jobworkerp_server(&mut client, &local)
        .await
        .expect("reconcile");

    assert_eq!(reconciled.outcome, ServerOutcome::Exists(id));
    assert!(!reconciled.enabled);
    assert!(!stored(&mut client, "local").await.enabled);
}

#[tokio::test]
async fn reconcile_leaves_a_user_created_server_unchanged() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let user_created = JobworkerpServerData {
        description: Some("registered by hand".to_string()),
        ..server_data("local", "9000")
    };
    let id = client
        .create(user_created.clone())
        .await
        .expect("create by hand")
        .into_inner()
        .id
        .expect("id");

    let outcome =
        reconcile_jobworkerp_server(&mut client, &desired("local", "http://127.0.0.1:9100"))
            .await
            .expect("reconcile")
            .outcome;

    assert!(
        matches!(&outcome, ServerOutcome::Unmanaged { id: got, current } if *got == id.value && current.port == "9000"),
        "got {outcome:?}"
    );
    let stored = stored(&mut client, "local").await;
    assert_eq!(stored.port, user_created.port);
    assert_eq!(stored.description, user_created.description);
}

#[tokio::test]
async fn connect_gives_up_after_the_timeout() {
    let url = format!("http://127.0.0.1:{}", free_port());
    let started = Instant::now();

    let result = connect_jobworkerp_server_service(&url, SHORT_WAIT).await;

    assert!(result.is_err(), "nothing listens on {url}");
    let elapsed = started.elapsed();
    assert!(elapsed >= SHORT_WAIT, "gave up early: {elapsed:?}");
    assert!(
        elapsed < CONNECT_TIMEOUT,
        "kept retrying past the timeout: {elapsed:?}"
    );
}

#[tokio::test]
async fn run_bootstrap_fails_when_jobworkerp_is_unreachable() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let config = BootstrapConfig {
        wait_timeout: SHORT_WAIT,
        ..config(&conductor, format!("http://127.0.0.1:{}", free_port()))
    };

    let error = run_bootstrap(&config)
        .await
        .expect_err("jobworkerp is not running");

    assert!(
        format!("{error:#}").contains("jobworkerp"),
        "error must name jobworkerp: {error:#}"
    );
    // Nothing is registered for a server that never answered.
    assert_eq!(count(&mut client).await, 0);
}

#[tokio::test]
async fn run_bootstrap_rejects_an_empty_server_name() {
    let config = BootstrapConfig {
        jobworkerp_url: "http://127.0.0.1:9000".to_string(),
        conductor_grpc_url: "http://127.0.0.1:9090".to_string(),
        server_name: " ".to_string(),
        wait_timeout: SHORT_WAIT,
    };

    let error = run_bootstrap(&config).await.expect_err("empty name");

    assert!(format!("{error:#}").contains("server name"), "{error:#}");
}

async fn jobworkerp_client(url: &str) -> JobworkerpClient {
    JobworkerpClient::new(url.to_string(), None)
        .await
        .expect("connect to jobworkerp")
}

async fn admin_function_set(client: &JobworkerpClient) -> FunctionSet {
    client
        .function_set_client()
        .await
        .find_by_name(FindByNameRequest {
            name: FUNCTION_SET_NAME.to_string(),
        })
        .await
        .expect("find FunctionSet")
        .into_inner()
        .data
        .expect("FunctionSet registered")
}

fn assert_targets_have_using(function_set: &FunctionSet) {
    let targets = &function_set.data.as_ref().expect("data").targets;
    assert_eq!(targets.len(), conductor_worker_count());
    assert!(
        targets.iter().all(|t| t.using.is_some()),
        "targets without using: {targets:?}"
    );
}

/// Calls a GRPC worker through FunctionService.Call, the path LLM and MCP
/// tool calls take, and returns the RPC's JSON response.
async fn call_worker(
    client: &JobworkerpClient,
    tool_name: &str,
    request: serde_json::Value,
) -> serde_json::Value {
    let args_json = serde_json::json!({ "jsonBody": request.to_string() }).to_string();
    let mut stream = client
        .function_client()
        .await
        .call(FunctionCallRequest {
            name: Some(function_call_request::Name::WorkerName(
                tool_name.to_string(),
            )),
            args_json,
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| panic!("call {tool_name}: {e}"))
        .into_inner();
    let mut output = String::new();
    while let Some(result) = stream
        .message()
        .await
        .unwrap_or_else(|e| panic!("call {tool_name}: {e}"))
    {
        assert!(
            result.error_message.is_none() && result.status() == ResultStatus::Success,
            "call {tool_name} failed: {result:?}"
        );
        output.push_str(&result.output);
    }
    // The runner reports an RPC error inside a successful result, as an
    // object without `jsonBody`.
    let output: serde_json::Value = serde_json::from_str(&output)
        .unwrap_or_else(|e| panic!("call {tool_name}: output is not JSON ({e}): {output}"));
    let body = output["jsonBody"]
        .as_str()
        .unwrap_or_else(|| panic!("call {tool_name}: no RPC response in {output}"));
    serde_json::from_str(body).expect("RPC response is JSON")
}

#[tokio::test]
#[ignore = "needs a jobworkerp all-in-one binary (TEST_JOBWORKERP_ALL_IN_ONE_BIN)"]
async fn run_bootstrap_registers_server_and_workers_idempotently() {
    let jobworkerp = JobworkerpProcess::spawn(&all_in_one_binary());
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let config = config(&conductor, jobworkerp.url().to_string());

    run_bootstrap(&config).await.expect("first bootstrap");
    run_bootstrap(&config).await.expect("second bootstrap");

    assert_eq!(count(&mut client).await, 1);
    assert_targets_have_using(
        &admin_function_set(&jobworkerp_client(jobworkerp.url()).await).await,
    );
}

#[tokio::test]
#[ignore = "needs a jobworkerp all-in-one binary (TEST_JOBWORKERP_ALL_IN_ONE_BIN)"]
async fn run_bootstrap_exposes_each_worker_with_the_method_of_its_rpc() {
    let jobworkerp = JobworkerpProcess::spawn(&all_in_one_binary());
    let mut conductor = ConductorProcess::spawn();
    run_bootstrap(&config(&conductor, jobworkerp.url().to_string()))
        .await
        .expect("bootstrap");
    let client = jobworkerp_client(jobworkerp.url()).await;

    // The function specs LLM and MCP tools are built from: one method per
    // worker, matching whether its RPC is server streaming.
    let mut specs = client
        .function_client()
        .await
        .find_list_by_set(FindFunctionSetRequest {
            name: FUNCTION_SET_NAME.to_string(),
        })
        .await
        .expect("find_list_by_set")
        .into_inner();
    let mut seen = 0;
    while let Some(spec) = specs.message().await.expect("function specs") {
        let methods: Vec<&str> = spec
            .methods
            .as_ref()
            .map(|m| m.schemas.keys().map(String::as_str).collect())
            .unwrap_or_default();
        let expected = if spec.name.ends_with("_find-list") {
            "streaming"
        } else {
            "unary"
        };
        assert_eq!(methods, [expected], "{}", spec.name);
        seen += 1;
    }
    assert_eq!(seen, conductor_worker_count());

    let found = call_worker(
        &client,
        "conductor_jobworkerp-server_find-by-name___unary",
        serde_json::json!({ "name": "local" }),
    )
    .await;
    assert_eq!(found["data"]["data"]["name"], "local", "{found}");

    // Two records, since a unary call to FindList returns only the first.
    conductor
        .admin_client()
        .await
        .create(server_data("second", "1"))
        .await
        .expect("create a second server");
    let listed = call_worker(
        &client,
        "conductor_jobworkerp-server_find-list___streaming",
        serde_json::json!({}),
    )
    .await;
    let mut names: Vec<&str> = listed
        .as_array()
        .unwrap_or_else(|| panic!("find-list response: {listed}"))
        .iter()
        .filter_map(|server| server["data"]["name"].as_str())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["local", "second"], "{listed}");
}

#[tokio::test]
#[ignore = "needs a jobworkerp all-in-one binary (TEST_JOBWORKERP_ALL_IN_ONE_BIN)"]
async fn run_bootstrap_replaces_function_set_targets_registered_without_using() {
    let jobworkerp = JobworkerpProcess::spawn(&all_in_one_binary());
    let conductor = ConductorProcess::spawn();
    let config = config(&conductor, jobworkerp.url().to_string());
    run_bootstrap(&config).await.expect("first bootstrap");
    let client = jobworkerp_client(jobworkerp.url()).await;

    // Reproduce a FunctionSet registered by an earlier version.
    let mut legacy = admin_function_set(&client).await;
    for target in &mut legacy.data.as_mut().expect("data").targets {
        target.using = None;
    }
    client
        .function_set_client()
        .await
        .update(legacy)
        .await
        .expect("update FunctionSet");

    run_bootstrap(&config).await.expect("second bootstrap");

    assert_targets_have_using(&admin_function_set(&client).await);
}
