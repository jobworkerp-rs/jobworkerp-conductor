//! `conductor-main bootstrap`: idempotent setup for running conductor next to a
//! jobworkerp server, so a fresh deployment needs no manual API calls.
//!
//! Settings are written through the admin gRPC API rather than the database,
//! so a running conductor sees them through its own cache and notifications.

use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use grpc_admin::proto::jobworkerp_conductor::data::{JobworkerpServer, JobworkerpServerData};
use grpc_admin::proto::jobworkerp_conductor::service::jobworkerp_server_service_client::JobworkerpServerServiceClient;
use grpc_admin::proto::jobworkerp_conductor::service::FindByNameRequest;
use jobworkerp_client::client::JobworkerpClient;
use shared::grpc_endpoint::{parse_grpc_endpoint, GrpcEndpoint};
use shared::worker_registration::register_conductor_workers;
use tonic::transport::Channel;

/// Description of the JobworkerpServer records bootstrap owns; only these are
/// updated when the endpoint changes, so hand-made records are never touched.
const MANAGED_DESCRIPTION: &str = "managed-by: conductor-main bootstrap";

const RETRY_INTERVAL: Duration = Duration::from_millis(200);
/// Least time one attempt gets, so a zero wait timeout still tries once.
const ATTEMPT_FLOOR: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub jobworkerp_url: String,
    pub conductor_grpc_url: String,
    pub server_name: String,
    /// How long to wait for each of conductor and jobworkerp to accept connections.
    pub wait_timeout: Duration,
}

/// What reconciling the JobworkerpServer record did; carries the record id.
#[derive(Debug, Clone, PartialEq)]
pub enum ServerOutcome {
    Created(i64),
    Updated(i64),
    /// The record already points at the desired endpoint.
    Exists(i64),
    /// A record not created by bootstrap points elsewhere and is left as it is.
    Unmanaged {
        id: i64,
        current: JobworkerpServerData,
    },
}

impl ServerOutcome {
    /// The result line; the all-in-one image checks it, so keep the format stable.
    fn report_line(&self, server_name: &str) -> String {
        let (label, id) = match self {
            Self::Created(id) => ("created", id),
            Self::Updated(id) => ("updated", id),
            Self::Exists(id) | Self::Unmanaged { id, .. } => ("exists", id),
        };
        format!("JobworkerpServer {server_name}: {label} (id={id})")
    }
}

/// The result of reconciling, with whether the record is enabled: bootstrap
/// never enables a record an operator disabled, but has to report it.
#[derive(Debug, Clone, PartialEq)]
pub struct Reconciled {
    pub outcome: ServerOutcome,
    pub enabled: bool,
}

/// The JobworkerpServer record bootstrap wants for `endpoint`.
pub fn server_data_from_endpoint(name: &str, endpoint: &GrpcEndpoint) -> JobworkerpServerData {
    let now = chrono::Utc::now().timestamp();
    JobworkerpServerData {
        name: name.to_string(),
        host: endpoint.host.clone(),
        port: endpoint.port.to_string(),
        ssl_enabled: endpoint.tls,
        description: Some(MANAGED_DESCRIPTION.to_string()),
        enabled: true,
        created_at: now,
        updated_at: now,
    }
}

fn same_endpoint(a: &JobworkerpServerData, b: &JobworkerpServerData) -> bool {
    a.host == b.host && a.port == b.port && a.ssl_enabled == b.ssl_enabled
}

fn is_managed(data: &JobworkerpServerData) -> bool {
    data.description.as_deref() == Some(MANAGED_DESCRIPTION)
}

/// Creates the record named `desired.name`, points a bootstrap-managed record
/// at the desired endpoint, and leaves a hand-made record as it is.
pub async fn reconcile_jobworkerp_server(
    client: &mut JobworkerpServerServiceClient<Channel>,
    desired: &JobworkerpServerData,
) -> Result<Reconciled> {
    let existing = client
        .find_by_name(FindByNameRequest {
            name: desired.name.clone(),
        })
        .await
        .context("Failed to look up the JobworkerpServer")?
        .into_inner()
        .data;

    let Some(existing) = existing else {
        let id = client
            .create(desired.clone())
            .await
            .context("Failed to create the JobworkerpServer")?
            .into_inner()
            .id
            .context("No id in CreateJobworkerpServerResponse")?;
        return Ok(Reconciled {
            outcome: ServerOutcome::Created(id.value),
            enabled: desired.enabled,
        });
    };

    let JobworkerpServer {
        id: Some(id),
        data: Some(current),
    } = existing
    else {
        bail!("JobworkerpServer {} has no id or data", desired.name);
    };

    let enabled = current.enabled;
    let outcome = if same_endpoint(&current, desired) {
        ServerOutcome::Exists(id.value)
    } else if !is_managed(&current) {
        ServerOutcome::Unmanaged {
            id: id.value,
            current,
        }
    } else {
        let updated = JobworkerpServerData {
            host: desired.host.clone(),
            port: desired.port.clone(),
            ssl_enabled: desired.ssl_enabled,
            updated_at: desired.updated_at,
            ..current
        };
        client
            .update(JobworkerpServer {
                id: Some(id),
                data: Some(updated),
            })
            .await
            .context("Failed to update the JobworkerpServer")?;
        ServerOutcome::Updated(id.value)
    };
    Ok(Reconciled { outcome, enabled })
}

/// Retries `attempt` until it succeeds or `timeout` has passed. Each attempt is
/// cut off when the time is up, because a connect to an address that drops
/// packets would otherwise block until the OS gives up; the wait can overrun
/// `timeout` by up to [`ATTEMPT_FLOOR`].
async fn retry_until<T, F, Fut>(what: &str, timeout: Duration, mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        let budget = deadline
            .saturating_duration_since(Instant::now())
            .max(ATTEMPT_FLOOR);
        let result = tokio::time::timeout(budget, attempt())
            .await
            .unwrap_or_else(|_| Err(anyhow!("the attempt did not finish within {budget:?}")));
        match result {
            Ok(value) => return Ok(value),
            Err(error) if Instant::now() >= deadline => {
                return Err(error.context(format!(
                    "{what} did not accept connections within {timeout:?}"
                )));
            }
            Err(_) => tokio::time::sleep(RETRY_INTERVAL).await,
        }
    }
}

/// Connects to the JobworkerpServer service of the conductor admin API,
/// retrying until `timeout`.
pub async fn connect_jobworkerp_server_service(
    url: &str,
    timeout: Duration,
) -> Result<JobworkerpServerServiceClient<Channel>> {
    retry_until("conductor", timeout, || async {
        JobworkerpServerServiceClient::connect(url.to_string())
            .await
            .with_context(|| format!("Failed to connect to conductor at {url}"))
    })
    .await
}

async fn wait_for_jobworkerp(url: &str, timeout: Duration) -> Result<()> {
    retry_until("jobworkerp", timeout, || async {
        JobworkerpClient::new(url.to_string(), None)
            .await
            .map(|_| ())
            .with_context(|| format!("Failed to connect to jobworkerp at {url}"))
    })
    .await
}

/// Waits for both servers, reconciles the JobworkerpServer record, then
/// registers the conductor admin workers; any worker that fails to register
/// fails the whole bootstrap.
pub async fn run_bootstrap(config: &BootstrapConfig) -> Result<()> {
    let server_name = config.server_name.trim();
    if server_name.is_empty() {
        bail!("The JobworkerpServer server name must not be empty");
    }
    let jobworkerp =
        parse_grpc_endpoint(&config.jobworkerp_url).context("Invalid jobworkerp URL")?;
    parse_grpc_endpoint(&config.conductor_grpc_url).context("Invalid conductor gRPC URL")?;

    let mut conductor =
        connect_jobworkerp_server_service(&config.conductor_grpc_url, config.wait_timeout).await?;
    wait_for_jobworkerp(&config.jobworkerp_url, config.wait_timeout).await?;

    let desired = server_data_from_endpoint(server_name, &jobworkerp);
    let Reconciled { outcome, enabled } =
        reconcile_jobworkerp_server(&mut conductor, &desired).await?;
    if let ServerOutcome::Unmanaged { current, .. } = &outcome {
        eprintln!(
            "Warning: JobworkerpServer {server_name} points to {}:{} (ssl={}), not {}:{} (ssl={}); \
             it was not created by bootstrap, so it is left unchanged",
            current.host,
            current.port,
            current.ssl_enabled,
            desired.host,
            desired.port,
            desired.ssl_enabled
        );
    }
    if !enabled {
        eprintln!(
            "Warning: JobworkerpServer {server_name} is disabled; bootstrap leaves it disabled, \
             so nothing runs on it until it is enabled"
        );
    }
    println!("{}", outcome.report_line(server_name));

    let summary =
        register_conductor_workers(&config.jobworkerp_url, &config.conductor_grpc_url).await?;
    if !summary.is_complete() {
        bail!(
            "Only {}/{} conductor workers were registered",
            summary.registered,
            summary.total
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(host: &str, port: u32, tls: bool) -> GrpcEndpoint {
        GrpcEndpoint {
            host: host.to_string(),
            port,
            tls,
        }
    }

    #[test]
    fn server_data_is_an_enabled_managed_record_for_the_endpoint() {
        let data = server_data_from_endpoint("local", &endpoint("127.0.0.1", 9000, false));

        assert_eq!(data.name, "local");
        assert_eq!(data.host, "127.0.0.1");
        assert_eq!(data.port, "9000");
        assert!(!data.ssl_enabled);
        assert!(data.enabled);
        assert!(is_managed(&data));
    }

    #[test]
    fn server_data_carries_tls() {
        let data = server_data_from_endpoint("remote", &endpoint("example", 443, true));

        assert!(data.ssl_enabled);
        assert_eq!(data.port, "443");
    }

    #[test]
    fn same_endpoint_ignores_everything_but_host_port_and_tls() {
        let a = server_data_from_endpoint("a", &endpoint("h", 1, false));
        let b = JobworkerpServerData {
            name: "b".to_string(),
            description: None,
            enabled: false,
            ..a.clone()
        };

        assert!(same_endpoint(&a, &b));
        assert!(!same_endpoint(
            &a,
            &server_data_from_endpoint("a", &endpoint("h", 2, false))
        ));
        assert!(!same_endpoint(
            &a,
            &server_data_from_endpoint("a", &endpoint("h", 1, true))
        ));
    }

    #[test]
    fn report_lines_have_a_stable_format() {
        assert_eq!(
            ServerOutcome::Created(1).report_line("local"),
            "JobworkerpServer local: created (id=1)"
        );
        assert_eq!(
            ServerOutcome::Updated(2).report_line("local"),
            "JobworkerpServer local: updated (id=2)"
        );
        assert_eq!(
            ServerOutcome::Exists(3).report_line("local"),
            "JobworkerpServer local: exists (id=3)"
        );
        let unmanaged = ServerOutcome::Unmanaged {
            id: 4,
            current: server_data_from_endpoint("local", &endpoint("h", 1, false)),
        };
        assert_eq!(
            unmanaged.report_line("local"),
            "JobworkerpServer local: exists (id=4)"
        );
    }

    #[tokio::test]
    async fn retry_until_tries_once_with_a_zero_timeout() {
        let mut attempts = 0;
        let result: Result<()> = retry_until("x", Duration::ZERO, || {
            attempts += 1;
            async { bail!("down") }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn retry_until_cuts_off_an_attempt_that_never_finishes() {
        let started = Instant::now();

        let result: Result<()> = retry_until("x", Duration::ZERO, std::future::pending).await;

        assert!(result.is_err());
        let elapsed = started.elapsed();
        assert!(elapsed < ATTEMPT_FLOOR * 2, "waited too long: {elapsed:?}");
    }

    #[tokio::test]
    async fn retry_until_returns_the_first_success() {
        let mut attempts = 0;
        let result = retry_until("x", Duration::from_secs(5), || {
            attempts += 1;
            let ok = attempts >= 3;
            async move {
                if ok {
                    Ok(attempts)
                } else {
                    bail!("not yet")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 3);
    }
}
