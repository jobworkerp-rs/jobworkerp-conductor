//! Runs the built conductor-main (and, for some tests, a jobworkerp all-in-one
//! binary) as child processes with their own SQLite file and port. Integration
//! tests exercise the real binaries this way, and each test gets a fresh
//! database even though the RDB pool is process-wide.
#![allow(dead_code)]

use std::fs::File;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use conductor_main::bootstrap::connect_jobworkerp_server_service;
use grpc_admin::proto::jobworkerp_conductor::data::{JobworkerpServer, JobworkerpServerData};
use grpc_admin::proto::jobworkerp_conductor::service::{
    jobworkerp_server_service_client::JobworkerpServerServiceClient, FindByNameRequest,
};
use tempfile::TempDir;
use tonic::transport::Channel;

pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
pub const STOP_TIMEOUT: Duration = Duration::from_secs(15);
/// Retry budget of the admin client; the process is already listening by then.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A port that was free when asked; the OS does not reuse it immediately.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("reserve a local port")
        .port()
}

/// Calls `check` until it returns Some, or returns None after `timeout`.
fn poll_until<T>(timeout: Duration, mut check: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check() {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(POLL_INTERVAL);
    }
}

fn accepts_connections(addr: SocketAddr) -> bool {
    TcpStream::connect_timeout(&addr, POLL_INTERVAL).is_ok()
}

/// A command for `binary` running in `workspace` with nothing inherited but
/// PATH, so settings of the developer's shell or a `.env` in a parent
/// directory cannot leak into the test. Logs go to `workspace`.
fn clean_command(binary: &Path, workspace: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .current_dir(workspace)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        // Both must be set, or the whole LOG_ config falls back to defaults.
        .env("LOG_USE_STDOUT", "true")
        .env("LOG_USE_JSON", "false")
        .env("LOG_FILE_DIR", workspace.join("log"));
    command
}

fn sqlite_url(workspace: &Path, file: &str) -> String {
    format!("sqlite://{}", workspace.join(file).display())
}

fn local_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], free_port()))
}

/// A child process killed when dropped, together with its workspace.
struct ChildGuard {
    child: Child,
    _workspace: TempDir,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub struct ConductorProcess {
    process: ChildGuard,
    addr: SocketAddr,
    log_path: PathBuf,
}

impl ConductorProcess {
    /// Starts `conductor-main` in its default serve mode.
    pub fn spawn() -> Self {
        Self::spawn_with_args(TempDir::new().expect("create workspace"), &[])
    }

    /// Starts `conductor-main serve --legacy-mode` with `workflows_toml` as
    /// its settings file. Legacy mode serves no admin API, so use
    /// [`Self::wait_for_log`] instead of [`Self::wait_until_ready`].
    pub fn spawn_legacy(workflows_toml: &str) -> Self {
        let workspace = TempDir::new().expect("create workspace");
        let settings = workspace.path().join("workflows.toml");
        std::fs::write(&settings, workflows_toml).expect("write workflows.toml");
        let settings = settings.to_str().expect("utf-8 path").to_string();
        Self::spawn_with_args(
            workspace,
            &["serve", "--legacy-mode", "--file-settings-toml", &settings],
        )
    }

    fn spawn_with_args(workspace: TempDir, args: &[&str]) -> Self {
        let addr = local_addr();
        let log_path = workspace.path().join("stdout.log");
        let log = File::create(&log_path).expect("create log file");

        let child = clean_command(
            Path::new(env!("CARGO_BIN_EXE_conductor-main")),
            workspace.path(),
        )
        .args(args)
        .env("GRPC_ADDR", addr.to_string())
        .env(
            "SQLITE_URL",
            sqlite_url(workspace.path(), "conductor.sqlite3"),
        )
        .env("SQLITE_MAX_CONNECTIONS", "5")
        .env("NOTIFICATION_TYPE", "channel")
        // conductor-main refuses to start without the memory cache settings.
        .env("MEMORY_CACHE_NUM_COUNTERS", "12960")
        .env("MEMORY_CACHE_MAX_COST", "1000000")
        .env("MEMORY_CACHE_USE_METRICS", "false")
        // Without RUST_LOG the tracing filter passes errors only.
        .env("RUST_LOG", "info")
        .stdout(Stdio::from(log.try_clone().expect("clone log file")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn conductor-main");

        Self {
            process: ChildGuard {
                child,
                _workspace: workspace,
            },
            addr,
            log_path,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Waits until `ready` holds; fails at once if the process exits first
    /// instead of waiting out the timeout.
    fn wait_for(&mut self, what: &str, mut ready: impl FnMut(&Self) -> bool) {
        let done = poll_until(STARTUP_TIMEOUT, || {
            if let Some(status) = self.try_wait() {
                panic!("conductor-main exited with {status:?}:\n{}", self.log());
            }
            ready(self).then_some(())
        });
        assert!(
            done.is_some(),
            "conductor-main never {what}:\n{}",
            self.log()
        );
    }

    /// Waits until the admin gRPC port accepts connections.
    pub fn wait_until_ready(&mut self) {
        let addr = self.addr;
        self.wait_for(&format!("listened on {addr}"), |_| {
            accepts_connections(addr)
        });
    }

    /// Waits until the log contains `needle`.
    pub fn wait_for_log(&mut self, needle: &str) {
        self.wait_for(&format!("logged {needle:?}"), |process| {
            process.log().contains(needle)
        });
    }

    /// Sends `signal` and asserts that the process exits with 0 in time.
    pub fn assert_stops_cleanly_on(&mut self, signal: &str) {
        self.signal(signal);
        let status = self
            .wait_for_exit(STOP_TIMEOUT)
            .unwrap_or_else(|| panic!("SIG{signal} did not stop conductor-main:\n{}", self.log()));
        assert!(
            status.success(),
            "SIG{signal} must exit with 0, got {status:?}:\n{}",
            self.log()
        );
    }

    /// Waits until ready, then connects to the JobworkerpServer service.
    pub async fn admin_client(&mut self) -> JobworkerpServerServiceClient<Channel> {
        self.wait_until_ready();
        connect_jobworkerp_server_service(&self.url(), CONNECT_TIMEOUT)
            .await
            .expect("connect to the conductor admin API")
    }

    /// Sends a signal by name (e.g. "TERM", "INT") with the system `kill`.
    pub fn signal(&self, name: &str) {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(self.process.child.id().to_string())
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -{name} failed");
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.process.child.try_wait().expect("poll conductor-main")
    }

    /// The exit status, or None if the process is still running after `timeout`.
    pub fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        poll_until(timeout, || self.try_wait())
    }

    /// Everything the process wrote to stdout and stderr so far.
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

/// A jobworkerp all-in-one process started the way the all-in-one image
/// starts it: a clean environment with Standalone storage in a temp dir.
pub struct JobworkerpProcess {
    _process: ChildGuard,
    url: String,
}

impl JobworkerpProcess {
    pub fn spawn(binary: &Path) -> Self {
        let workspace = TempDir::new().expect("create workspace");
        // jobworkerp searches parent directories for a .env when its own is missing.
        std::fs::write(workspace.path().join(".env"), "").expect("write empty .env");
        let addr = local_addr();
        let child = clean_command(binary, workspace.path())
            .env("STORAGE_TYPE", "Standalone")
            .env("GRPC_ADDR", addr.to_string())
            .env(
                "SQLITE_URL",
                sqlite_url(workspace.path(), "jobworkerp.sqlite3"),
            )
            .env("PLUGINS_RUNNER_DIR", workspace.path().join("plugins"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn jobworkerp all-in-one");
        let process = ChildGuard {
            child,
            _workspace: workspace,
        };
        assert!(
            poll_until(STARTUP_TIMEOUT, || accepts_connections(addr).then_some(())).is_some(),
            "jobworkerp all-in-one did not listen on {addr}"
        );
        Self {
            _process: process,
            url: format!("http://{addr}"),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

/// A JobworkerpServer record with the given name and port on 127.0.0.1.
pub fn server_data(name: &str, port: &str) -> JobworkerpServerData {
    JobworkerpServerData {
        name: name.to_string(),
        host: "127.0.0.1".to_string(),
        port: port.to_string(),
        ssl_enabled: false,
        description: None,
        enabled: true,
        created_at: 0,
        updated_at: 0,
    }
}

pub async fn find_server_by_name(
    client: &mut JobworkerpServerServiceClient<Channel>,
    name: &str,
) -> Option<JobworkerpServer> {
    client
        .find_by_name(FindByNameRequest {
            name: name.to_string(),
        })
        .await
        .expect("find_by_name")
        .into_inner()
        .data
}

/// The jobworkerp all-in-one binary the ignored tests run against.
pub fn all_in_one_binary() -> PathBuf {
    std::env::var_os("TEST_JOBWORKERP_ALL_IN_ONE_BIN")
        .map(PathBuf::from)
        .expect("set TEST_JOBWORKERP_ALL_IN_ONE_BIN to a jobworkerp all-in-one binary")
}
