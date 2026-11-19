//! Container runtimes stop processes with SIGTERM and terminals with SIGINT;
//! conductor-main must run its graceful shutdown for both.

#[path = "support/conductor_process.rs"]
mod conductor_process;

use std::time::Duration;

use conductor_process::{all_in_one_binary, ConductorProcess, JobworkerpProcess, STOP_TIMEOUT};

const STOPPED_LOG: &str = "DB-driven UI Event Handler stopped";

fn assert_graceful_shutdown(signal: &str) {
    let mut conductor = ConductorProcess::spawn();
    conductor.wait_until_ready();

    conductor.assert_stops_cleanly_on(signal);

    assert!(
        conductor.log().contains(STOPPED_LOG),
        "SIG{signal} must run the graceful shutdown:\n{}",
        conductor.log()
    );
}

#[test]
fn sigterm_runs_graceful_shutdown() {
    assert_graceful_shutdown("TERM");
}

#[test]
fn sigint_runs_graceful_shutdown() {
    assert_graceful_shutdown("INT");
}

#[test]
fn sigterm_during_startup_does_not_hang() {
    let mut conductor = ConductorProcess::spawn();

    // Before the handler is installed the default action ends the process, so
    // only termination is guaranteed, not the exit status.
    conductor.signal("TERM");

    assert!(
        conductor.wait_for_exit(STOP_TIMEOUT).is_some(),
        "SIGTERM during startup must stop conductor-main:\n{}",
        conductor.log()
    );
}

#[test]
#[ignore = "needs a jobworkerp all-in-one binary (TEST_JOBWORKERP_ALL_IN_ONE_BIN)"]
fn sigterm_stops_legacy_mode_with_a_job_result_listener() {
    // Legacy settings connect to every jobworkerp server when they are loaded.
    let jobworkerp = JobworkerpProcess::spawn(&all_in_one_binary());
    let mut conductor = ConductorProcess::spawn_legacy(&format!(
        r#"
[[jobworkerp]]
name = "local"
address = "{url}"

[[listeners]]
name = "legacy-listener"
listen_jobworkerp = "local"
listen_worker_name = "NoSuchWorker"
workflow_url = "unused.yaml"
jobworkerp = "local"
"#,
        url = jobworkerp.url()
    ));
    conductor.wait_for_log("worker_name: legacy-listener");
    // The listener tasks are spawned right before the shutdown wait starts.
    std::thread::sleep(Duration::from_millis(500));

    conductor.assert_stops_cleanly_on("TERM");
}
