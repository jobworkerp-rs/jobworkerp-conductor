/// Phase 4: DB-driven UI Event Handler Main
///
/// Full migration from TOML file-driven to DB-driven architecture.
/// Uses InitializationLayer for clean architecture.
use clap::{Parser, Subcommand};
use command_utils::util::tracing::LoggingConfig;
use jobworkerp_handler::settings::WorkflowSettings;
use tokio::signal;
use tokio::sync::watch;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Enable tracing (generates a trace-timestamp.json file).
    #[arg(short, long, default_value = "false")]
    debug: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the conductor server (default when no subcommand is given)
    Serve(ServeArgs),

    /// Register conductor admin workers and FunctionSet on a jobworkerp server
    RegisterWorkers(RegisterWorkersArgs),

    /// Unregister conductor admin workers and FunctionSet from a jobworkerp server
    UnregisterWorkers(UnregisterWorkersArgs),
}

#[derive(clap::Args, Debug)]
struct ServeArgs {
    /// Legacy mode: use TOML file instead of DB
    #[arg(long)]
    legacy_mode: bool,

    /// TOML file path (for legacy mode only)
    #[arg(long, default_value = "./workflows.toml")]
    file_settings_toml: String,
}

#[derive(clap::Args, Debug)]
struct RegisterWorkersArgs {
    /// jobworkerp server gRPC endpoint
    #[arg(long, env = "JOBWORKERP_URL")]
    jobworkerp_url: String,

    /// Conductor gRPC endpoint (set as runner_settings for registered workers)
    #[arg(
        long,
        env = "CONDUCTOR_GRPC_URL",
        default_value = "http://localhost:9090"
    )]
    conductor_grpc_url: String,
}

#[derive(clap::Args, Debug)]
struct UnregisterWorkersArgs {
    /// jobworkerp server gRPC endpoint
    #[arg(long, env = "JOBWORKERP_URL")]
    jobworkerp_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Load .env before Args::parse() so clap's env feature can read .env values
    dotenvy::dotenv().ok();
    let args = Args::parse();

    // Initialize logging
    let conf = command_utils::util::tracing::load_tracing_config_from_env().unwrap_or_default();
    let log_filename =
        command_utils::util::tracing::create_filename_with_ip_postfix("conductor", "log");
    let mut conf = LoggingConfig {
        file_name: Some(log_filename),
        ..conf
    };
    if args.debug {
        conf.level = Some("debug".to_string());
    }
    command_utils::util::tracing::tracing_init(conf).await?;

    match args.command {
        None => {
            // No subcommand → default DB-driven mode
            tracing::info!("DB-driven mode enabled (Phase 4)");
            run_db_driven_mode().await
        }
        Some(Command::Serve(serve_args)) => {
            if serve_args.legacy_mode {
                // Legacy TOML file-driven mode
                tracing::info!("Legacy TOML file mode enabled");
                run_legacy_mode(&serve_args.file_settings_toml).await
            } else {
                // DB-driven mode
                tracing::info!("DB-driven mode enabled (Phase 4)");
                run_db_driven_mode().await
            }
        }
        Some(Command::RegisterWorkers(rw_args)) => {
            shared::worker_registration::register_conductor_workers(
                &rw_args.jobworkerp_url,
                &rw_args.conductor_grpc_url,
            )
            .await?;
            Ok(())
        }
        Some(Command::UnregisterWorkers(uw_args)) => {
            shared::worker_registration::unregister_conductor_workers(&uw_args.jobworkerp_url)
                .await?;
            Ok(())
        }
    }
}

/// Legacy TOML file-driven mode (backward compatibility)
async fn run_legacy_mode(toml_file: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use conductor_main::ConductorServer;
    use tokio::sync::OnceCell;

    static SERVER_CELL: OnceCell<ConductorServer> = OnceCell::<ConductorServer>::const_new();

    let workflow_settings = WorkflowSettings::load_from_toml_file(toml_file)
        .await
        .expect("load_from_toml_file failed");

    tracing::info!("workflow_settings: {:#?}", workflow_settings);
    let server = SERVER_CELL
        .get_or_init(|| async { ConductorServer::new(workflow_settings).await.unwrap() })
        .await;

    server
        .serve()
        .await
        .expect("jobworkerp_listener listen failed");

    Ok(())
}

/// DB-driven mode (Phase 4 full implementation)
async fn run_db_driven_mode() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Phase 4: Starting DB-driven UI Event Handler");

    // 1. Initialize App layer (NotificationService is also initialized automatically)
    let repositories = infra::infra::module::RepositoryModule::new_by_env().await;
    let app_module = app::module::AppModule::new_by_env(repositories).await;

    tracing::info!("App layer initialized (NotificationService integrated)");

    // 2. Safe lifetime management and separation of concerns - use InitializationConfigLoaderImpl
    let app_module_arc = std::sync::Arc::new(app_module);
    let initialization_config_loader =
        app::module::InitializationConfigLoaderImpl::new(app_module_arc.clone());
    let initialization_layer =
        jobworkerp_conductor::initialization::layer::InitializationLayer::new(std::sync::Arc::new(
            initialization_config_loader,
        ));

    // 3. Load initial config
    let initial_config = initialization_layer
        .load_initial_config()
        .await
        .expect("Failed to load initial configuration");

    tracing::info!(
        "Initial config loaded: {} total configs",
        initial_config.total_count()
    );

    // 4. Create and start EventHandlerServerManager (NotificationService obtained from AppModule)
    let mut server_manager = initialization_layer
        .create_server_manager(
            initial_config,
            app_module_arc.notification_service.clone(),
            app_module_arc.execution_status_app.clone(),
        )
        .expect("Failed to create EventHandlerServerManager");

    server_manager
        .start()
        .await
        .expect("Failed to start EventHandlerServerManager");

    tracing::info!("DB-driven UI Event Handler started");
    tracing::info!("System status:");
    tracing::info!(
        "  - Active listeners: {}",
        server_manager.get_active_listener_count()
    );
    tracing::info!(
        "  - Config stats: {:?}",
        server_manager.get_config_stats().unwrap_or_default()
    );

    // 5. Start gRPC admin server with shared AppModule so that the same
    //    NotificationService instance is used, ensuring reliable notification delivery.
    tracing::info!("gRPC server starting...");
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let grpc_app_module = app_module_arc.clone();
    let mut grpc_server = tokio::spawn(async move {
        grpc_admin::start_front_server_with_app_module_and_shutdown(
            grpc_app_module,
            wait_for_shutdown_request(shutdown_receiver),
        )
        .await
    });

    // 6. Graceful shutdown monitoring
    let grpc_server_result = tokio::select! {
        signal_result = signal::ctrl_c() => {
            signal_result?;
            tracing::info!("Shutdown signal received");
            let _ = shutdown_sender.send(true);
            None
        }
        result = &mut grpc_server => Some(result),
    };

    // 7. Stop system
    tracing::info!("Stopping system...");
    server_manager
        .stop()
        .await
        .expect("Failed to stop EventHandlerServerManager");

    match grpc_server_result {
        Some(result) => {
            result??;
        }
        None => {
            grpc_server.await??;
        }
    }

    tracing::info!("DB-driven UI Event Handler stopped");
    Ok(())
}

async fn wait_for_shutdown_request(mut shutdown_receiver: watch::Receiver<bool>) {
    if *shutdown_receiver.borrow() {
        return;
    }

    while shutdown_receiver.changed().await.is_ok() {
        if *shutdown_receiver.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_for_shutdown_request_returns_after_single_notification() {
        let (tx, rx) = tokio::sync::watch::channel(false);

        let waiter = tokio::spawn(wait_for_shutdown_request(rx));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());

        tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_shutdown_request_returns_when_already_requested() {
        let (_tx, rx) = tokio::sync::watch::channel(true);

        tokio::time::timeout(Duration::from_secs(1), wait_for_shutdown_request(rx))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_shutdown_request_returns_when_sender_is_dropped() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        drop(tx);

        tokio::time::timeout(Duration::from_secs(1), wait_for_shutdown_request(rx))
            .await
            .unwrap();
    }
}
