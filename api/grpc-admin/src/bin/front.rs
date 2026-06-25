use anyhow::Result;
use dotenvy::dotenv;

// start front_server
#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    command_utils::util::tracing::tracing_init_test(tracing::Level::INFO);

    // trace::tracing_init_jaeger(&jeager_addr);
    let ret = grpc_admin::start_front_server().await;
    command_utils::util::tracing::shutdown_tracer_provider();
    ret
}
