#[macro_use]
extern crate debug_stub_derive;

pub mod front;
pub mod proto;
pub mod service;

use anyhow::Result;
use app::module::AppModule;
use std::env;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn start_front_server() -> Result<()> {
    let (grpc_addr, use_web, max_frame_size) = load_front_server_config();
    front::server::create_server(grpc_addr, use_web, max_frame_size)
        .await
        .map_err(|err| {
            tracing::error!("failed to create server: {:?}", err);
            err
        })
}

/// AppModuleを共有してgRPCサーバーを起動
///
/// 同一プロセス内でAppModuleを共有することで、NotificationServiceのインスタンスを
/// 統一し、確実な通知配信を実現する。
pub async fn start_front_server_with_app_module(app_module: Arc<AppModule>) -> Result<()> {
    let (grpc_addr, use_web, max_frame_size) = load_front_server_config();

    tracing::info!(
        "🚀 gRPCサーバーを共有AppModuleで起動中 (addr: {})",
        grpc_addr
    );

    front::server::create_server_with_app_module(app_module, grpc_addr, use_web, max_frame_size)
        .await
        .map_err(|err| {
            tracing::error!("failed to create server with shared app module: {:?}", err);
            err
        })
}

/// AppModuleを共有して、外部 shutdown future で gRPC サーバーを停止する。
pub async fn start_front_server_with_app_module_and_shutdown<F>(
    app_module: Arc<AppModule>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (grpc_addr, use_web, max_frame_size) = load_front_server_config();

    tracing::info!(
        "🚀 gRPCサーバーを共有AppModuleで起動中 (addr: {})",
        grpc_addr
    );

    front::server::create_server_with_app_module_and_shutdown(
        app_module,
        grpc_addr,
        use_web,
        max_frame_size,
        shutdown,
    )
    .await
    .map_err(|err| {
        tracing::error!("failed to create server with shared app module: {:?}", err);
        err
    })
}

fn load_front_server_config() -> (SocketAddr, bool, Option<u32>) {
    let grpc_addr: SocketAddr = env::var("GRPC_ADDR")
        .expect("GRPC_ADDR must be specified.")
        .parse()
        .unwrap();
    let use_web: bool = env::var("USE_GRPC_WEB")
        .unwrap_or("false".to_owned())
        .parse()
        .unwrap();
    let max_frame_size: Option<u32> = env::var("MAX_FRAME_SIZE").ok().map(|s| s.parse().unwrap());

    (grpc_addr, use_web, max_frame_size)
}
