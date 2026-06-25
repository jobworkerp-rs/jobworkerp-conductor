use anyhow::anyhow;
use command_utils::util::tracing::LoggingConfig;
use slack_handler::SlackMessageHandlerServer;
use slack_morphism::SlackApiToken;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();

    // TODO https://github.com/abdolence/slack-morphism-rust/issues/286
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let conf = command_utils::util::tracing::load_tracing_config_from_env().unwrap_or_default();
    let log_filename =
        command_utils::util::tracing::create_filename_with_ip_postfix("slack_handler", "log");
    let conf = LoggingConfig {
        file_name: Some(log_filename),
        ..conf
    };
    command_utils::util::tracing::tracing_init(conf).await?;
    let token =
        std::env::var("SLACK_APP_TOKEN").map_err(|e| anyhow!("SLACK_APP_TOKEN : {:?}", e))?;
    let app_token = Arc::new(SlackApiToken::new(token.into()));
    // let bot_token: SlackApiToken = SlackApiToken::new(
    //     std::env::var("SLACK_BOT_TOKEN")
    //         .context("parse env SLACK_BOT_TOKEN")?
    //         .into(),
    // );

    // let listen_worker_name =
    //     std::env::var("LISTEN_WORKER_NAME").context("LISTEN_WORKER_NAME is not set")?;

    // tokio::spawn(async move {
    //     let jobworkerp_listener =
    //         jobworkerp_handler::job_result_listener::JobworkerpResultListener::new(bot_token)
    //             .await
    //             .expect("jobworkerp_listener init failed");
    //     jobworkerp_listener
    //         .listen(listen_worker_name)
    //         .await
    //         .expect("jobworkerp_listener listen failed");
    // });
    SlackMessageHandlerServer::new(app_token)
        .await?
        .serve()
        .await?;

    Ok(())
}
