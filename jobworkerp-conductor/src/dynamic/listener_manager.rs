use anyhow::Result;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_handler::job_result_listener::JobworkerpResultListener;
use jobworkerp_handler::settings::JobResultListenerSetting;
use shared::SharedLocalConfigStore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// 動的リスナー管理（ローカル設定ベース、循環参照回避）
///
/// JobResultListenerの動的制御を提供し、設定変更に応じて
/// リスナーの起動・停止・更新を安全に実行する。
#[derive(Clone)]
pub struct DynamicListenerManager {
    /// Keyed by handler id: by the time a change event arrives the local
    /// store already holds the new (or no) entry, so the old name is gone.
    active_listeners: Arc<tokio::sync::Mutex<HashMap<i64, ListenerHandle>>>,
    local_config_store: SharedLocalConfigStore,
    execution_ref_recorder: shared::SharedExecutionRefRecorder,
}

struct ListenerHandle {
    name: String,
    task_handle: JoinHandle<Result<()>>,
    shutdown_sender: oneshot::Sender<()>,
}

impl ListenerHandle {
    /// Stops the listener and hands back its name for logging.
    async fn stop(self) -> String {
        let _ = self.shutdown_sender.send(());
        let _ = self.task_handle.await;
        self.name
    }
}

impl DynamicListenerManager {
    /// ローカル設定ベースの初期化（循環参照回避）
    pub fn new_with_local_config(local_config_store: SharedLocalConfigStore) -> Self {
        Self::new_with_local_config_and_recorder(
            local_config_store,
            shared::noop_execution_ref_recorder(),
        )
    }

    pub fn new_with_local_config_and_recorder(
        local_config_store: SharedLocalConfigStore,
        execution_ref_recorder: shared::SharedExecutionRefRecorder,
    ) -> Self {
        Self {
            active_listeners: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            local_config_store,
            execution_ref_recorder,
        }
    }

    /// ローカル設定からリスナー追加（IDベース、App層非依存）
    pub async fn add_listener_from_local(
        &self,
        handler_id: &proto::jobworkerp_conductor::data::WorkerResultHandlerId,
    ) -> Result<()> {
        let handler = {
            let store = self.local_config_store.read().unwrap();
            store
                .get_worker_result_handler(handler_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("WorkerResultHandler id={} not found", handler_id.value)
                })?
                .clone()
        };

        let handler_data = handler
            .data
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("WorkerResultHandler data is missing"))?;

        if !handler_data.enabled {
            tracing::debug!(
                "WorkerResultHandler id={} is disabled, skipping",
                handler_id.value
            );
            return Ok(());
        }

        let name = handler_data.name.clone();

        // 既存のリスナーが存在する場合は停止
        self.remove_listener_by_id(handler_id).await?;

        // ローカル設定からリスナー設定を作成
        let listener_setting = self
            .create_listener_setting_from_local(handler_id, handler_data)
            .await?;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        let listener_name = name.clone();
        let task_handle = tokio::spawn(async move {
            tokio::select! {
                result = JobworkerpResultListener::listen(listener_setting) => {
                    result
                }
                _ = shutdown_receiver => {
                    tracing::info!("Job result listener '{}' shutdown requested", listener_name);
                    Ok(())
                }
            }
        });

        let handle = ListenerHandle {
            name: name.clone(),
            task_handle,
            shutdown_sender,
        };

        self.active_listeners
            .lock()
            .await
            .insert(handler_id.value, handle);
        tracing::info!(
            "Added WorkerResultHandler: id={}, name={}",
            handler_id.value,
            name
        );

        Ok(())
    }

    /// イベントからリスナーを更新（IDベース）
    pub async fn update_listener_from_event(
        &self,
        config_event: &shared::config_events_proto::ConfigChangeEventWrapper,
    ) -> Result<()> {
        use proto::jobworkerp_conductor::data::ChangeAction;
        use shared::config_events_proto::EntityId;

        let handler_id = match config_event.typed_id() {
            Some(EntityId::WorkerResultHandler(id)) => id,
            _ => {
                return Err(anyhow::anyhow!(
                    "WorkerResultHandler event has no id or wrong type"
                ))
            }
        };

        match config_event.action() {
            ChangeAction::Created | ChangeAction::Updated => {
                self.remove_listener_by_id(&handler_id).await?;
                self.add_listener_from_local(&handler_id).await?;
            }
            ChangeAction::Deleted => {
                self.remove_listener_by_id(&handler_id).await?;
            }
            _ => {}
        }

        Ok(())
    }

    /// IDベースでリスナーを削除
    async fn remove_listener_by_id(
        &self,
        handler_id: &proto::jobworkerp_conductor::data::WorkerResultHandlerId,
    ) -> Result<()> {
        let handle = self.active_listeners.lock().await.remove(&handler_id.value);
        if let Some(handle) = handle {
            let name = handle.stop().await;
            tracing::info!(
                "Removed WorkerResultHandler: id={}, name={}",
                handler_id.value,
                name
            );
        }

        Ok(())
    }

    pub async fn stop_all(&self) -> Result<()> {
        let handles: Vec<_> = self.active_listeners.lock().await.drain().collect();

        for (_, handle) in handles {
            handle.stop().await;
        }

        tracing::info!("Stopped all job result listeners");
        Ok(())
    }

    /// アクティブリスナー数取得
    pub fn active_listener_count(&self) -> usize {
        match self.active_listeners.try_lock() {
            Ok(listeners) => listeners.len(),
            Err(_) => {
                tracing::warn!("Failed to acquire listeners lock for count");
                0
            }
        }
    }

    /// ローカル設定からリスナー設定作成（App層非依存）
    #[allow(clippy::await_holding_lock)]
    async fn create_listener_setting_from_local(
        &self,
        handler_id: &proto::jobworkerp_conductor::data::WorkerResultHandlerId,
        handler_data: &proto::jobworkerp_conductor::data::WorkerResultHandlerData,
    ) -> Result<JobResultListenerSetting> {
        // ローカル設定からJobworkerpServerの設定を取得
        let (listen_server_data, process_server_data, process_server_id_value) = {
            let store = self.local_config_store.read().unwrap();

            let listen_server_id = handler_data
                .listen_jobworkerp_server_id
                .as_ref()
                .ok_or_else(|| {
                    anyhow::anyhow!("WorkerResultHandler listen_jobworkerp_server_id is missing")
                })?;

            let listen_server = store
                .get_jobworkerp_server(listen_server_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Listen JobworkerpServer not found in local config: {}",
                        listen_server_id.value
                    )
                })?;

            let process_server_id = handler_data
                .process_jobworkerp_server_id
                .as_ref()
                .ok_or_else(|| {
                    anyhow::anyhow!("WorkerResultHandler process_jobworkerp_server_id is missing")
                })?;

            let process_server =
                store
                    .get_jobworkerp_server(process_server_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Process JobworkerpServer not found in local config: {}",
                            process_server_id.value
                        )
                    })?;

            let listen_server_data = listen_server
                .data
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Listen JobworkerpServer data is missing"))?
                .clone();
            let process_server_data = process_server
                .data
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Process JobworkerpServer data is missing"))?
                .clone();

            (
                listen_server_data,
                process_server_data,
                process_server_id.value,
            )
        }; // store is automatically dropped here

        // JobworkerpClientWrapperの作成（host:portから address を構築）
        let listen_address = if listen_server_data.ssl_enabled {
            format!(
                "https://{}:{}",
                listen_server_data.host, listen_server_data.port
            )
        } else {
            format!(
                "http://{}:{}",
                listen_server_data.host, listen_server_data.port
            )
        };

        let process_address = if process_server_data.ssl_enabled {
            format!(
                "https://{}:{}",
                process_server_data.host, process_server_data.port
            )
        } else {
            format!(
                "http://{}:{}",
                process_server_data.host, process_server_data.port
            )
        };

        let listen_client = Arc::new(JobworkerpClientWrapper::new(&listen_address, None).await?);
        let process_client = Arc::new(JobworkerpClientWrapper::new(&process_address, None).await?);

        // Extract execution target fields from oneof
        use proto::jobworkerp_conductor::data::worker_result_handler_data::ExecutionTarget;
        let (workflow_url, channel, worker_name, using) = match &handler_data.execution_target {
            Some(ExecutionTarget::Worker(w)) => (
                String::new(),
                None,
                Some(w.worker_name.clone()),
                w.using.as_ref().filter(|s| !s.is_empty()).cloned(),
            ),
            Some(ExecutionTarget::Workflow(wf)) => {
                (wf.workflow_url.clone(), wf.channel.clone(), None, None)
            }
            None => {
                // Fallback to deprecated fields
                (
                    handler_data.workflow_url.clone(),
                    handler_data.channel.clone(),
                    None,
                    None,
                )
            }
        };

        Ok(JobResultListenerSetting {
            handler_id: Some(handler_id.value),
            name: handler_data.name.clone(),
            listen_worker_name: handler_data.listen_worker_name.clone(),
            workflow_url,
            channel,
            listen_jobworkerp: listen_client,
            process_jobworkerp: process_client,
            args: handler_data.args.clone(),
            worker_name,
            using,
            process_jobworkerp_server_id: Some(process_server_id_value),
            execution_ref_recorder: self.execution_ref_recorder.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::{
        JobworkerpServer, JobworkerpServerData, JobworkerpServerId, WorkerResultHandlerData,
    };
    use shared::LocalConfigStore;
    use std::sync::RwLock;

    #[tokio::test]
    async fn test_dynamic_listener_manager_creation() {
        let local_config_store = Arc::new(RwLock::new(LocalConfigStore::new()));
        let manager = DynamicListenerManager::new_with_local_config(local_config_store);
        assert_eq!(manager.active_listener_count(), 0);
    }

    #[tokio::test]
    async fn test_listener_lifecycle() {
        use proto::jobworkerp_conductor::data::{WorkerResultHandler, WorkerResultHandlerId};

        let local_config_store = Arc::new(RwLock::new(LocalConfigStore::new()));

        // テスト用のJobworkerpServerを設定
        {
            let mut store = local_config_store.write().unwrap();
            let server = JobworkerpServer {
                id: Some(JobworkerpServerId { value: 1 }),
                data: Some(JobworkerpServerData {
                    name: "test_server".to_string(),
                    host: "localhost".to_string(),
                    port: "50051".to_string(),
                    ssl_enabled: false,
                    description: Some("Test server".to_string()),
                    enabled: true,
                    created_at: 0,
                    updated_at: 0,
                }),
            };
            store.upsert_jobworkerp_server(server.clone()).unwrap();

            // テスト用のWorkerResultHandlerを設定
            let handler = WorkerResultHandler {
                id: Some(WorkerResultHandlerId { value: 100 }),
                data: Some(WorkerResultHandlerData {
                    name: "test_listener".to_string(),
                    enabled: true,
                    workflow_url: "file://test_workflow.yaml".to_string(),
                    channel: Some("test_channel".to_string()),
                    listen_worker_name: "test_worker".to_string(),
                    listen_jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                    process_jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                    description: Some("Test listener".to_string()),
                    created_at: 0,
                    updated_at: 0,
                    args: Some(r#"{"test": "listener_args"}"#.to_string()),
                    execution_target: None,
                }),
            };
            store.upsert_worker_result_handler(handler).unwrap();
        }

        let manager = DynamicListenerManager::new_with_local_config(local_config_store);

        // リスナー追加（実際のgRPC接続はテスト環境では失敗するが、構造のテストは可能）
        let handler_id = proto::jobworkerp_conductor::data::WorkerResultHandlerId { value: 100 };
        let result = manager.add_listener_from_local(&handler_id).await;

        // 接続エラーが予想されるが、リスナーハンドルは作成される
        match result {
            Ok(_) => {
                assert_eq!(manager.active_listener_count(), 1);
            }
            Err(e) => {
                // gRPC接続エラーは予想される
                tracing::info!("Expected connection error in test: {:?}", e);
            }
        }

        // 全停止
        let _ = manager.stop_all().await;
        assert_eq!(manager.active_listener_count(), 0);
    }

    mod events {
        use super::*;
        use proto::jobworkerp_conductor::data::{WorkerResultHandler, WorkerResultHandlerId};
        use shared::config_events_proto::ConfigChangeEventWrapper as Event;
        use std::collections::HashMap;

        const ID: WorkerResultHandlerId = WorkerResultHandlerId { value: 100 };

        fn handler(name: &str) -> WorkerResultHandler {
            WorkerResultHandler {
                id: Some(ID),
                data: Some(WorkerResultHandlerData {
                    name: name.to_string(),
                    enabled: true,
                    workflow_url: "file://wf.yaml".to_string(),
                    listen_worker_name: "source".to_string(),
                    listen_jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                    process_jobworkerp_server_id: Some(JobworkerpServerId { value: 1 }),
                    ..Default::default()
                }),
            }
        }

        /// A manager running listener `name` against a port that accepts
        /// connections, so the listener starts without a jobworkerp server.
        async fn running(name: &str) -> (DynamicListenerManager, tokio::net::TcpListener) {
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let store = Arc::new(RwLock::new(LocalConfigStore::new()));
            {
                let mut store = store.write().unwrap();
                store
                    .upsert_jobworkerp_server(JobworkerpServer {
                        id: Some(JobworkerpServerId { value: 1 }),
                        data: Some(JobworkerpServerData {
                            name: "local".to_string(),
                            host: "127.0.0.1".to_string(),
                            port: socket.local_addr().unwrap().port().to_string(),
                            enabled: true,
                            ..Default::default()
                        }),
                    })
                    .unwrap();
                store.upsert_worker_result_handler(handler(name)).unwrap();
            }
            let manager = DynamicListenerManager::new_with_local_config(store);
            manager.add_listener_from_local(&ID).await.unwrap();
            assert_eq!(manager.active_listener_count(), 1);
            (manager, socket)
        }

        // The config change loop updates the local store before it tells the
        // manager, so the manager must not rely on the store's old entry.
        #[tokio::test]
        async fn a_deleted_listener_stops_after_the_store_dropped_it() {
            let (manager, _socket) = running("on-result").await;
            manager
                .local_config_store
                .write()
                .unwrap()
                .remove_worker_result_handler(&ID);
            let deleted = Event::create_worker_result_handler_deleted(
                "on-result".to_string(),
                Some(ID),
                None,
                HashMap::new(),
            );
            manager.update_listener_from_event(&deleted).await.unwrap();
            assert_eq!(manager.active_listener_count(), 0);
        }

        #[tokio::test]
        async fn a_renamed_listener_replaces_the_old_one() {
            let (manager, _socket) = running("old-name").await;
            let renamed = handler("new-name");
            manager
                .local_config_store
                .write()
                .unwrap()
                .upsert_worker_result_handler(renamed.clone())
                .unwrap();
            let updated = Event::create_worker_result_handler_updated(
                "new-name".to_string(),
                Some(ID),
                renamed.data,
                HashMap::new(),
            );
            manager.update_listener_from_event(&updated).await.unwrap();
            assert_eq!(manager.active_listener_count(), 1);
            manager.stop_all().await.unwrap();
        }
    }
}
