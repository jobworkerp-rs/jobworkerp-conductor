use crate::error::UiEventHandlerError;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::redis::{RedisPool, UseRedisPool};
use prost::Message;
use proto::jobworkerp_conductor::data::{
    WorkerResultHandler, WorkerResultHandlerData, WorkerResultHandlerId,
};
use redis::AsyncCommands;
use std::collections::BTreeMap;
use std::io::Cursor;

// TODO use if you need (not using in default)
#[async_trait]
pub trait RedisWorkerResultHandlerRepository: UseRedisPool + Sync + 'static
where
    Self: Send + 'static,
{
    const CACHE_KEY: &'static str = "WORKER_RESULT_HANDLER_DEF";

    async fn create(
        &self,
        id: &WorkerResultHandlerId,
        worker_result_handler: &WorkerResultHandlerData,
    ) -> Result<()> {
        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset_nx(
                Self::CACHE_KEY,
                id.value,
                Self::serialize_worker_result_handler(worker_result_handler),
            )
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        match res {
            Ok(r) => {
                if r {
                    Ok(())
                } else {
                    Err(UiEventHandlerError::AlreadyExists(format!(
                        "worker_result_handler creation error: already exists id={}",
                        id.value
                    ))
                    .into())
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn upsert(
        &self,
        id: &WorkerResultHandlerId,
        worker_result_handler: &WorkerResultHandlerData,
    ) -> Result<bool> {
        let m = Self::serialize_worker_result_handler(worker_result_handler);

        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset(Self::CACHE_KEY, id.value, m)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        res
    }

    async fn delete(&self, id: &WorkerResultHandlerId) -> Result<bool> {
        self.redis_pool()
            .get()
            .await?
            .hdel(Self::CACHE_KEY, id.value)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into())
    }

    async fn find(&self, id: &WorkerResultHandlerId) -> Result<Option<WorkerResultHandler>> {
        match self
            .redis_pool()
            .get()
            .await?
            .hget(Self::CACHE_KEY, id.value)
            .await
        {
            Ok(Some(v)) => Self::deserialize_to_worker_result_handler(&v).map(|d| {
                Some(WorkerResultHandler {
                    id: Some(*id),
                    data: Some(d),
                })
            }),
            Ok(None) => Ok(None),
            Err(e) => Err(UiEventHandlerError::RedisError(e).into()),
        }
    }

    async fn find_all(&self) -> Result<Vec<WorkerResultHandler>> {
        let res: Result<BTreeMap<i64, Vec<u8>>> = self
            .redis_pool()
            .get()
            .await?
            .hgetall(Self::CACHE_KEY)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        res.map(|tree| {
            tree.iter()
                .flat_map(|(id, v)| {
                    Self::deserialize_to_worker_result_handler(v).map(|d| WorkerResultHandler {
                        id: Some(WorkerResultHandlerId { value: *id }),
                        data: Some(d),
                    })
                })
                .collect()
        })
    }

    async fn count(&self) -> Result<i64> {
        self.redis_pool()
            .get()
            .await?
            .hlen(Self::CACHE_KEY)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into())
    }

    fn serialize_worker_result_handler(w: &WorkerResultHandlerData) -> Vec<u8> {
        let mut buf = Vec::with_capacity(w.encoded_len());
        w.encode(&mut buf).unwrap();
        buf
    }

    fn deserialize_to_worker_result_handler(buf: &Vec<u8>) -> Result<WorkerResultHandlerData> {
        WorkerResultHandlerData::decode(&mut Cursor::new(buf))
            .map_err(|e| UiEventHandlerError::CodecError(e).into())
    }
    fn deserialize_bytes_to_worker_result_handler(buf: &[u8]) -> Result<WorkerResultHandlerData> {
        WorkerResultHandlerData::decode(&mut Cursor::new(buf))
            .map_err(|e| UiEventHandlerError::CodecError(e).into())
    }
}

impl<T: UseRedisPool + Send + Sync + 'static> RedisWorkerResultHandlerRepository for T {}

pub struct RedisWorkerResultHandlerRepositoryImpl {
    pub redis_pool: &'static RedisPool,
}

impl UseRedisPool for RedisWorkerResultHandlerRepositoryImpl {
    fn redis_pool(&self) -> &'static RedisPool {
        self.redis_pool
    }
}

pub trait UseRedisWorkerResultHandlerRepository {
    fn redis_worker_result_handler_repository(&self) -> &RedisWorkerResultHandlerRepositoryImpl;
}

#[tokio::test]
async fn redis_test() -> Result<()> {
    use command_utils::util::datetime::now_seconds;
    let pool = infra_utils::infra::test::setup_test_redis_pool().await;

    let repo = RedisWorkerResultHandlerRepositoryImpl { redis_pool: pool };
    let id = WorkerResultHandlerId { value: 1 };
    let worker_result_handler = &WorkerResultHandlerData {
        name: "hoge1".to_string(),
        listen_jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
            value: 3,
        }),
        listen_worker_name: "hoge3".to_string(),
        process_jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
            value: 5,
        }),
        workflow_url: "hoge5".to_string(),
        channel: Some("hoge6".to_string()),
        enabled: true,
        description: Some("hoge8".to_string()),
        created_at: now_seconds(),
        updated_at: now_seconds(),
        args: Some(r#"{"redis": "worker_test"}"#.to_string()),
        execution_target: None,
    };
    // clear first
    repo.delete(&id).await?;

    // create and find
    repo.create(&id, worker_result_handler).await?;
    assert!(repo
        .create(&id, worker_result_handler)
        .await
        .err()
        .is_some()); // already exists
    let res = repo.find(&id).await?;
    assert_eq!(
        res.and_then(|r| r.data).as_ref(),
        Some(worker_result_handler)
    );

    let mut worker_result_handler2 = worker_result_handler.clone();
    worker_result_handler2.name = "fuga1".to_string();
    worker_result_handler2.listen_jobworkerp_server_id =
        Some(proto::jobworkerp_conductor::data::JobworkerpServerId { value: 4 });
    worker_result_handler2.listen_worker_name = "fuga3".to_string();
    worker_result_handler2.process_jobworkerp_server_id =
        Some(proto::jobworkerp_conductor::data::JobworkerpServerId { value: 6 });
    worker_result_handler2.workflow_url = "fuga5".to_string();
    worker_result_handler2.channel = Some("fuga6".to_string());
    worker_result_handler2.enabled = false;
    worker_result_handler2.description = Some("fuga8".to_string());
    worker_result_handler2.created_at = worker_result_handler.created_at;
    worker_result_handler2.updated_at = worker_result_handler.updated_at;
    // update and find
    assert!(!repo.upsert(&id, &worker_result_handler2).await?);
    let res2 = repo.find(&id).await?;
    assert_eq!(
        res2.and_then(|r| r.data).as_ref(),
        Some(&worker_result_handler2)
    );

    // delete and not found
    assert!(repo.delete(&id).await?);
    assert_eq!(repo.find(&id).await?, None);

    Ok(())
}
