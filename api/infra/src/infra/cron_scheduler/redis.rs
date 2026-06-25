use crate::error::UiEventHandlerError;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::redis::{RedisPool, UseRedisPool};
use prost::Message;
use proto::jobworkerp_conductor::data::{CronScheduler, CronSchedulerData, CronSchedulerId};
use redis::AsyncCommands;
use std::collections::BTreeMap;
use std::io::Cursor;

// TODO use if you need (not using in default)
#[async_trait]
pub trait RedisCronSchedulerRepository: UseRedisPool + Sync + 'static
where
    Self: Send + 'static,
{
    const CACHE_KEY: &'static str = "CRON_SCHEDULER_DEF";

    async fn create(&self, id: &CronSchedulerId, cron_scheduler: &CronSchedulerData) -> Result<()> {
        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset_nx(
                Self::CACHE_KEY,
                id.value,
                Self::serialize_cron_scheduler(cron_scheduler),
            )
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        match res {
            Ok(r) => {
                if r {
                    Ok(())
                } else {
                    Err(UiEventHandlerError::AlreadyExists(format!(
                        "cron_scheduler creation error: already exists id={}",
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
        id: &CronSchedulerId,
        cron_scheduler: &CronSchedulerData,
    ) -> Result<bool> {
        let m = Self::serialize_cron_scheduler(cron_scheduler);

        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset(Self::CACHE_KEY, id.value, m)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        res
    }

    async fn delete(&self, id: &CronSchedulerId) -> Result<bool> {
        self.redis_pool()
            .get()
            .await?
            .hdel(Self::CACHE_KEY, id.value)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into())
    }

    async fn find(&self, id: &CronSchedulerId) -> Result<Option<CronScheduler>> {
        match self
            .redis_pool()
            .get()
            .await?
            .hget(Self::CACHE_KEY, id.value)
            .await
        {
            Ok(Some(v)) => Self::deserialize_to_cron_scheduler(&v).map(|d| {
                Some(CronScheduler {
                    id: Some(*id),
                    data: Some(d),
                })
            }),
            Ok(None) => Ok(None),
            Err(e) => Err(UiEventHandlerError::RedisError(e).into()),
        }
    }

    async fn find_all(&self) -> Result<Vec<CronScheduler>> {
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
                    Self::deserialize_to_cron_scheduler(v).map(|d| CronScheduler {
                        id: Some(CronSchedulerId { value: *id }),
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

    fn serialize_cron_scheduler(w: &CronSchedulerData) -> Vec<u8> {
        let mut buf = Vec::with_capacity(w.encoded_len());
        w.encode(&mut buf).unwrap();
        buf
    }

    fn deserialize_to_cron_scheduler(buf: &Vec<u8>) -> Result<CronSchedulerData> {
        CronSchedulerData::decode(&mut Cursor::new(buf))
            .map_err(|e| UiEventHandlerError::CodecError(e).into())
    }
    fn deserialize_bytes_to_cron_scheduler(buf: &[u8]) -> Result<CronSchedulerData> {
        CronSchedulerData::decode(&mut Cursor::new(buf))
            .map_err(|e| UiEventHandlerError::CodecError(e).into())
    }
}

impl<T: UseRedisPool + Send + Sync + 'static> RedisCronSchedulerRepository for T {}

pub struct RedisCronSchedulerRepositoryImpl {
    pub redis_pool: &'static RedisPool,
}

impl UseRedisPool for RedisCronSchedulerRepositoryImpl {
    fn redis_pool(&self) -> &'static RedisPool {
        self.redis_pool
    }
}

pub trait UseRedisCronSchedulerRepository {
    fn redis_cron_scheduler_repository(&self) -> &RedisCronSchedulerRepositoryImpl;
}

#[tokio::test]
async fn redis_test() -> Result<()> {
    use proto::jobworkerp_conductor::data::{
        cron_scheduler_data::ExecutionTarget, WorkerExecution,
    };
    let pool = infra_utils::infra::test::setup_test_redis_pool().await;

    let repo = RedisCronSchedulerRepositoryImpl { redis_pool: pool };
    let id = CronSchedulerId { value: 1 };
    let cron_scheduler = &CronSchedulerData {
        name: "hoge1".to_string(),
        jobworkerp_server_id: Some(proto::jobworkerp_conductor::data::JobworkerpServerId {
            value: 3,
        }),
        workflow_url: "hoge3".to_string(),
        channel: Some("hoge4".to_string()),
        crontab: "hoge5".to_string(),
        enabled: true,
        description: Some("hoge7".to_string()),
        created_at: 0,
        updated_at: 0,
        args: Some(r#"{"redis": "test"}"#.to_string()),
        execution_target: None,
    };
    // clear first
    repo.delete(&id).await?;

    // create and find
    repo.create(&id, cron_scheduler).await?;
    assert!(repo.create(&id, cron_scheduler).await.err().is_some()); // already exists
    let res = repo.find(&id).await?;
    assert_eq!(res.and_then(|r| r.data).as_ref(), Some(cron_scheduler));

    let mut cron_scheduler2 = cron_scheduler.clone();
    cron_scheduler2.name = "fuga1".to_string();
    cron_scheduler2.jobworkerp_server_id =
        Some(proto::jobworkerp_conductor::data::JobworkerpServerId { value: 4 });
    cron_scheduler2.workflow_url = "fuga3".to_string();
    cron_scheduler2.channel = Some("fuga4".to_string());
    cron_scheduler2.crontab = "fuga5".to_string();
    cron_scheduler2.enabled = false;
    cron_scheduler2.description = Some("fuga7".to_string());
    cron_scheduler2.created_at = 0;
    cron_scheduler2.updated_at = 0;
    cron_scheduler2.execution_target = Some(ExecutionTarget::Worker(WorkerExecution {
        worker_name: "test-worker".to_string(),
        r#using: Some("run".to_string()),
    }));
    // update and find (with WorkerExecution execution_target)
    assert!(!repo.upsert(&id, &cron_scheduler2).await?);
    let res2 = repo.find(&id).await?;
    assert_eq!(res2.and_then(|r| r.data).as_ref(), Some(&cron_scheduler2));

    // delete and not found
    assert!(repo.delete(&id).await?);
    assert_eq!(repo.find(&id).await?, None);

    Ok(())
}
