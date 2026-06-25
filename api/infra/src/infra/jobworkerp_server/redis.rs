use crate::error::UiEventHandlerError;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::redis::{RedisPool, UseRedisPool};
use prost::Message;
use proto::jobworkerp_conductor::data::{
    JobworkerpServer, JobworkerpServerData, JobworkerpServerId,
};
use redis::AsyncCommands;
use std::collections::BTreeMap;
use std::io::Cursor;

// TODO use if you need (not using in default)
#[async_trait]
pub trait RedisJobworkerpServerRepository: UseRedisPool + Sync + 'static
where
    Self: Send + 'static,
{
    const CACHE_KEY: &'static str = "JOBWORKERP_SERVER_DEF";

    async fn create(
        &self,
        id: &JobworkerpServerId,
        jobworkerp_server: &JobworkerpServerData,
    ) -> Result<()> {
        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset_nx(
                Self::CACHE_KEY,
                id.value,
                Self::serialize_jobworkerp_server(jobworkerp_server),
            )
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        match res {
            Ok(r) => {
                if r {
                    Ok(())
                } else {
                    Err(UiEventHandlerError::AlreadyExists(format!(
                        "jobworkerp_server creation error: already exists id={}",
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
        id: &JobworkerpServerId,
        jobworkerp_server: &JobworkerpServerData,
    ) -> Result<bool> {
        let m = Self::serialize_jobworkerp_server(jobworkerp_server);

        let res: Result<bool> = self
            .redis_pool()
            .get()
            .await?
            .hset(Self::CACHE_KEY, id.value, m)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        res
    }

    async fn delete(&self, id: &JobworkerpServerId) -> Result<bool> {
        self.redis_pool()
            .get()
            .await?
            .hdel(Self::CACHE_KEY, id.value)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into())
    }

    async fn find(&self, id: &JobworkerpServerId) -> Result<Option<JobworkerpServer>> {
        match self
            .redis_pool()
            .get()
            .await?
            .hget(Self::CACHE_KEY, id.value)
            .await
        {
            Ok(Some(v)) => Self::deserialize_to_jobworkerp_server(&v).map(|d| {
                Some(JobworkerpServer {
                    id: Some(*id),
                    data: Some(d),
                })
            }),
            Ok(None) => Ok(None),
            Err(e) => Err(UiEventHandlerError::RedisError(e).into()),
        }
    }

    async fn find_all(&self) -> Result<Vec<JobworkerpServer>> {
        let res: Result<BTreeMap<i64, Vec<u8>>> = self
            .redis_pool()
            .get()
            .await?
            .hgetall(Self::CACHE_KEY)
            .await
            .map_err(|e| UiEventHandlerError::RedisError(e).into());
        res.map(|tree| {
            tree.iter()
                .filter_map(|(id, v)| {
                    Self::deserialize_to_jobworkerp_server(v)
                        .ok()
                        .map(|d| JobworkerpServer {
                            id: Some(JobworkerpServerId { value: *id }),
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

    fn serialize_jobworkerp_server(w: &JobworkerpServerData) -> Vec<u8> {
        let mut buf = Vec::with_capacity(w.encoded_len());
        w.encode(&mut buf).unwrap();
        buf
    }

    fn deserialize_to_jobworkerp_server(buf: &Vec<u8>) -> Result<JobworkerpServerData> {
        JobworkerpServerData::decode(&mut Cursor::new(buf))
            .map_err(|e| UiEventHandlerError::CodecError(e).into())
    }
    fn deserialize_bytes_to_jobworkerp_server(buf: &[u8]) -> Result<JobworkerpServerData> {
        JobworkerpServerData::decode(&mut Cursor::new(buf))
            .map_err(|e| UiEventHandlerError::CodecError(e).into())
    }
}

impl<T: UseRedisPool + Send + Sync + 'static> RedisJobworkerpServerRepository for T {}

pub struct RedisJobworkerpServerRepositoryImpl {
    pub redis_pool: &'static RedisPool,
}

impl UseRedisPool for RedisJobworkerpServerRepositoryImpl {
    fn redis_pool(&self) -> &'static RedisPool {
        self.redis_pool
    }
}

pub trait UseRedisJobworkerpServerRepository {
    fn redis_jobworkerp_server_repository(&self) -> &RedisJobworkerpServerRepositoryImpl;
}

#[tokio::test]
async fn redis_test() -> Result<()> {
    let pool = infra_utils::infra::test::setup_test_redis_pool().await;

    let repo = RedisJobworkerpServerRepositoryImpl { redis_pool: pool };
    let id = JobworkerpServerId { value: 1 };
    let jobworkerp_server = &JobworkerpServerData {
        name: "hoge1".to_string(),
        host: "hoge2".to_string(),
        port: "hoge3".to_string(),
        ssl_enabled: true,
        description: Some("hoge5".to_string()),
        enabled: true,
        created_at: 0,
        updated_at: 0,
    };
    // clear first
    repo.delete(&id).await?;

    // create and find
    repo.create(&id, jobworkerp_server).await?;
    assert!(repo.create(&id, jobworkerp_server).await.err().is_some()); // already exists
    let res = repo.find(&id).await?;
    assert_eq!(res.and_then(|r| r.data).as_ref(), Some(jobworkerp_server));

    let mut jobworkerp_server2 = jobworkerp_server.clone();
    jobworkerp_server2.name = "fuga1".to_string();
    jobworkerp_server2.host = "fuga2".to_string();
    jobworkerp_server2.port = "fuga3".to_string();
    jobworkerp_server2.ssl_enabled = false;
    jobworkerp_server2.description = Some("fuga5".to_string());
    jobworkerp_server2.enabled = false;
    jobworkerp_server2.created_at = 0;
    jobworkerp_server2.updated_at = 0;
    // update and find
    assert!(!repo.upsert(&id, &jobworkerp_server2).await?);
    let res2 = repo.find(&id).await?;
    assert_eq!(
        res2.and_then(|r| r.data).as_ref(),
        Some(&jobworkerp_server2)
    );

    // delete and not found
    assert!(repo.delete(&id).await?);
    assert_eq!(repo.find(&id).await?, None);

    Ok(())
}
