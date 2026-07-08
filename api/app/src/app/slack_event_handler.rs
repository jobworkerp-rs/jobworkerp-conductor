use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use infra::error::UiEventHandlerError;
use infra::infra::jobworkerp_server::rdb::{
    JobworkerpServerRepository, JobworkerpServerRepositoryImpl,
};
use infra::infra::slack_event_handler::rdb::{
    SlackEventHandlerRepository, SlackEventHandlerRepositoryImpl, UseSlackEventHandlerRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use proto::jobworkerp_conductor::data::{
    SlackEventHandler, SlackEventHandlerData, SlackEventHandlerId,
};
use regex::RegexBuilder;
use shared::notification::service::ConfigChangeNotificationService;
use std::{sync::Arc, time::Duration};
use stretto::TokioCache;

#[async_trait]
pub trait SlackEventHandlerApp:
    UseSlackEventHandlerRepository
    + UseMemoryCache<Arc<String>, SlackEventHandler>
    + Send
    + Sync
    + Sized
    + 'static
{
    fn notification_service(&self) -> &Arc<dyn ConfigChangeNotificationService>;

    // Validation methods
    fn validate_event_type_consistency(data: &SlackEventHandlerData) -> Result<()>;
    fn validate_regex_pattern(pattern: &str) -> Result<()>;
    fn validate_reaction_names(names: &str) -> Result<()>;
    fn validate_workflow_url(url: &str) -> Result<()>;
    fn validate_handler_data(data: &SlackEventHandlerData) -> Result<()>;

    // CRUD operations
    async fn create_slack_event_handler(
        &self,
        data: &SlackEventHandlerData,
    ) -> Result<SlackEventHandlerId>;

    async fn update_slack_event_handler(
        &self,
        id: &SlackEventHandlerId,
        data: &SlackEventHandlerData,
    ) -> Result<bool>;

    async fn delete_slack_event_handler(&self, id: &SlackEventHandlerId) -> Result<bool>;

    // Query operations
    fn find_cache_key(id: &i64) -> String;
    fn find_by_name_cache_key(name: &str) -> String;

    async fn find_slack_event_handler(
        &self,
        id: &SlackEventHandlerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<SlackEventHandler>>
    where
        Self: Send + 'static;

    async fn find_slack_event_handler_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<SlackEventHandler>>
    where
        Self: Send + 'static;

    async fn find_slack_event_handler_list(&self) -> Result<Vec<SlackEventHandler>>
    where
        Self: Send + 'static;

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static;
}

pub struct SlackEventHandlerAppImpl {
    slack_event_handler_repository: SlackEventHandlerRepositoryImpl,
    jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
    memory_cache: TokioCache<Arc<String>, SlackEventHandler>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
    notification_service: Arc<dyn ConfigChangeNotificationService>,
}

impl SlackEventHandlerAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60;

    pub fn new(
        slack_event_handler_repository: SlackEventHandlerRepositoryImpl,
        jobworkerp_server_repository: JobworkerpServerRepositoryImpl,
        memory_cache: TokioCache<Arc<String>, SlackEventHandler>,
        notification_service: Arc<dyn ConfigChangeNotificationService>,
    ) -> Self {
        Self {
            slack_event_handler_repository,
            jobworkerp_server_repository,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024),
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
            notification_service,
        }
    }
}

impl UseSlackEventHandlerRepository for SlackEventHandlerAppImpl {
    fn slack_event_handler_repository(&self) -> &SlackEventHandlerRepositoryImpl {
        &self.slack_event_handler_repository
    }
}

#[async_trait]
impl SlackEventHandlerApp for SlackEventHandlerAppImpl {
    fn notification_service(&self) -> &Arc<dyn ConfigChangeNotificationService> {
        &self.notification_service
    }

    /// Validate event type consistency
    /// - Message and reaction conditions are mutually exclusive
    /// - At least one event condition must be specified
    fn validate_event_type_consistency(data: &SlackEventHandlerData) -> Result<()> {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let has_message_condition = data.message_pattern.is_some() || data.mention_required;
        let has_reaction_condition = data.reaction_names.is_some()
            || ReactionOperation::try_from(data.reaction_operation)
                .ok()
                .is_some_and(|v| v != ReactionOperation::Unspecified);

        // Check mutual exclusivity
        if has_message_condition && has_reaction_condition {
            return Err(anyhow!(
                "Message conditions and reaction conditions are mutually exclusive. \
                Cannot have both (message_pattern/mention_required) and (reaction_names/reaction_operation) set."
            ));
        }

        // Check at least one condition is set
        if !has_message_condition && !has_reaction_condition {
            return Err(anyhow!(
                "At least one event condition must be specified. \
                Set either message conditions (message_pattern/mention_required) or \
                reaction conditions (reaction_names/reaction_operation)."
            ));
        }

        Ok(())
    }

    /// Validate regex pattern for ReDoS attack prevention
    /// - Max pattern length: 500 characters
    /// - Compile size limit: 10MB
    /// - Compile timeout: 1 second
    fn validate_regex_pattern(pattern: &str) -> Result<()> {
        // Pattern length check
        if pattern.len() > 500 {
            return Err(anyhow!(
                "Regex pattern too long (max 500 chars): {} chars",
                pattern.len()
            ));
        }

        // Compile attempt with timeout
        let start = std::time::Instant::now();
        let _regex = RegexBuilder::new(pattern)
            .size_limit(10 * 1024 * 1024) // 10MB
            .build()
            .context("Invalid regex pattern")?;

        if start.elapsed() > Duration::from_secs(1) {
            return Err(anyhow!("Regex compilation timeout (max 1 second)"));
        }

        Ok(())
    }

    /// Validate reaction_names format
    /// - Max 50 reactions
    /// - Each reaction name max 100 characters
    /// - Allowed characters: alphanumeric, _, -, +
    fn validate_reaction_names(names: &str) -> Result<()> {
        let reactions: Vec<&str> = names.split(',').map(|s| s.trim()).collect();

        if reactions.len() > 50 {
            return Err(anyhow!("Too many reactions (max 50): {}", reactions.len()));
        }

        for reaction in reactions {
            if reaction.len() > 100 {
                return Err(anyhow!(
                    "Reaction name too long (max 100 chars): '{}'",
                    reaction
                ));
            }

            if !reaction
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '+')
            {
                return Err(anyhow!(
                    "Invalid reaction name '{}': only alphanumeric, _, -, + allowed",
                    reaction
                ));
            }
        }

        Ok(())
    }

    /// Validate workflow_url
    /// - Valid URL format
    /// - Allowed schemes: http, https, file
    /// - Path traversal prevention (no ..)
    /// - Max 2048 characters
    fn validate_workflow_url(url: &str) -> Result<()> {
        if url.len() > 2048 {
            return Err(anyhow!(
                "Workflow URL too long (max 2048 chars): {} chars",
                url.len()
            ));
        }

        // Basic URL validation
        if !url.starts_with("http://")
            && !url.starts_with("https://")
            && !url.starts_with("file://")
        {
            return Err(anyhow!(
                "Invalid URL scheme: must be http://, https://, or file://"
            ));
        }

        // Path traversal prevention
        if url.contains("..") {
            return Err(anyhow!(
                "Path traversal detected in URL: '..' is not allowed"
            ));
        }

        Ok(())
    }

    /// Validate all handler data fields (called by both create and update)
    fn validate_handler_data(data: &SlackEventHandlerData) -> Result<()> {
        Self::validate_event_type_consistency(data)?;

        if let Some(pattern) = &data.message_pattern {
            Self::validate_regex_pattern(pattern)?;
        }
        if let Some(filter) = &data.reaction_user_filter {
            Self::validate_regex_pattern(filter)?;
        }
        if let Some(names) = &data.reaction_names {
            Self::validate_reaction_names(names)?;
        }

        // Validate execution_target (worker_name non-empty, workflow_url non-empty, etc.)
        // Called inline (not via define_validate_execution_target_app! macro) because
        // SlackEventHandler needs additional URL-level validation after the base check.
        use proto::jobworkerp_conductor::data::slack_event_handler_data::ExecutionTarget;
        use shared::validation::{validate_execution_target_impl, ExecutionTargetRef};
        let et_ref = data.execution_target.as_ref().map(|t| match t {
            ExecutionTarget::Workflow(wf) => ExecutionTargetRef::Workflow(wf),
            ExecutionTarget::Worker(w) => ExecutionTargetRef::Worker(w),
        });
        validate_execution_target_impl(et_ref, &data.workflow_url).map_err(|msg| anyhow!(msg))?;

        // Additional URL-level validation (length, scheme, path traversal)
        match &data.execution_target {
            Some(ExecutionTarget::Workflow(wf)) => {
                Self::validate_workflow_url(&wf.workflow_url)?;
            }
            None if !data.workflow_url.is_empty() => {
                Self::validate_workflow_url(&data.workflow_url)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn find_cache_key(id: &i64) -> String {
        ["slack_event_handler_id:", &id.to_string()].join("")
    }

    fn find_by_name_cache_key(name: &str) -> String {
        ["slack_event_handler_name:", name].join("")
    }

    async fn find_slack_event_handler(
        &self,
        id: &SlackEventHandlerId,
        ttl: Option<&Duration>,
    ) -> Result<Option<SlackEventHandler>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        self.with_cache_if_some(&k, ttl, || async {
            self.slack_event_handler_repository().find(id).await
        })
        .await
    }

    async fn find_slack_event_handler_by_name(
        &self,
        name: &str,
        ttl: Option<&Duration>,
    ) -> Result<Option<SlackEventHandler>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_by_name_cache_key(name));
        self.with_cache_if_some(&k, ttl, || async {
            self.slack_event_handler_repository()
                .find_by_name(name)
                .await
        })
        .await
    }

    async fn find_slack_event_handler_list(&self) -> Result<Vec<SlackEventHandler>>
    where
        Self: Send + 'static,
    {
        self.slack_event_handler_repository().find_all().await
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        self.slack_event_handler_repository().count().await
    }

    async fn create_slack_event_handler(
        &self,
        data: &SlackEventHandlerData,
    ) -> Result<SlackEventHandlerId> {
        // Handle enabled field default value (proto3 default=false, DB default=true)
        let mut data = data.clone();
        if !data.enabled {
            tracing::warn!(
                "enabled field is false (proto3 default), setting to true (DB default) for handler '{}'",
                data.name
            );
            data.enabled = true;
        }

        Self::validate_handler_data(&data)?;

        // Transaction
        let db = self.slack_event_handler_repository().db_pool();
        let mut tx = db.begin().await.map_err(UiEventHandlerError::DBError)?;

        let id = self
            .slack_event_handler_repository()
            .create(&mut *tx, &data)
            .await?;

        // Fetch related JobworkerpServer data for DB independence
        let jobworkerp_server = if let Some(server_id) = &data.jobworkerp_server_id {
            self.jobworkerp_server_repository.find(server_id).await?
        } else {
            None
        };

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        // Notification
        let event = shared::config_events_proto::ConfigChangeEventWrapper::create_slack_event_handler_created(
            data.name.clone(),
            Some(id),
            Some(data),
            jobworkerp_server,
        );

        self.notification_service().notify(event).await?;

        Ok(id)
    }

    async fn update_slack_event_handler(
        &self,
        id: &SlackEventHandlerId,
        data: &SlackEventHandlerData,
    ) -> Result<bool> {
        Self::validate_handler_data(data)?;

        // Transaction
        let db = self.slack_event_handler_repository().db_pool();
        let mut tx = db.begin().await.map_err(UiEventHandlerError::DBError)?;

        let updated = self
            .slack_event_handler_repository()
            .update(&mut *tx, id, data)
            .await?;

        if !updated {
            return Ok(false);
        }

        // Fetch related JobworkerpServer data for DB independence
        let jobworkerp_server = if let Some(server_id) = &data.jobworkerp_server_id {
            self.jobworkerp_server_repository.find(server_id).await?
        } else {
            None
        };

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        // Invalidate cache
        let id_key = Arc::new(Self::find_cache_key(&id.value));
        let name_key = Arc::new(Self::find_by_name_cache_key(&data.name));
        self.memory_cache.remove(&id_key).await;
        self.memory_cache.remove(&name_key).await;

        // Notification
        let event = shared::config_events_proto::ConfigChangeEventWrapper::create_slack_event_handler_updated(
            data.name.clone(),
            Some(*id),
            Some(data.clone()),
            jobworkerp_server,
        );

        self.notification_service().notify(event).await?;

        Ok(true)
    }

    async fn delete_slack_event_handler(&self, id: &SlackEventHandlerId) -> Result<bool> {
        // Fetch handler data before deletion for notification
        let handler = self.slack_event_handler_repository().find(id).await?;

        if handler.is_none() {
            return Ok(false);
        }

        let handler_data = handler.as_ref().and_then(|h| h.data.as_ref());
        let name = handler_data.map(|d| d.name.clone()).unwrap_or_default();

        // Transaction
        let db = self.slack_event_handler_repository().db_pool();
        let mut tx = db.begin().await.map_err(UiEventHandlerError::DBError)?;

        let deleted = self
            .slack_event_handler_repository()
            .delete(&mut *tx, id)
            .await?;

        if !deleted {
            return Ok(false);
        }

        tx.commit().await.map_err(UiEventHandlerError::DBError)?;

        // Invalidate cache
        let id_key = Arc::new(Self::find_cache_key(&id.value));
        let name_key = Arc::new(Self::find_by_name_cache_key(&name));
        self.memory_cache.remove(&id_key).await;
        self.memory_cache.remove(&name_key).await;

        // Notification
        let event = shared::config_events_proto::ConfigChangeEventWrapper::create_slack_event_handler_deleted(
            name,
            Some(*id),
        );

        self.notification_service().notify(event).await?;

        Ok(true)
    }
}

impl UseMemoryCache<Arc<String>, SlackEventHandler> for SlackEventHandlerAppImpl {
    fn cache(&self) -> &TokioCache<Arc<String>, SlackEventHandler> {
        &self.memory_cache
    }

    fn key_lock(&self) -> &RwLockWithKey<Arc<String>> {
        &self.key_lock
    }

    fn default_ttl(&self) -> Option<&Duration> {
        Some(&self.default_ttl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_event_type_consistency_valid_message_only() {
        let data = SlackEventHandlerData {
            name: "test_handler".to_string(),
            message_pattern: Some("test".to_string()),
            ..Default::default()
        };

        let result = SlackEventHandlerAppImpl::validate_event_type_consistency(&data);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_event_type_consistency_valid_mention_only() {
        let data = SlackEventHandlerData {
            name: "test_handler".to_string(),
            mention_required: true,
            ..Default::default()
        };

        let result = SlackEventHandlerAppImpl::validate_event_type_consistency(&data);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_event_type_consistency_valid_reaction_only() {
        let data = SlackEventHandlerData {
            name: "test_handler".to_string(),
            reaction_names: Some("thumbsup".to_string()),
            ..Default::default()
        };

        let result = SlackEventHandlerAppImpl::validate_event_type_consistency(&data);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_event_type_consistency_invalid_mixed() {
        let data = SlackEventHandlerData {
            name: "test_handler".to_string(),
            message_pattern: Some("test".to_string()),
            reaction_names: Some("thumbsup".to_string()),
            ..Default::default()
        };

        let result = SlackEventHandlerAppImpl::validate_event_type_consistency(&data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mutually exclusive"));
    }

    #[tokio::test]
    async fn test_validate_event_type_consistency_invalid_no_conditions() {
        let data = SlackEventHandlerData {
            name: "test_handler".to_string(),
            ..Default::default()
        };

        let result = SlackEventHandlerAppImpl::validate_event_type_consistency(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("At least one"));
    }

    #[tokio::test]
    async fn test_validate_regex_pattern_valid() {
        let result = SlackEventHandlerAppImpl::validate_regex_pattern("^test.*");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_regex_pattern_too_long() {
        let long_pattern = "a".repeat(501);
        let result = SlackEventHandlerAppImpl::validate_regex_pattern(&long_pattern);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[tokio::test]
    async fn test_validate_regex_pattern_invalid() {
        let result = SlackEventHandlerAppImpl::validate_regex_pattern("[invalid");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_reaction_names_valid() {
        let result = SlackEventHandlerAppImpl::validate_reaction_names("thumbsup,heart,rocket");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_reaction_names_too_many() {
        let names = (0..51)
            .map(|i| format!("reaction{}", i))
            .collect::<Vec<_>>()
            .join(",");
        let result = SlackEventHandlerAppImpl::validate_reaction_names(&names);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max 50"));
    }

    #[tokio::test]
    async fn test_validate_reaction_names_too_long() {
        let long_name = "a".repeat(101);
        let result = SlackEventHandlerAppImpl::validate_reaction_names(&long_name);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max 100 chars"));
    }

    #[tokio::test]
    async fn test_validate_reaction_names_invalid_chars() {
        let result = SlackEventHandlerAppImpl::validate_reaction_names("invalid@name");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("alphanumeric"));
    }

    #[tokio::test]
    async fn test_validate_workflow_url_valid_http() {
        let result =
            SlackEventHandlerAppImpl::validate_workflow_url("http://example.com/workflow.yaml");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_workflow_url_valid_https() {
        let result =
            SlackEventHandlerAppImpl::validate_workflow_url("https://example.com/workflow.yaml");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_workflow_url_valid_file() {
        let result =
            SlackEventHandlerAppImpl::validate_workflow_url("file:///path/to/workflow.yaml");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_workflow_url_invalid_scheme() {
        let result =
            SlackEventHandlerAppImpl::validate_workflow_url("ftp://example.com/workflow.yaml");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid URL scheme"));
    }

    #[tokio::test]
    async fn test_validate_workflow_url_path_traversal() {
        let result = SlackEventHandlerAppImpl::validate_workflow_url("file:///../../../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Path traversal"));
    }

    #[tokio::test]
    async fn test_validate_workflow_url_too_long() {
        let long_url = format!("http://example.com/{}", "a".repeat(2040));
        let result = SlackEventHandlerAppImpl::validate_workflow_url(&long_url);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }
}
