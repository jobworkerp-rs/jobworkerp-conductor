// Event Matcher
// Matches Slack events to configured handlers based on conditions

use crate::config_cache::SlackHandlerCache;
use anyhow::Result;
use proto::jobworkerp_conductor::data::SlackEventHandler;
use shared::SharedLocalConfigStore;
use slack_morphism::prelude::*;
use std::sync::{Arc, RwLock};

/// Event type for handler matching
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventType {
    Message,
    AppMention,
    ReactionAdded,
    ReactionRemoved,
}

/// Event Matcher
/// Matches incoming Slack events to configured handlers
pub struct EventMatcher {
    // LocalConfigStore reference (for configuration data)
    local_config_store: SharedLocalConfigStore,

    // Regex cache reference (for performance optimization)
    slack_handler_cache: Arc<RwLock<SlackHandlerCache>>,
}

impl EventMatcher {
    /// Create new EventMatcher
    pub fn new(
        local_config_store: SharedLocalConfigStore,
        slack_handler_cache: Arc<RwLock<SlackHandlerCache>>,
    ) -> Self {
        Self {
            local_config_store,
            slack_handler_cache,
        }
    }

    /// Fetch enabled handlers from LocalConfigStore
    pub fn fetch_enabled_handlers(&self) -> Result<Vec<SlackEventHandler>> {
        let store = self
            .local_config_store
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to read lock config store: {}", e))?;

        Ok(store
            .get_enabled_slack_event_handlers()
            .into_iter()
            .cloned()
            .collect())
    }

    /// Match event to handlers
    /// Returns handlers that match the event conditions
    ///
    /// Important: Implements duplicate execution prevention for mentions
    /// - mention_required=true: Only match app_mention events
    /// - mention_required=false: Only match message events
    pub fn match_event_to_handlers(
        &self,
        event: &SlackEventCallbackBody,
        channel_id: &SlackChannelId,
    ) -> Result<Vec<SlackEventHandler>> {
        let enabled_handlers = self.fetch_enabled_handlers()?;
        let mut matched_handlers = Vec::new();

        for handler in enabled_handlers {
            if let Some(data) = &handler.data {
                // Channel filter
                if let Some(target_channel) = &data.slack_channel_id {
                    if target_channel != channel_id.as_ref() {
                        continue;
                    }
                }

                // Event type specific matching
                let matches = match event {
                    SlackEventCallbackBody::AppMention(mention_event) => {
                        // Only match handlers with mention_required=true
                        // This prevents duplicate execution when both app_mention and message events fire
                        if !data.mention_required {
                            continue;
                        }

                        let text = mention_event.content.text.as_deref().unwrap_or("");

                        self.check_message_conditions(&handler, text)
                    }
                    SlackEventCallbackBody::Message(message_event) => {
                        // Only match handlers with mention_required=false
                        // This prevents duplicate execution when both app_mention and message events fire
                        if data.mention_required {
                            continue;
                        }

                        let text = message_event
                            .content
                            .as_ref()
                            .and_then(|c| c.text.as_deref())
                            .unwrap_or("");

                        self.check_message_conditions(&handler, text)
                    }
                    SlackEventCallbackBody::ReactionAdded(reaction_event) => self
                        .check_reaction_conditions(
                            &handler,
                            "added",
                            reaction_event.reaction.as_ref(),
                            reaction_event.user.as_ref(),
                        ),
                    SlackEventCallbackBody::ReactionRemoved(reaction_event) => self
                        .check_reaction_conditions(
                            &handler,
                            "removed",
                            reaction_event.reaction.as_ref(),
                            reaction_event.user.as_ref(),
                        ),
                    _ => {
                        // Unsupported event type
                        tracing::debug!("Unsupported event type for handler {}", data.name);
                        false
                    }
                };

                if matches {
                    matched_handlers.push(handler);
                }
            }
        }

        tracing::debug!(
            "Matched {} handlers for event in channel {}",
            matched_handlers.len(),
            channel_id
        );

        Ok(matched_handlers)
    }

    /// Check message event conditions (AND logic)
    /// Uses compiled regex from SlackHandlerCache
    fn check_message_conditions(&self, handler: &SlackEventHandler, message_text: &str) -> bool {
        let data = match &handler.data {
            Some(d) => d,
            None => return false,
        };

        // message_pattern check (if present)
        if data.message_pattern.is_some() {
            let handler_id = match &handler.id {
                Some(id) => id,
                None => return false,
            };

            let cache = match self.slack_handler_cache.read() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to read slack handler cache: {}", e);
                    return false;
                }
            };

            if let Some(regex) = cache.get_message_pattern(handler_id) {
                if !regex.is_match(message_text) {
                    return false;
                }
            } else {
                // Pattern is set but not cached - should not happen
                tracing::warn!(
                    "message_pattern set but not cached for handler {}",
                    data.name
                );
                return false;
            }
        }

        // All conditions matched (or no conditions set)
        true
    }

    /// Check reaction event conditions (AND logic)
    /// Uses cached data from SlackHandlerCache
    fn check_reaction_conditions(
        &self,
        handler: &SlackEventHandler,
        actual_operation: &str, // "added" or "removed"
        reaction_name: &str,
        user_id: &str,
    ) -> bool {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let data = match &handler.data {
            Some(d) => d,
            None => return false,
        };

        // reaction_operation filter
        let matches = match ReactionOperation::try_from(data.reaction_operation) {
            Ok(ReactionOperation::Added) => actual_operation == "added",
            Ok(ReactionOperation::Removed) => actual_operation == "removed",
            Ok(ReactionOperation::Both) => true, // Match both added and removed
            Ok(ReactionOperation::Unspecified) => {
                // UNSPECIFIED (default): treat as BOTH when reaction_names is set
                true
            }
            Err(_) => {
                tracing::warn!(
                    "Invalid reaction_operation value: {}",
                    data.reaction_operation
                );
                false
            }
        };

        if !matches {
            return false;
        }

        // reaction_names filter
        if data.reaction_names.is_some() {
            let handler_id = match &handler.id {
                Some(id) => id,
                None => return false,
            };

            let cache = match self.slack_handler_cache.read() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to read slack handler cache: {}", e);
                    return false;
                }
            };

            if let Some(names_set) = cache.get_reaction_names(handler_id) {
                if !names_set.contains(reaction_name) {
                    return false;
                }
            } else {
                // reaction_names set but not cached - should not happen
                tracing::warn!(
                    "reaction_names set but not cached for handler {}",
                    data.name
                );
                return false;
            }
        }
        // If reaction_names is None, match all reactions

        // reaction_user_filter check
        if data.reaction_user_filter.is_some() {
            let handler_id = match &handler.id {
                Some(id) => id,
                None => return false,
            };

            let cache = match self.slack_handler_cache.read() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to read slack handler cache: {}", e);
                    return false;
                }
            };

            if let Some(regex) = cache.get_reaction_user_filter(handler_id) {
                if !regex.is_match(user_id) {
                    return false;
                }
            } else {
                tracing::warn!(
                    "reaction_user_filter set but not cached for handler {}",
                    data.name
                );
                return false;
            }
        }
        // If reaction_user_filter is None, match all users

        // All conditions matched
        true
    }

    /// Determine event type from handler configuration
    /// (For future use if needed)
    #[allow(dead_code)]
    fn determine_event_type(handler: &SlackEventHandler) -> Option<EventType> {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let data = handler.data.as_ref()?;

        let has_message_condition = data.message_pattern.is_some() || data.mention_required;
        let has_reaction_condition = data.reaction_names.is_some()
            || ReactionOperation::try_from(data.reaction_operation)
                .ok()
                .is_some_and(|v| v != ReactionOperation::Unspecified);

        if has_message_condition && !has_reaction_condition {
            if data.mention_required {
                Some(EventType::AppMention)
            } else {
                Some(EventType::Message)
            }
        } else if has_reaction_condition && !has_message_condition {
            // Determine reaction type based on reaction_operation
            match ReactionOperation::try_from(data.reaction_operation).ok()? {
                ReactionOperation::Added => Some(EventType::ReactionAdded),
                ReactionOperation::Removed => Some(EventType::ReactionRemoved),
                ReactionOperation::Both | ReactionOperation::Unspecified => {
                    None // Both added and removed
                }
            }
        } else {
            // Invalid configuration or ambiguous
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::{SlackEventHandlerData, SlackEventHandlerId};
    use shared::LocalConfigStore;

    #[allow(clippy::too_many_arguments)]
    fn create_test_handler(
        id: i64,
        name: &str,
        enabled: bool,
        channel_id: Option<String>,
        message_pattern: Option<String>,
        mention_required: bool,
        reaction_names: Option<String>,
        reaction_operation: i32, // i32 (enum value, default: 0 = UNSPECIFIED)
    ) -> SlackEventHandler {
        SlackEventHandler {
            id: Some(SlackEventHandlerId { value: id }),
            data: Some(SlackEventHandlerData {
                name: name.to_string(),
                enabled,
                slack_channel_id: channel_id,
                message_pattern,
                mention_required,
                reaction_names,
                reaction_operation,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_fetch_enabled_handlers() {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let mut store = LocalConfigStore::default();
        let handler1 = create_test_handler(
            1,
            "handler1",
            true,
            None,
            None,
            false,
            None,
            ReactionOperation::Unspecified as i32,
        );
        let handler2 = create_test_handler(
            2,
            "handler2",
            false,
            None,
            None,
            false,
            None,
            ReactionOperation::Unspecified as i32,
        );

        store.upsert_slack_event_handler(handler1.clone()).unwrap();
        store.upsert_slack_event_handler(handler2).unwrap();

        let local_config_store = Arc::new(RwLock::new(store));
        let cache = Arc::new(RwLock::new(SlackHandlerCache::new()));
        let matcher = EventMatcher::new(local_config_store, cache);

        let enabled = matcher.fetch_enabled_handlers().unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].data.as_ref().unwrap().name, "handler1");
    }

    #[test]
    fn test_message_pattern_matching() {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let mut store = LocalConfigStore::default();
        let handler = create_test_handler(
            1,
            "deploy_handler",
            true,
            None,
            Some("^deploy".to_string()),
            false,
            None,
            ReactionOperation::Unspecified as i32,
        );

        store.upsert_slack_event_handler(handler.clone()).unwrap();

        let local_config_store = Arc::new(RwLock::new(store));
        let mut cache_inner = SlackHandlerCache::new();
        cache_inner.compile_and_cache_patterns(&handler).unwrap();
        let cache = Arc::new(RwLock::new(cache_inner));

        let matcher = EventMatcher::new(local_config_store, cache);

        // Test matching
        assert!(matcher.check_message_conditions(&handler, "deploy to production"));
        assert!(!matcher.check_message_conditions(&handler, "rollback deploy"));
    }

    #[test]
    fn test_reaction_names_matching() {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let mut cache = SlackHandlerCache::new();
        let handler = create_test_handler(
            1,
            "approval_handler",
            true,
            None,
            None,
            false,
            Some("thumbsup,heart".to_string()),
            ReactionOperation::Unspecified as i32,
        );

        cache.compile_and_cache_patterns(&handler).unwrap();
        let cache_arc = Arc::new(RwLock::new(cache));

        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));

        let matcher = EventMatcher::new(local_config_store, cache_arc);

        assert!(matcher.check_reaction_conditions(&handler, "added", "thumbsup", "U123"));
        assert!(matcher.check_reaction_conditions(&handler, "added", "heart", "U123"));
        assert!(!matcher.check_reaction_conditions(&handler, "added", "smile", "U123"));
    }

    #[test]
    fn test_reaction_operation_filter() {
        use proto::jobworkerp_conductor::data::ReactionOperation;

        let cache = Arc::new(RwLock::new(SlackHandlerCache::new()));
        let store = LocalConfigStore::default();
        let local_config_store = Arc::new(RwLock::new(store));

        let matcher = EventMatcher::new(local_config_store, cache);

        let handler_added = create_test_handler(
            1,
            "added_only",
            true,
            None,
            None,
            false,
            None,
            ReactionOperation::Added as i32,
        );

        assert!(matcher.check_reaction_conditions(&handler_added, "added", "smile", "U123"));
        assert!(!matcher.check_reaction_conditions(&handler_added, "removed", "smile", "U123"));
    }
}
