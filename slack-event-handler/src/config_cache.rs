// Slack Handler Cache
// Manages heavyweight derived data (compiled regex, parsed reaction names)
// Separate from LocalConfigStore which handles core configuration synchronization

use anyhow::{Context, Result};
use proto::jobworkerp_conductor::data::{SlackEventHandler, SlackEventHandlerId};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

/// Slack Handler Cache
/// Manages performance-critical cached data for Slack event handlers
///
/// Design Philosophy:
/// - LocalConfigStore: Core configuration only (SlackEventHandler entities)
/// - SlackHandlerCache: Derived data only (compiled regex, parsed data)
///
/// Cache Contents (Phase 1):
/// 1. Compiled regex patterns (keyed by typed ID)
/// 2. Pre-parsed reaction_names (keyed by typed ID)
///
/// Cache Management:
/// - Explicit invalidation on handler deletion/update
/// - Lazy initialization (compile/parse on first use)
/// - Phase 2: Consider TTL management with moka library
#[derive(Debug, Clone)]
pub struct SlackHandlerCache {
    // Compiled regex cache (keyed by SlackEventHandlerId)
    message_pattern_cache: HashMap<i64, Arc<Regex>>,
    reaction_user_filter_cache: HashMap<i64, Arc<Regex>>,

    // Pre-parsed reaction_names cache (keyed by SlackEventHandlerId)
    // Avoid splitting comma-separated string on every event
    reaction_names_cache: HashMap<i64, Arc<HashSet<String>>>,

    // Cache metadata
    last_update: SystemTime,
    version: u64,
}

impl Default for SlackHandlerCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SlackHandlerCache {
    /// Create new empty cache
    pub fn new() -> Self {
        Self {
            message_pattern_cache: HashMap::new(),
            reaction_user_filter_cache: HashMap::new(),
            reaction_names_cache: HashMap::new(),
            last_update: SystemTime::now(),
            version: 0,
        }
    }

    /// Get compiled message_pattern regex
    pub fn get_message_pattern(&self, handler_id: &SlackEventHandlerId) -> Option<Arc<Regex>> {
        self.message_pattern_cache.get(&handler_id.value).cloned()
    }

    /// Get compiled reaction_user_filter regex
    pub fn get_reaction_user_filter(&self, handler_id: &SlackEventHandlerId) -> Option<Arc<Regex>> {
        self.reaction_user_filter_cache
            .get(&handler_id.value)
            .cloned()
    }

    /// Get parsed reaction_names set
    pub fn get_reaction_names(
        &self,
        handler_id: &SlackEventHandlerId,
    ) -> Option<Arc<HashSet<String>>> {
        self.reaction_names_cache.get(&handler_id.value).cloned()
    }

    /// Compile and cache patterns for a handler
    /// Returns error if regex compilation fails
    pub fn compile_and_cache_patterns(&mut self, handler: &SlackEventHandler) -> Result<()> {
        let handler_id = handler.id.as_ref().context("Handler must have ID")?.value;

        let data = handler.data.as_ref().context("Handler must have data")?;

        // Compile message_pattern if present
        if let Some(pattern) = &data.message_pattern {
            let regex = Regex::new(pattern)
                .with_context(|| format!("Failed to compile message_pattern: {}", pattern))?;
            self.message_pattern_cache
                .insert(handler_id, Arc::new(regex));
            tracing::debug!(
                "Cached message_pattern for handler {} (id={})",
                data.name,
                handler_id
            );
        }

        // Compile reaction_user_filter if present
        if let Some(filter) = &data.reaction_user_filter {
            let regex = Regex::new(filter)
                .with_context(|| format!("Failed to compile reaction_user_filter: {}", filter))?;
            self.reaction_user_filter_cache
                .insert(handler_id, Arc::new(regex));
            tracing::debug!(
                "Cached reaction_user_filter for handler {} (id={})",
                data.name,
                handler_id
            );
        }

        // Parse reaction_names if present
        if let Some(names) = &data.reaction_names {
            let parsed: HashSet<String> = names
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            self.reaction_names_cache
                .insert(handler_id, Arc::new(parsed));
            tracing::debug!(
                "Cached reaction_names for handler {} (id={}): {} reactions",
                data.name,
                handler_id,
                names
            );
        }

        self.last_update = SystemTime::now();
        self.version += 1;

        Ok(())
    }

    /// Remove all cached data for a handler
    pub fn invalidate_handler(&mut self, handler_id: &SlackEventHandlerId) {
        let id = handler_id.value;
        let mut removed_count = 0;

        if self.message_pattern_cache.remove(&id).is_some() {
            removed_count += 1;
        }
        if self.reaction_user_filter_cache.remove(&id).is_some() {
            removed_count += 1;
        }
        if self.reaction_names_cache.remove(&id).is_some() {
            removed_count += 1;
        }

        if removed_count > 0 {
            tracing::debug!(
                "Invalidated {} cache entries for handler id={}",
                removed_count,
                id
            );
            self.last_update = SystemTime::now();
            self.version += 1;
        }
    }

    /// Clear all cache
    pub fn clear(&mut self) {
        self.message_pattern_cache.clear();
        self.reaction_user_filter_cache.clear();
        self.reaction_names_cache.clear();
        self.last_update = SystemTime::now();
        self.version += 1;
        tracing::info!("Cleared all Slack handler cache");
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            message_pattern_count: self.message_pattern_cache.len(),
            reaction_user_filter_count: self.reaction_user_filter_cache.len(),
            reaction_names_count: self.reaction_names_cache.len(),
            version: self.version,
            last_update: self.last_update,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub message_pattern_count: usize,
    pub reaction_user_filter_count: usize,
    pub reaction_names_count: usize,
    pub version: u64,
    pub last_update: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::jobworkerp_conductor::data::{SlackEventHandlerData, SlackEventHandlerId};

    fn create_test_handler(
        id: i64,
        message_pattern: Option<String>,
        reaction_names: Option<String>,
        reaction_user_filter: Option<String>,
    ) -> SlackEventHandler {
        SlackEventHandler {
            id: Some(SlackEventHandlerId { value: id }),
            data: Some(SlackEventHandlerData {
                name: format!("test_handler_{}", id),
                message_pattern,
                reaction_names,
                reaction_user_filter,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_cache_message_pattern() {
        let mut cache = SlackHandlerCache::new();
        let handler = create_test_handler(1, Some("^deploy".to_string()), None, None);

        cache.compile_and_cache_patterns(&handler).unwrap();

        let id = SlackEventHandlerId { value: 1 };
        let regex = cache.get_message_pattern(&id).unwrap();
        assert!(regex.is_match("deploy to production"));
        assert!(!regex.is_match("rollback"));
    }

    #[test]
    fn test_cache_reaction_names() {
        let mut cache = SlackHandlerCache::new();
        let handler = create_test_handler(2, None, Some("thumbsup,heart,rocket".to_string()), None);

        cache.compile_and_cache_patterns(&handler).unwrap();

        let id = SlackEventHandlerId { value: 2 };
        let names = cache.get_reaction_names(&id).unwrap();
        assert_eq!(names.len(), 3);
        assert!(names.contains("thumbsup"));
        assert!(names.contains("heart"));
        assert!(names.contains("rocket"));
        assert!(!names.contains("smile"));
    }

    #[test]
    fn test_invalidate_handler() {
        let mut cache = SlackHandlerCache::new();
        let handler =
            create_test_handler(3, Some("test".to_string()), Some("smile".to_string()), None);

        cache.compile_and_cache_patterns(&handler).unwrap();

        let id = SlackEventHandlerId { value: 3 };
        assert!(cache.get_message_pattern(&id).is_some());
        assert!(cache.get_reaction_names(&id).is_some());

        cache.invalidate_handler(&id);
        assert!(cache.get_message_pattern(&id).is_none());
        assert!(cache.get_reaction_names(&id).is_none());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = SlackHandlerCache::new();

        let handler1 = create_test_handler(1, Some("pattern1".to_string()), None, None);
        let handler2 = create_test_handler(2, None, Some("smile,heart".to_string()), None);

        cache.compile_and_cache_patterns(&handler1).unwrap();
        cache.compile_and_cache_patterns(&handler2).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.message_pattern_count, 1);
        assert_eq!(stats.reaction_names_count, 1);
        assert_eq!(stats.version, 2);
    }

    #[test]
    fn test_invalid_regex_pattern() {
        let mut cache = SlackHandlerCache::new();
        let handler = create_test_handler(4, Some("[invalid(".to_string()), None, None);

        let result = cache.compile_and_cache_patterns(&handler);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to compile message_pattern"));
    }
}
