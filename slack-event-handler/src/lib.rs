// Slack Event Handler Library
// Dynamic Slack event handling for jobworkerp UI event handler

pub mod config_cache;
pub mod event_matcher;
pub mod handler_executor;
pub mod manager;
pub mod socket_mode;

// Re-export main types
pub use manager::DynamicSlackHandlerManager;
pub use socket_mode::SocketModeListener;

// Re-export main types
pub use config_cache::{CacheStats, SlackHandlerCache};
pub use event_matcher::{EventMatcher, EventType};
pub use handler_executor::HandlerExecutor;
