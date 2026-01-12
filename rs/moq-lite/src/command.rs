//! Commands that can be sent to an active session.

use crate::PathOwned;

/// Commands that can be sent to the subscriber side of a session.
#[derive(Debug, Clone)]
pub enum SubscriberCommand {
    /// Manually announce a remote broadcast without waiting for PUBLISH_NAMESPACE.
    /// Use this when you know a broadcast exists on the remote but they won't announce it.
    AnnounceRemote(PathOwned),
}
