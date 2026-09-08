//! Graceful shutdown coordination for the SACI service.
//!
//! [`ShutdownCoordinator`] owns the root [`CancellationToken`] and installs
//! OS signal handlers (SIGINT / SIGTERM). When a signal is received it cancels
//! the root token, which propagates to all child tokens handed to sub-tasks
//! (HTTP server, runners, watchdog, etc.).
//!
//! ## Typical wiring
//!
//! ```rust,no_run
//! # #[cfg(feature = "service")]
//! # {
//! use std::time::Duration;
//! use saci_service::service::shutdown::ShutdownCoordinator;
//!
//! #[tokio::main]
//! async fn main() {
//!     let coord = ShutdownCoordinator::new(Duration::from_secs(30));
//!     let cancel = coord.root();
//!
//!     // Hand child tokens to every sub-task:
//!     // serve_http(&http_cfg, state, coord.child()).await?;
//!
//!     // Block until SIGINT or SIGTERM.
//!     coord.wait_for_signal().await;
//! }
//! # }
//! ```

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Coordinates graceful shutdown across all service sub-tasks.
///
/// Owns the root [`CancellationToken`]. Call [`child`](Self::child) to create
/// scoped child tokens for individual sub-tasks. When [`wait_for_signal`](Self::wait_for_signal)
/// resolves (on SIGINT or SIGTERM) the root token is cancelled, which
/// propagates to all outstanding child tokens automatically.
///
/// After cancellation, call [`drain`](Self::drain) to wait for all tracked
/// [`JoinHandle`]s to finish, subject to the configured budget.
pub struct ShutdownCoordinator {
    root: CancellationToken,
    budget: Duration,
}

impl ShutdownCoordinator {
    /// Create a new coordinator with the given graceful-shutdown budget.
    ///
    /// If tasks do not finish within `budget` after cancellation,
    /// [`drain`](Self::drain) returns `false` and the caller should exit
    /// immediately (e.g. `std::process::exit(1)`).
    pub fn new(budget: Duration) -> Self {
        Self {
            root: CancellationToken::new(),
            budget,
        }
    }

    /// Clone the root cancellation token.
    ///
    /// Cancelling the returned token will also cancel all child tokens.
    /// Useful for passing to components that need to initiate their own
    /// shutdown (e.g. the HTTP server's `with_graceful_shutdown` future).
    pub fn root(&self) -> CancellationToken {
        self.root.clone()
    }

    /// Create a child cancellation token.
    ///
    /// The child is cancelled automatically when the root is cancelled, but
    /// can also be cancelled independently without affecting the root or other
    /// children.
    pub fn child(&self) -> CancellationToken {
        self.root.child_token()
    }

    /// Install OS signal handlers and block until SIGINT or SIGTERM is received.
    ///
    /// On receipt the root token is cancelled and this method returns.
    ///
    /// On non-Unix platforms only SIGINT (Ctrl-C) is handled; SIGTERM is
    /// simulated with a `pending` future that never resolves.
    pub async fn wait_for_signal(self) {
        #[cfg(unix)]
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                match result {
                    Ok(()) => tracing::info!("SIGINT received, initiating graceful shutdown"),
                    Err(e) => tracing::error!("SIGINT handler error: {e}"),
                }
            }
            _ = async {
                #[cfg(unix)]
                { sigterm.recv().await }
                #[cfg(not(unix))]
                { std::future::pending::<()>().await }
            } => {
                tracing::info!("SIGTERM received, initiating graceful shutdown");
            }
        }

        self.root.cancel();
    }

    /// Wait for all tasks to complete within the configured shutdown budget.
    ///
    /// Iterates through `tasks` in order, waiting up to the remaining budget
    /// for each one. If the budget is exceeded before all tasks finish, logs
    /// an error and returns `false`. Returns `true` if every task finished
    /// cleanly.
    ///
    /// Tasks that have already completed are handled without blocking.
    pub async fn drain(&self, mut tasks: Vec<JoinHandle<()>>) -> bool {
        let deadline = tokio::time::Instant::now() + self.budget;

        for task in tasks.drain(..) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                tracing::error!("shutdown budget exceeded, forcing exit");
                return false;
            }
            match tokio::time::timeout(remaining, task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!("task panicked during shutdown: {e}");
                }
                Err(_elapsed) => {
                    tracing::error!("shutdown budget exceeded waiting for a task");
                    return false;
                }
            }
        }

        true
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_root_token_initially_not_cancelled() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        assert!(!coord.root().is_cancelled());
    }

    #[tokio::test]
    async fn test_child_token_cancelled_when_root_is_cancelled() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let child = coord.child();
        assert!(!child.is_cancelled());

        coord.root().cancel();
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn test_drain_all_tasks_complete_within_budget() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));

        let handle1 = tokio::spawn(async { sleep(Duration::from_millis(10)).await });
        let handle2 = tokio::spawn(async { sleep(Duration::from_millis(10)).await });

        let clean = coord.drain(vec![handle1, handle2]).await;
        assert!(clean, "all tasks should complete within budget");
    }

    #[tokio::test]
    async fn test_drain_budget_exceeded_returns_false() {
        let coord = ShutdownCoordinator::new(Duration::from_millis(50));

        // Task that sleeps longer than the budget.
        let handle = tokio::spawn(async { sleep(Duration::from_secs(10)).await });

        let clean = coord.drain(vec![handle]).await;
        assert!(!clean, "budget exceeded should return false");
    }

    #[tokio::test]
    async fn test_drain_empty_task_list_returns_true() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let clean = coord.drain(vec![]).await;
        assert!(clean);
    }

    #[tokio::test]
    async fn test_multiple_children_all_cancelled_on_root_cancel() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let c1 = coord.child();
        let c2 = coord.child();
        let c3 = coord.child();

        coord.root().cancel();

        assert!(c1.is_cancelled());
        assert!(c2.is_cancelled());
        assert!(c3.is_cancelled());
    }

    #[tokio::test]
    async fn test_child_cancel_does_not_affect_root() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let child = coord.child();
        let root = coord.root();

        child.cancel();

        assert!(child.is_cancelled());
        assert!(!root.is_cancelled());
    }

    #[tokio::test]
    async fn test_drain_panicking_task_handled_gracefully() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let handle = tokio::spawn(async { panic!("intentional panic for test") });

        // Give task time to panic.
        sleep(Duration::from_millis(20)).await;

        let clean = coord.drain(vec![handle]).await;
        // A panicking task finishes, so drain returns true (budget not exceeded).
        assert!(clean);
    }

    /// Cancelling a root clone obtained via `coord.root()` propagates to all
    /// child tokens.
    #[tokio::test]
    async fn test_root_clone_cancel_propagates_to_children() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));

        let shutdown_token = coord.root();

        let http_cancel = coord.child();
        let watchdog_cancel = coord.child();

        assert!(!http_cancel.is_cancelled());
        assert!(!watchdog_cancel.is_cancelled());

        shutdown_token.cancel();

        assert!(http_cancel.is_cancelled(), "HTTP child should be cancelled");
        assert!(
            watchdog_cancel.is_cancelled(),
            "watchdog child should be cancelled"
        );
    }

    /// When the runner future completes before a signal arrives, cancelling the
    /// root token must signal all child tasks so drain completes within budget.
    #[tokio::test]
    async fn test_runner_exits_first_drains_without_timeout() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let shutdown_token = coord.root();

        // Two tasks that block until their cancel token fires.
        let http_cancel = coord.child();
        let watchdog_cancel = coord.child();

        let http_handle = tokio::spawn(async move {
            http_cancel.cancelled().await;
        });
        let watchdog_handle = tokio::spawn(async move {
            watchdog_cancel.cancelled().await;
        });

        // Runner completes immediately; no signal arrives.
        let runner_fut = async { Ok::<(), ()>(()) };
        let _runner_result: Result<(), ()> = tokio::select! {
            _ = std::future::pending::<()>() => Ok(()),
            result = runner_fut => result,
        };

        shutdown_token.cancel();
        let drain_coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let clean = drain_coord.drain(vec![http_handle, watchdog_handle]).await;
        assert!(
            clean,
            "drain should complete cleanly — tasks must have received cancellation"
        );
    }
}
