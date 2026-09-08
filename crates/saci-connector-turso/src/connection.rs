//! Opening the database, embedded or synced.
//!
//! The two halves of the `turso` crate expose the same [`turso::Connection`]
//! but different builders and different `connect()` signatures: an embedded
//! [`turso::Database::connect`] is synchronous, while a synced
//! [`turso::sync::Database::connect`] is asynchronous because the first connect
//! may bootstrap the local replica. [`Db`] hides that split behind one open and
//! one connect.
//!
//! Nothing here ever logs a remote token or an encryption key; an error names
//! the embedded path or the remote URL's host instead.

use std::time::Duration;

use saci_core::error::SaciError;

use crate::config::ConnectionConfig;

/// A live database handle.
pub(crate) enum Db {
    /// An embedded local database.
    Local(turso::Database),
    /// An embedded replica kept in sync with a remote endpoint.
    Synced(turso::sync::Database),
}

impl Db {
    /// Open the database named by `config`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the target when the file or the
    /// remote endpoint cannot be opened.
    pub(crate) async fn open(config: &ConnectionConfig, what: &str) -> Result<Self, SaciError> {
        let target = display_target(config);
        match &config.remote {
            None => {
                let mut builder = turso::Builder::new_local(&config.path);
                if let Some(encryption) = &config.encryption {
                    // Both the feature token and the options are required: the
                    // first turns the engine's `encryption` feature on, the
                    // second carries the cipher and key.
                    builder = builder.experimental_encryption(true).with_encryption(
                        turso::EncryptionOpts {
                            cipher: encryption.cipher.trim().to_ascii_lowercase(),
                            hexkey: encryption.hexkey.trim().to_string(),
                        },
                    );
                }
                builder
                    .build()
                    .await
                    .map(Db::Local)
                    .map_err(|e| open_error(what, &target, e))
            }
            Some(remote) => {
                let mut builder = turso::sync::Builder::new_remote(&config.path)
                    .with_remote_url(normalize_url(&remote.url))
                    .with_auth_token(&remote.token)
                    .bootstrap_if_empty(remote.bootstrap_if_empty);
                if let Some(timeout_ms) = remote.long_poll_timeout_ms {
                    builder = builder.with_long_poll_timeout(Duration::from_millis(timeout_ms));
                }
                if let Some(enabled) = remote.logical_mvcc_pull {
                    builder = builder.with_logical_mvcc_pull(enabled);
                }
                builder
                    .build()
                    .await
                    .map(Db::Synced)
                    .map_err(|e| open_error(what, &target, e))
            }
        }
    }

    /// Open a connection to the already-open database.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the target when the connection
    /// cannot be established.
    pub(crate) async fn connect(
        &self,
        config: &ConnectionConfig,
        what: &str,
    ) -> Result<turso::Connection, SaciError> {
        let target = display_target(config);
        match self {
            Db::Local(db) => db.connect(),
            Db::Synced(db) => db.connect().await,
        }
        .map_err(|e| SaciError::generic(format!("{what}: connecting to '{target}': {e}")))
    }

    /// Pull remote changes into the local replica, a no-op when embedded.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the target when the pull fails.
    pub(crate) async fn pull(
        &self,
        config: &ConnectionConfig,
        what: &str,
    ) -> Result<(), SaciError> {
        match self {
            Db::Local(_) => Ok(()),
            Db::Synced(db) => db.pull().await.map(|_| ()).map_err(|e| {
                SaciError::generic(format!(
                    "{what}: pulling changes from '{}': {e}",
                    display_target(config)
                ))
            }),
        }
    }

    /// Push local changes to the remote, a no-op when embedded.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the target when the push fails.
    pub(crate) async fn push(
        &self,
        config: &ConnectionConfig,
        what: &str,
    ) -> Result<(), SaciError> {
        match self {
            Db::Local(_) => Ok(()),
            Db::Synced(db) => db.push().await.map_err(|e| {
                SaciError::generic(format!(
                    "{what}: pushing changes to '{}': {e}",
                    display_target(config)
                ))
            }),
        }
    }
}

/// The target an error may name: the embedded path, or the remote URL's host.
///
/// The remote token and the encryption key live in separate fields and are never
/// part of this string.
pub(crate) fn display_target(config: &ConnectionConfig) -> String {
    match &config.remote {
        None => config.path.clone(),
        Some(remote) => remote
            .url
            .split_once("://")
            .map(|(_, rest)| rest.split('/').next().unwrap_or(rest).to_string())
            .unwrap_or_else(|| remote.url.clone()),
    }
}

/// The endpoint URL the synced builder accepts.
///
/// The engine takes only `http://` and `https://` but Turso hands out
/// `turso://` and `libsql://` hostnames, so both are rewritten to `https://`.
pub(crate) fn normalize_url(url: &str) -> String {
    let trimmed = url.trim();
    for alias in ["turso://", "libsql://"] {
        if let Some(rest) = trimmed.strip_prefix(alias) {
            return format!("https://{rest}");
        }
    }
    trimmed.to_string()
}

/// Wrap a `turso` error from an open.
fn open_error(what: &str, target: &str, error: turso::Error) -> SaciError {
    SaciError::generic(format!("{what}: opening '{target}': {error}"))
}
