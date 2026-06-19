use futures_util::StreamExt;
use iroh::endpoint::{Connection, PathEvent};
use std::fmt;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    Relay,
    DirectIp,
    CustomOrUnknown,
}

impl fmt::Display for PathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Relay => f.write_str("relay"),
            Self::DirectIp => f.write_str("direct_ip"),
            Self::CustomOrUnknown => f.write_str("custom_or_unknown"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathSummary {
    pub path_id: String,
    pub remote_addr: String,
    pub local_addr: String,
    pub kind: PathKind,
    pub selected: bool,
    pub rtt_ms: u64,
}

pub fn classify_path_kind(is_relay: bool, is_ip: bool) -> PathKind {
    if is_relay {
        PathKind::Relay
    } else if is_ip {
        PathKind::DirectIp
    } else {
        PathKind::CustomOrUnknown
    }
}

pub fn selected_path(paths: &[PathSummary]) -> Option<&PathSummary> {
    paths.iter().find(|path| path.selected)
}

pub fn collect_path_summaries(connection: &Connection) -> Vec<PathSummary> {
    connection
        .paths()
        .iter()
        .map(|path| PathSummary {
            path_id: format!("{:?}", path.id()),
            remote_addr: format!("{:?}", path.remote_addr()),
            local_addr: format!("{:?}", path.local_addr()),
            kind: classify_path_kind(path.is_relay(), path.is_ip()),
            selected: path.is_selected(),
            rtt_ms: duration_millis_u64(path.rtt()),
        })
        .collect()
}

pub fn log_path_snapshot(role: &'static str, connection: &Connection) {
    let remote = connection.remote_id().to_string();
    let paths = collect_path_summaries(connection);

    if paths.is_empty() {
        debug!(
            event = "iroh_path_snapshot_empty",
            role,
            iroh_remote = %remote,
            "iroh path snapshot has no open paths yet"
        );
        return;
    }

    for path in paths {
        let path_kind = path.kind.to_string();
        info!(
            event = "iroh_path_snapshot",
            role,
            iroh_remote = %remote,
            path_id = %path.path_id,
            path_remote = %path.remote_addr,
            path_local = %path.local_addr,
            path_kind = %path_kind,
            selected = path.selected,
            rtt_ms = path.rtt_ms,
            "iroh path snapshot"
        );
    }
}

pub fn log_connection_route(
    role: &'static str,
    connection: &Connection,
    socks_peer: Option<SocketAddr>,
    target: Option<&str>,
) {
    let remote = connection.remote_id().to_string();
    let paths = collect_path_summaries(connection);
    let selected = selected_path(&paths);
    let socks_peer = socks_peer
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let target = target.unwrap_or("unknown");

    match selected {
        Some(path) => {
            let path_kind = path.kind.to_string();
            info!(
                event = "connection_route",
                role,
                iroh_remote = %remote,
                socks_peer = %socks_peer,
                target = %target,
                selected_path_id = %path.path_id,
                selected_path_kind = %path_kind,
                selected_path_remote = %path.remote_addr,
                selected_path_local = %path.local_addr,
                selected_path_rtt_ms = path.rtt_ms,
                "connection route"
            );
        }
        None => {
            warn!(
                event = "connection_route",
                role,
                iroh_remote = %remote,
                socks_peer = %socks_peer,
                target = %target,
                selected_path_kind = "unknown",
                "connection route has no selected iroh path yet"
            );
        }
    }
}

pub fn spawn_path_event_logger(role: &'static str, connection: &Connection) -> JoinHandle<()> {
    let remote = connection.remote_id().to_string();
    let mut events = connection.path_events();
    log_path_snapshot(role, connection);

    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            log_path_event(role, &remote, event);
        }
        debug!(
            event = "iroh_path_events_ended",
            role,
            iroh_remote = %remote,
            "iroh path event stream ended"
        );
    })
}

fn log_path_event(role: &'static str, remote: &str, event: PathEvent) {
    match event {
        PathEvent::Opened {
            id,
            remote_addr,
            local_addr,
            ..
        } => {
            let kind = classify_path_kind(remote_addr.is_relay(), remote_addr.is_ip());
            let path_kind = kind.to_string();
            info!(
                event = "iroh_path_opened",
                role,
                iroh_remote = %remote,
                path_id = ?id,
                path_remote = ?remote_addr,
                path_local = ?local_addr,
                path_kind = %path_kind,
                "iroh path opened"
            );
        }
        PathEvent::Selected {
            id,
            remote_addr,
            local_addr,
            ..
        } => {
            let kind = classify_path_kind(remote_addr.is_relay(), remote_addr.is_ip());
            let path_kind = kind.to_string();
            info!(
                event = "iroh_path_selected",
                role,
                iroh_remote = %remote,
                path_id = ?id,
                path_remote = ?remote_addr,
                path_local = ?local_addr,
                path_kind = %path_kind,
                "iroh path selected"
            );
        }
        PathEvent::Closed {
            id,
            remote_addr,
            local_addr,
            last_stats,
            ..
        } => {
            let kind = classify_path_kind(remote_addr.is_relay(), remote_addr.is_ip());
            let path_kind = kind.to_string();
            info!(
                event = "iroh_path_closed",
                role,
                iroh_remote = %remote,
                path_id = ?id,
                path_remote = ?remote_addr,
                path_local = ?local_addr,
                path_kind = %path_kind,
                rtt_ms = duration_millis_u64(last_stats.rtt),
                tx_datagrams = last_stats.udp_tx.datagrams,
                tx_bytes = last_stats.udp_tx.bytes,
                rx_datagrams = last_stats.udp_rx.datagrams,
                rx_bytes = last_stats.udp_rx.bytes,
                "iroh path closed"
            );
        }
        PathEvent::Lagged { missed, .. } => {
            warn!(
                event = "iroh_path_events_lagged",
                role,
                iroh_remote = %remote,
                missed,
                "iroh path event logger lagged"
            );
        }
        _ => {
            debug!(
                event = "iroh_path_event_unknown",
                role,
                iroh_remote = %remote,
                "unknown iroh path event"
            );
        }
    }
}

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
