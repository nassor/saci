//! The sink's liveness probe: is the peer still there?
//!
//! UDP tells a sender nothing, so a sink that wants to know whether its peer
//! is up has to ask. The probe asks in the only two ways a datagram socket
//! can, and [`ProbeKind`] picks which.
//!
//! `Echo` sends a zero-length datagram and waits for one back. A
//! [`UdpSource`](crate::UdpSource) answers every zero-length datagram with a
//! zero-length datagram to its sender, so this kind gets a positive answer
//! from the peer's own receive loop. Any other peer never answers, so `Echo`
//! reports it down whether it is or not.
//!
//! `Icmp` sends the same datagram and reads only what the OS reports back. A
//! closed port on a reachable host answers with an ICMP port-unreachable, which
//! the OS relays on the connected socket as `ConnectionRefused` or
//! `ConnectionReset`. Silence is the answer for a peer that is up, so `Icmp`
//! cannot tell a live peer from a host whose firewall drops the report, and it
//! reports both as up.
//!
//! Both kinds need a socket connected to exactly one peer, which is why
//! [`UdpSink::connect`](crate::UdpSink::connect) refuses a probe next to
//! `broadcast` or a multicast address.

use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::Instant;

/// How the sink decides whether a peer is up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeKind {
    /// Wait for the peer to send a datagram back. A `udp` source does.
    Echo,
    /// Wait only for the OS to report the peer unreachable. Silence is up.
    Icmp,
}

/// What a failed probe does to the writes that follow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnFailure {
    /// Stop sending to a peer that failed its last probe. A batch no peer
    /// accepted is a write error, which is what the host's `retry`, `heal` and
    /// `dlq` blocks act on.
    Refuse,
    /// Log the failure and keep sending.
    Warn,
}

/// The sink's `probe` block: kind, timing, and what a failure does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeSettings {
    /// How the peer's liveness is decided.
    pub kind: ProbeKind,
    /// How long the probe task waits between rounds.
    pub interval: Duration,
    /// How long one round waits for an answer.
    pub timeout: Duration,
    /// What a failed round does to the writes that follow.
    pub on_failure: OnFailure,
}

/// One peer's state after a probe round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Liveness {
    Up,
    Down(String),
}

/// Whether an error is the OS relaying an ICMP unreachable rather than a
/// local failure.
///
/// Both platforms hold one such report per socket and hand it to the next
/// socket operation, clearing it there. Windows reports it on `recv`, Linux
/// and macOS on `recv` or `send`, and whichever call collects it may be one
/// the report has nothing to do with: a data datagram sent while the peer was
/// away leaves a report that the next probe collects rounds later.
pub(crate) fn is_unreachable_report(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionRefused
    )
}

/// How many queued datagrams one round drains before it stops trying. A peer
/// sending faster than that is answering, which is what the round asks.
const DRAIN_LIMIT: usize = 16;

/// Probe the peer `socket` is connected to and report what came back.
///
/// A round drains the socket, sends, sends once more if that first send
/// collected an unreachable report, then waits out `timeout`. The drain and
/// the second send are both about staleness: nothing else reads this socket,
/// and each OS holds one unreachable report per socket and hands it to the
/// next call, so without them a round could answer with a datagram or a report
/// that an earlier round or an earlier data write earned.
///
/// `buf` receives whatever datagram arrives during the round, so the caller
/// owns one buffer large enough for any datagram: Windows fails a `recv` whose
/// buffer is smaller than the datagram, and a data datagram may well arrive
/// while the probe is waiting.
pub(crate) async fn probe(
    socket: &UdpSocket,
    kind: ProbeKind,
    timeout: Duration,
    buf: &mut [u8],
) -> Liveness {
    // Nothing else reads this socket, so an echo that arrived after its own
    // round's deadline would answer this one. Draining first is what keeps a
    // round's verdict about the round it belongs to.
    for _ in 0..DRAIN_LIMIT {
        match socket.try_recv(buf) {
            Ok(_) => {}
            Err(e) if is_unreachable_report(&e) => {}
            Err(_) => break,
        }
    }

    // A pending report is cleared by the call that collects it, so a first
    // send that fails may be answering a datagram sent rounds ago. The second
    // send is the one that asks about now, and a peer that really is gone
    // fails it again.
    let mut sent = socket.send(&[]).await;
    if let Err(e) = &sent
        && is_unreachable_report(e)
    {
        sent = socket.send(&[]).await;
    }
    if let Err(e) = sent {
        return Liveness::Down(format!("sending the probe failed: {e}"));
    }

    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, socket.recv(buf)).await {
            // Nothing came back within the deadline. For `Echo` that is the
            // failure; for `Icmp` no report of death is the only "alive" it
            // can give.
            Err(_elapsed) => {
                return match kind {
                    ProbeKind::Echo => {
                        Liveness::Down(format!("no echo within {} ms", timeout.as_millis()))
                    }
                    ProbeKind::Icmp => Liveness::Up,
                };
            }
            // Any datagram from the connected peer proves its port is open,
            // whether it is the echo or data.
            Ok(Ok(_)) => return Liveness::Up,
            Ok(Err(e)) if is_unreachable_report(&e) => {
                // The probe datagram above raised this one, since anything
                // older was drained before it went out.
                if kind == ProbeKind::Icmp {
                    return Liveness::Down(format!("the peer reported unreachable: {e}"));
                }
                // `Echo` keeps waiting for the datagram that proves the port
                // open. This runs on a background task, so the extra wait
                // costs the write path nothing, and a peer that really is dead
                // still ends in the timeout arm.
            }
            Ok(Err(e)) => {
                return Liveness::Down(format!("receiving the probe reply failed: {e}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A peer bound on loopback that answers every datagram with a
    /// zero-length one, the way a `udp` source's receive loop does.
    async fn echoing_peer() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
        let addr = socket.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            while let Ok((_n, from)) = socket.recv_from(&mut buf).await {
                let _ = socket.send_to(&[], from).await;
            }
        });
        (addr, task)
    }

    /// A peer that reads and never answers: its port is open and silent.
    async fn silent_peer() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
        let addr = socket.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            while socket.recv_from(&mut buf).await.is_ok() {}
        });
        (addr, task)
    }

    async fn connected_to(peer: std::net::SocketAddr) -> Arc<UdpSocket> {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind prober");
        socket.connect(peer).await.expect("connect");
        Arc::new(socket)
    }

    #[tokio::test]
    async fn echo_kind_is_up_when_the_peer_echoes() {
        let (addr, task) = echoing_peer().await;
        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let liveness = probe(
            &socket,
            ProbeKind::Echo,
            Duration::from_millis(500),
            &mut buf,
        )
        .await;

        task.abort();
        assert_eq!(liveness, Liveness::Up);
    }

    #[tokio::test]
    async fn echo_kind_is_down_when_nothing_answers() {
        let (addr, task) = silent_peer().await;
        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let liveness = probe(
            &socket,
            ProbeKind::Echo,
            Duration::from_millis(200),
            &mut buf,
        )
        .await;

        task.abort();
        match liveness {
            Liveness::Down(reason) => assert!(reason.contains("no echo"), "got: {reason}"),
            Liveness::Up => panic!("a peer that never echoes must be down for the echo kind"),
        }
    }

    /// The ICMP kind's whole purpose: a host that is up with the port closed
    /// answers with a port-unreachable, and the OS relays it on the connected
    /// socket.
    #[tokio::test]
    async fn icmp_kind_is_down_on_a_closed_port() {
        let closed = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = closed.local_addr().expect("local_addr");
        drop(closed);

        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let liveness = probe(
            &socket,
            ProbeKind::Icmp,
            Duration::from_millis(500),
            &mut buf,
        )
        .await;

        match liveness {
            Liveness::Down(reason) => assert!(reason.contains("unreachable"), "got: {reason}"),
            Liveness::Up => panic!("a closed port must report unreachable"),
        }
    }

    /// Silence is the only "alive" ICMP can give, so an open, quiet port is up.
    #[tokio::test]
    async fn icmp_kind_is_up_when_the_port_is_open_and_silent() {
        let (addr, task) = silent_peer().await;
        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let liveness = probe(
            &socket,
            ProbeKind::Icmp,
            Duration::from_millis(200),
            &mut buf,
        )
        .await;

        task.abort();
        assert_eq!(liveness, Liveness::Up);
    }

    /// A peer that answers with a non-empty datagram, not the echo the probe
    /// itself would use: any datagram from the connected peer proves the
    /// port open.
    async fn data_answering_peer() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
        let addr = socket.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            while let Ok((_n, from)) = socket.recv_from(&mut buf).await {
                let _ = socket.send_to(b"x", from).await;
            }
        });
        (addr, task)
    }

    #[tokio::test]
    async fn a_data_datagram_from_the_peer_counts_as_up_for_echo() {
        let (addr, task) = data_answering_peer().await;
        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let liveness = probe(
            &socket,
            ProbeKind::Echo,
            Duration::from_millis(500),
            &mut buf,
        )
        .await;

        task.abort();
        assert_eq!(liveness, Liveness::Up);
    }

    /// `Icmp` returns as soon as any datagram answers, rather than waiting
    /// out the whole timeout the way silence-is-alive does.
    #[tokio::test]
    async fn icmp_kind_is_up_when_the_peer_answers() {
        let (addr, task) = echoing_peer().await;
        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let started = Instant::now();
        let liveness = probe(
            &socket,
            ProbeKind::Icmp,
            Duration::from_millis(500),
            &mut buf,
        )
        .await;
        let elapsed = started.elapsed();

        task.abort();
        assert_eq!(liveness, Liveness::Up);
        assert!(
            elapsed < Duration::from_millis(300),
            "an answering peer must return well before the timeout, took {elapsed:?}"
        );
    }

    /// A peer that answers the first probe only after a 250 ms delay, and
    /// never again.
    async fn late_once_peer() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
        let addr = socket.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            let mut answered = false;
            while let Ok((_n, from)) = socket.recv_from(&mut buf).await {
                if !answered {
                    answered = true;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let _ = socket.send_to(&[], from).await;
                }
            }
        });
        (addr, task)
    }

    /// A round that missed its deadline must not let a late echo, still
    /// sitting in the receive buffer, count toward the next round.
    #[tokio::test]
    async fn a_late_echo_does_not_answer_the_next_round() {
        let (addr, task) = late_once_peer().await;
        let socket = connected_to(addr).await;
        let mut buf = vec![0u8; 65535];

        let first = probe(
            &socket,
            ProbeKind::Echo,
            Duration::from_millis(100),
            &mut buf,
        )
        .await;
        match first {
            Liveness::Down(_) => {}
            Liveness::Up => panic!("a 250 ms echo must miss a 100 ms deadline"),
        }

        // The late echo lands mid-way through this sleep, sitting unread in
        // the socket's receive buffer.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let second = probe(
            &socket,
            ProbeKind::Echo,
            Duration::from_millis(100),
            &mut buf,
        )
        .await;
        task.abort();
        match second {
            Liveness::Down(_) => {}
            Liveness::Up => {
                panic!("a stale echo from the previous round must not answer this one")
            }
        }
    }

    /// A stale ICMP report from an earlier datagram must not mask a peer
    /// that has since come back: the probe's own opening send retries once
    /// rather than declaring the peer down on the first stale failure.
    #[tokio::test]
    async fn a_stale_unreachable_report_does_not_mask_a_peer_that_came_back() {
        let closed = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let port = closed.local_addr().expect("local_addr").port();
        drop(closed);

        let prober = UdpSocket::bind("127.0.0.1:0").await.expect("bind prober");
        prober.connect(("127.0.0.1", port)).await.expect("connect");
        // Arms the stale ICMP report: the port is closed, so this send, or a
        // later socket operation, will surface it.
        let _ = prober.send(b"data").await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");
        let mut task = None;
        for _ in 0..20 {
            if let Ok(socket) = UdpSocket::bind(addr).await {
                task = Some(tokio::spawn(async move {
                    let mut buf = vec![0u8; 1024];
                    while let Ok((_n, from)) = socket.recv_from(&mut buf).await {
                        let _ = socket.send_to(&[], from).await;
                    }
                }));
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let task = task.expect("must be able to rebind the port for the echoing peer");

        let mut buf = vec![0u8; 65535];
        let liveness = probe(
            &prober,
            ProbeKind::Echo,
            Duration::from_millis(500),
            &mut buf,
        )
        .await;

        task.abort();
        assert_eq!(liveness, Liveness::Up);
    }
}
