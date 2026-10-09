use super::*;
use dash_common::conn::{Conn, ConnConfig, ConnFrontend, Lane, PENDING_POLL_INTERVAL, Rejected};

pub(super) fn serve_http_with_workers(
    runtime: IngestionRuntime,
    bind_addr: &str,
    worker_count: usize,
    shutdown: std::sync::Arc<dash_common::ShutdownSignal>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    let worker_count = worker_count.max(1);
    let queue_capacity = resolve_http_queue_capacity(worker_count);
    let wal_async_flush_interval = runtime.wal_async_flush_interval();
    let segment_maintenance_interval = runtime.segment_maintenance_interval();
    let replication_pull = ReplicationPullConfig::from_env();
    let runtime = Arc::new(Mutex::new(runtime));
    if let Some(config) = replication_pull.as_ref()
        && let Ok(mut guard) = runtime.lock()
    {
        guard.enable_replication_follower(config);
    }
    let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
    if let Ok(mut guard) = runtime.lock() {
        guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
    }
    let (tx, rx) = mpsc::sync_channel::<Conn>(queue_capacity);
    let rx = Arc::new(Mutex::new(rx));
    // Reserved lane for health-class requests so slow work can never starve
    // liveness/readiness probes.
    let (health_tx, health_rx) =
        mpsc::sync_channel::<Conn>(queue_capacity.min(HEALTH_QUEUE_CAPACITY));
    let health_rx = Arc::new(Mutex::new(health_rx));
    let request_timeout = resolve_request_timeout();
    let (flush_shutdown_tx, flush_shutdown_rx) = mpsc::channel::<()>();
    let (segment_shutdown_tx, segment_shutdown_rx) = mpsc::channel::<()>();
    let (replication_shutdown_tx, replication_shutdown_rx) = mpsc::channel::<()>();

    std::thread::scope(|scope| {
        if let Some(async_interval) = wal_async_flush_interval {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                loop {
                    match flush_shutdown_rx.recv_timeout(async_interval) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let Ok(mut guard) = runtime.lock() else {
                        break;
                    };
                    guard.flush_wal_for_async_tick();
                    drop(guard);
                    refresh_placement(&runtime);
                }
            });
        }
        if let Some(maintenance_interval) = segment_maintenance_interval {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                loop {
                    match segment_shutdown_rx.recv_timeout(maintenance_interval) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let Ok(mut guard) = runtime.lock() else {
                        break;
                    };
                    guard.run_segment_maintenance_tick();
                    drop(guard);
                    refresh_placement(&runtime);
                }
            });
        }
        if let Some(replication_pull) = replication_pull.clone() {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                // Back off (up to the configured cap) while pulls keep
                // failing instead of hammering a dead or misbehaving leader.
                let mut delay = replication_pull.poll_interval;
                loop {
                    match replication_shutdown_rx.recv_timeout(delay) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let failures = run_replication_pull_tick(&runtime, &replication_pull);
                    delay = replication_pull.backoff_delay(failures);
                }
            });
        }

        for lane_rx in std::iter::repeat_n(&rx, worker_count)
            .chain(std::iter::repeat_n(&health_rx, HEALTH_WORKERS))
        {
            let runtime = Arc::clone(&runtime);
            let rx = Arc::clone(lane_rx);
            let backpressure_metrics = Arc::clone(&backpressure_metrics);
            scope.spawn(move || {
                loop {
                    let mut conn = {
                        let guard = match rx.lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };
                        match guard.recv() {
                            Ok(conn) => {
                                backpressure_metrics.observe_dequeued();
                                conn
                            }
                            Err(_) => break,
                        }
                    };
                    // A connection that already waited out its request
                    // deadline in the queue is closed without any work.
                    if conn.is_stale(request_timeout) {
                        continue;
                    }
                    let deadline = conn.deadline(request_timeout);
                    if let Err(err) = handle_connection(&runtime, &mut conn.stream, deadline) {
                        eprintln!("ingestion transport error: {err}");
                    }
                }
            });
        }

        listener
            .set_nonblocking(true)
            .expect("set listener non-blocking");
        let mut accept_error_streak: u32 = 0;
        let mut frontend = ConnFrontend::new(ConnConfig::from_env());
        let mut ready: Vec<(Lane, Conn)> = Vec::new();
        loop {
            if shutdown.is_triggered() {
                eprintln!("ingestion: shutdown signal received, draining in-flight requests");
                break;
            }
            if frontend.has_pending() && frontend.poll_due() {
                frontend.poll(&mut ready);
                for (lane, conn) in ready.drain(..) {
                    backpressure_metrics.observe_enqueued();
                    let target = if lane == Lane::Health {
                        &health_tx
                    } else {
                        &tx
                    };
                    match target.try_send(conn) {
                        Ok(()) => {}
                        Err(mpsc::TrySendError::Full(conn)) => {
                            backpressure_metrics.observe_dequeued();
                            backpressure_metrics.observe_rejected();
                            if let Err(err) =
                                write_backpressure_response(conn.stream, SOCKET_TIMEOUT_SECS)
                            {
                                eprintln!(
                                    "ingestion transport backpressure response failed: {err}"
                                );
                            }
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            backpressure_metrics.observe_dequeued();
                            eprintln!("ingestion transport worker queue closed");
                        }
                    }
                }
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    accept_error_streak = 0;
                    if let Err(Rejected(stream)) = frontend.admit(stream) {
                        // Per-IP cap or pending bound exceeded.
                        backpressure_metrics.observe_rejected();
                        if let Err(err) = write_backpressure_response(stream, SOCKET_TIMEOUT_SECS) {
                            eprintln!("ingestion transport backpressure response failed: {err}");
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    let nap = if frontend.has_pending() {
                        PENDING_POLL_INTERVAL
                    } else {
                        Duration::from_millis(50)
                    };
                    std::thread::sleep(nap);
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    // Transient failures (EMFILE, ECONNABORTED, ...) must not
                    // take the server down; back off briefly and keep serving.
                    // Only the shutdown flag ends this loop.
                    accept_error_streak = accept_error_streak.saturating_add(1);
                    eprintln!("ingestion transport accept error: {err}");
                    std::thread::sleep(accept_error_backoff(accept_error_streak));
                    continue;
                }
            }
        }
        let _ = flush_shutdown_tx.send(());
        let _ = segment_shutdown_tx.send(());
        let _ = replication_shutdown_tx.send(());
        drop(tx);
        drop(health_tx);
    });

    Ok(())
}

/// Bounded exponential backoff (10ms .. 100ms) for repeated accept failures.
fn accept_error_backoff(streak: u32) -> Duration {
    let millis = 10u64.saturating_mul(1u64 << streak.saturating_sub(1).min(4));
    Duration::from_millis(millis.min(100))
}

fn handle_connection(
    runtime: &SharedRuntime,
    stream: &mut TcpStream,
    deadline: Instant,
) -> std::io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;

    // The read deadline covers the whole request (headers and body), not
    // each individual read, so slow-trickle clients are dropped. It is
    // measured from accept, so queue wait counts against it.
    let request = match read_http_request_until(stream, deadline) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(err) => {
            return write_response(
                stream,
                HttpResponse::error_with_status(err.status, &err.message),
            );
        }
    };

    let response = handle_request(runtime, &request);
    write_response(stream, response)
}
