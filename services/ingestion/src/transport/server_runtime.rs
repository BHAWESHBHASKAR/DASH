use super::*;

pub(super) fn serve_http_with_workers(
    runtime: IngestionRuntime,
    bind_addr: &str,
    worker_count: usize,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    let worker_count = worker_count.max(1);
    let queue_capacity = resolve_http_queue_capacity(worker_count);
    let wal_async_flush_interval = runtime.wal_async_flush_interval();
    let segment_maintenance_interval = runtime.segment_maintenance_interval();
    let replication_pull = ReplicationPullConfig::from_env();
    let runtime = Arc::new(Mutex::new(runtime));
    let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
    if let Ok(mut guard) = runtime.lock() {
        guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
    }
    let (tx, rx) = mpsc::sync_channel::<TcpStream>(queue_capacity);
    let rx = Arc::new(Mutex::new(rx));
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
                    guard.refresh_placement_if_due();
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
                    guard.refresh_placement_if_due();
                }
            });
        }
        if let Some(replication_pull) = replication_pull.clone() {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                loop {
                    match replication_shutdown_rx.recv_timeout(replication_pull.poll_interval) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    run_replication_pull_tick(&runtime, &replication_pull);
                }
            });
        }

        for _ in 0..worker_count {
            let runtime = Arc::clone(&runtime);
            let rx = Arc::clone(&rx);
            let backpressure_metrics = Arc::clone(&backpressure_metrics);
            scope.spawn(move || {
                loop {
                    let stream = {
                        let guard = match rx.lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };
                        match guard.recv() {
                            Ok(stream) => {
                                backpressure_metrics.observe_dequeued();
                                stream
                            }
                            Err(_) => break,
                        }
                    };
                    if let Err(err) = handle_connection(&runtime, stream) {
                        eprintln!("ingestion transport error: {err}");
                    }
                }
            });
        }

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    backpressure_metrics.observe_enqueued();
                    match tx.try_send(stream) {
                        Ok(()) => {}
                        Err(mpsc::TrySendError::Full(stream)) => {
                            backpressure_metrics.observe_dequeued();
                            backpressure_metrics.observe_rejected();
                            if let Err(err) =
                                write_backpressure_response(stream, SOCKET_TIMEOUT_SECS)
                            {
                                eprintln!(
                                    "ingestion transport backpressure response failed: {err}"
                                );
                            }
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            backpressure_metrics.observe_dequeued();
                            eprintln!("ingestion transport worker queue closed");
                            break;
                        }
                    }
                }
                Err(err) => eprintln!("ingestion transport accept error: {err}"),
            }
        }
        let _ = flush_shutdown_tx.send(());
        let _ = segment_shutdown_tx.send(());
        let _ = replication_shutdown_tx.send(());
        drop(tx);
    });

    Ok(())
}

fn handle_connection(runtime: &SharedRuntime, mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;
    stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;

    let request = match read_http_request(&mut stream) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(err) => return write_response(&mut stream, HttpResponse::bad_request(&err)),
    };

    let response = handle_request(runtime, &request);
    write_response(&mut stream, response)
}
