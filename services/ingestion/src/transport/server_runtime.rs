use super::*;

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
    let vector_index_persistence = runtime.vector_index_persistence();
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
    let (flush_shutdown_tx, flush_shutdown_rx) = mpsc::channel::<()>();
    let (segment_shutdown_tx, segment_shutdown_rx) = mpsc::channel::<()>();
    let (replication_shutdown_tx, replication_shutdown_rx) = mpsc::channel::<()>();

    let result = std::thread::scope(|scope| {
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
        if let Some(persistence) = vector_index_persistence.clone() {
            // The runtime lock is held only to take the (copy-on-write)
            // snapshot; the file is written without it.
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                persistence.run(
                    || runtime.lock().ok()?.vector_index_snapshot(),
                    log_vector_index_save,
                );
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

        let hooks = Arc::new(IngestionHooks {
            metrics: Arc::clone(&backpressure_metrics),
            shutdown_txs: vec![
                flush_shutdown_tx.clone(),
                segment_shutdown_tx.clone(),
                replication_shutdown_tx.clone(),
            ],
            vector_index_persistence: vector_index_persistence.clone(),
        });
        let handler_runtime = Arc::clone(&runtime);
        let handler: dash_http::Handler = Arc::new(move |request| {
            handle_request(&handler_runtime, &HttpRequest::from(request)).into()
        });
        let result = dash_http::serve(
            listener,
            server_config(worker_count, queue_capacity),
            handler,
            dash_http::default_health_classifier,
            &|| shutdown.is_triggered(),
            hooks,
        );
        // `serve` signals these on shutdown; also covers an early error.
        let _ = flush_shutdown_tx.send(());
        let _ = segment_shutdown_tx.send(());
        let _ = replication_shutdown_tx.send(());
        if let Some(persistence) = vector_index_persistence.as_ref() {
            persistence.stop();
        }
        result
    });
    // Every worker and background thread has exited: save the final state
    // so the next start loads it instead of rebuilding.
    if let Some(persistence) = vector_index_persistence {
        let snapshot = runtime
            .lock()
            .ok()
            .and_then(|mut guard| guard.vector_index_snapshot());
        if let Some(snapshot) = snapshot {
            log_vector_index_save(persistence.save_if_changed(&snapshot));
        }
    }
    result
}

/// Feeds the transport backpressure metrics and, when the accept loop stops,
/// tells the background maintenance threads to exit.
struct IngestionHooks {
    metrics: Arc<TransportBackpressureMetrics>,
    shutdown_txs: Vec<mpsc::Sender<()>>,
    vector_index_persistence: Option<Arc<VectorIndexPersistence>>,
}

impl dash_http::ServerHooks for IngestionHooks {
    fn on_enqueued(&self) {
        self.metrics.observe_enqueued();
    }

    fn on_dequeued(&self) {
        self.metrics.observe_dequeued();
    }

    fn on_reject(&self, _reason: dash_http::RejectReason) {
        self.metrics.observe_rejected();
    }

    fn on_read_error(&self, status: u16) {
        self.metrics.observe_read_error(status);
    }

    fn on_shutdown(&self) {
        for tx in &self.shutdown_txs {
            let _ = tx.send(());
        }
        if let Some(persistence) = self.vector_index_persistence.as_ref() {
            persistence.stop();
        }
    }
}
