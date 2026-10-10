//! The placement client against an `https://` control plane served by the
//! shared server with required client certificates.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use metadata_router::{
    PlacementSourceOptions, load_shard_placements_from_control_plane_with_options,
};

struct ControlPlane {
    base: String,
    stop: Arc<AtomicBool>,
    pki: dash_tls_fixtures::TestPki,
}

impl Drop for ControlPlane {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

fn tls_control_plane() -> ControlPlane {
    let pki = dash_tls_fixtures::TestPki::new();
    let mut config = dash_http::ServerConfig::control_plane(2, 8);
    config.tls = Some(
        dash_http::TlsAcceptor::new(dash_http::TlsSettings {
            cert_file: pki.server_cert.clone(),
            key_file: pki.server_key.clone(),
            client_ca_file: Some(pki.client_ca.clone()),
            require_client_cert: true,
        })
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let handler: dash_http::Handler = Arc::new(|request: dash_http::Request| {
        if request.path() != "/v1/control-plane/placement"
            || request.header("authorization") != Some("Bearer cp-token")
        {
            return dash_http::Response::error(403, "forbidden");
        }
        dash_http::Response::new(
            200,
            "text/csv",
            "tenant-a,0,3,node-1,leader,healthy\n".to_string(),
        )
        .with_header("x-dash-leader", "true")
    });
    {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            dash_http::serve(
                listener,
                config,
                handler,
                |_, _| false,
                &|| stop.load(Ordering::SeqCst),
                Arc::new(dash_http::NoHooks),
            )
        });
    }
    ControlPlane {
        base: format!("https://127.0.0.1:{port}"),
        stop,
        pki,
    }
}

fn options(cp: &ControlPlane, identity: bool) -> PlacementSourceOptions {
    PlacementSourceOptions {
        bearer_token: Some("cp-token".to_string()),
        ca_file: Some(cp.pki.server_ca.clone()),
        client_cert_file: identity.then(|| cp.pki.client_cert.clone()),
        client_key_file: identity.then(|| cp.pki.client_key.clone()),
        ..PlacementSourceOptions::default()
    }
}

#[test]
fn placement_is_fetched_over_mutual_tls() {
    let cp = tls_control_plane();
    let placements =
        load_shard_placements_from_control_plane_with_options(&cp.base, &options(&cp, true))
            .expect("placement over mTLS");
    assert_eq!(placements.len(), 1);
    assert_eq!(placements[0].tenant_id, "tenant-a");
    assert_eq!(placements[0].epoch, 3);
}

#[test]
fn placement_client_without_a_certificate_or_trust_is_refused() {
    let cp = tls_control_plane();
    assert!(
        load_shard_placements_from_control_plane_with_options(&cp.base, &options(&cp, false))
            .is_err(),
        "the control plane requires a client certificate"
    );
    let untrusting = PlacementSourceOptions {
        ca_file: None,
        ..options(&cp, true)
    };
    let err =
        load_shard_placements_from_control_plane_with_options(&cp.base, &untrusting).unwrap_err();
    assert!(err.to_ascii_lowercase().contains("certificate"), "{err}");

    let half = PlacementSourceOptions {
        client_key_file: None,
        ..options(&cp, true)
    };
    let err = load_shard_placements_from_control_plane_with_options(&cp.base, &half).unwrap_err();
    assert!(
        err.contains("DASH_ROUTER_CONTROL_PLANE_CLIENT_KEY_FILE"),
        "{err}"
    );
}
