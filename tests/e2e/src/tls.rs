//! A blocking HTTPS client for services started with TLS on, built on rustls
//! directly (the services themselves are not linked in).

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

use crate::http::{Resp, read_response};
use crate::proc::Proc;

#[derive(Clone)]
pub struct TlsClient {
    pub addr: SocketAddr,
    pub config: Arc<rustls::ClientConfig>,
    pub timeout: Duration,
}

impl TlsClient {
    /// Trust only `ca_pem`; present `identity` (certificate, key) if given.
    pub fn new(addr: SocketAddr, ca_pem: &Path, identity: Option<(&Path, &Path)>) -> Self {
        let mut roots = rustls::RootCertStore::empty();
        let pem = std::fs::read(ca_pem).expect("read CA");
        for cert in CertificateDer::pem_slice_iter(&pem) {
            roots.add(cert.expect("CA PEM")).expect("add CA");
        }
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots);
        let mut config = match identity {
            None => builder.with_no_client_auth(),
            Some((cert, key)) => {
                let chain = CertificateDer::pem_file_iter(cert)
                    .expect("client cert")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("client cert PEM");
                let key = PrivateKeyDer::from_pem_file(key).expect("client key");
                builder
                    .with_client_auth_cert(chain, key)
                    .expect("client identity")
            }
        };
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self {
            addr,
            config: Arc::new(config),
            timeout: Duration::from_secs(20),
        }
    }

    pub fn request(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> std::io::Result<Resp> {
        let tcp = TcpStream::connect_timeout(&self.addr, Duration::from_secs(5))?;
        tcp.set_read_timeout(Some(self.timeout))?;
        tcp.set_write_timeout(Some(self.timeout))?;
        let name = ServerName::from(self.addr.ip());
        let conn = rustls::ClientConnection::new(Arc::clone(&self.config), name)
            .map_err(std::io::Error::other)?;
        let mut tls = rustls::StreamOwned::new(conn, tcp);
        let mut head = format!(
            "{method} {target} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.addr
        );
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        if let Some(b) = body {
            head.push_str(&format!("Content-Length: {}\r\n", b.len()));
        }
        head.push_str("\r\n");
        tls.write_all(head.as_bytes())?;
        if let Some(b) = body {
            tls.write_all(b)?;
        }
        tls.flush()?;
        read_response(&mut tls)
    }

    pub fn get(&self, target: &str, headers: &[(&str, &str)]) -> Resp {
        self.request("GET", target, headers, None)
            .unwrap_or_else(|e| panic!("GET {target} over TLS failed: {e}"))
    }

    pub fn post_json(
        &self,
        target: &str,
        headers: &[(&str, &str)],
        body: &serde_json::Value,
    ) -> Resp {
        let mut h: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
        h.extend_from_slice(headers);
        let bytes = serde_json::to_vec(body).unwrap();
        self.request("POST", target, &h, Some(&bytes))
            .unwrap_or_else(|e| panic!("POST {target} over TLS failed: {e}"))
    }
}

/// Like [`Proc::wait_live`], over TLS.
pub fn wait_live_tls(proc: &mut Proc, client: &TlsClient, path: &str, timeout: Duration) {
    let end = Instant::now() + timeout;
    let mut client = client.clone();
    client.timeout = Duration::from_secs(2);
    loop {
        if let Some(status) = proc.try_exit() {
            panic!(
                "{} exited during startup with {status}\n--- log ---\n{}",
                proc.name,
                proc.log()
            );
        }
        if let Ok(r) = client.request("GET", path, &[], None)
            && r.status == 200
        {
            return;
        }
        if Instant::now() > end {
            panic!(
                "{} did not become live over TLS on {} within {timeout:?}\n--- log ---\n{}",
                proc.name,
                client.addr,
                proc.log()
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
