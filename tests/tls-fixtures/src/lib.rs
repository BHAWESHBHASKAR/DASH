//! Test-only PKI shared by the TLS tests: a server CA, a client CA, a rogue
//! CA nobody trusts, and leaf certificates written as PEM files into a
//! temporary directory. Keys are fresh ECDSA P-256 keys on every run.

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};

pub use rcgen;

/// A certificate authority that can sign leaves.
pub type Ca = CertifiedIssuer<'static, KeyPair>;

/// Names every server certificate is valid for.
pub const SERVER_NAMES: [&str; 2] = ["localhost", "127.0.0.1"];

pub fn new_ca(name: &str) -> Ca {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    CertifiedIssuer::self_signed(params, KeyPair::generate().expect("ca key")).expect("ca cert")
}

/// A leaf certificate signed by a CA.
pub struct Leaf {
    pub cert_pem: String,
    pub key_pem: String,
    pub cert_der: Vec<u8>,
}

/// A leaf for `names` (DNS names or IP addresses), for server or client use.
pub fn leaf(ca: &Ca, names: &[&str], client: bool) -> Leaf {
    let mut params = CertificateParams::new(
        names
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>(),
    )
    .expect("leaf params");
    params
        .distinguished_name
        .push(DnType::CommonName, names.first().copied().unwrap_or("leaf"));
    params.extended_key_usages = vec![if client {
        ExtendedKeyUsagePurpose::ClientAuth
    } else {
        ExtendedKeyUsagePurpose::ServerAuth
    }];
    let key = KeyPair::generate().expect("leaf key");
    let cert = params.signed_by(&key, ca).expect("leaf cert");
    Leaf {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
        cert_der: cert.der().to_vec(),
    }
}

/// One test's PKI, as PEM files in a temporary directory removed on drop.
/// `client_der` is the DER the client-certificate fingerprint is taken over.
pub struct TestPki {
    pub dir: tempfile::TempDir,
    pub server_ca: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_ca: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    pub client_der: Vec<u8>,
    /// A client certificate from a CA no server trusts.
    pub rogue_cert: PathBuf,
    pub rogue_key: PathBuf,
}

impl Default for TestPki {
    fn default() -> Self {
        Self::new()
    }
}

impl TestPki {
    pub fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("dash-tls-")
            .tempdir()
            .expect("tempdir");
        let write = |name: &str, content: &str| write_file(dir.path(), name, content);
        let server_ca = new_ca("DASH test server CA");
        let client_ca = new_ca("DASH test client CA");
        let rogue_ca = new_ca("rogue CA");
        let server = leaf(&server_ca, &SERVER_NAMES, false);
        let client = leaf(&client_ca, &["replica-1"], true);
        let rogue = leaf(&rogue_ca, &["replica-x"], true);
        TestPki {
            server_ca: write("server-ca.pem", &server_ca.pem()),
            server_cert: write("server.pem", &server.cert_pem),
            server_key: write("server.key", &server.key_pem),
            client_ca: write("client-ca.pem", &client_ca.pem()),
            client_cert: write("client.pem", &client.cert_pem),
            client_key: write("client.key", &client.key_pem),
            client_der: client.cert_der,
            rogue_cert: write("rogue.pem", &rogue.cert_pem),
            rogue_key: write("rogue.key", &rogue.key_pem),
            dir,
        }
    }

    /// Write `content` to `name` inside the PKI directory.
    pub fn write(&self, name: &str, content: &str) -> PathBuf {
        write_file(self.dir.path(), name, content)
    }
}

fn write_file(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).expect("write pem");
    path
}
