//! One active provider plus decrypt-only providers (migration between
//! providers), and the start-up check of a provider.

use std::sync::Arc;

use zeroize::Zeroizing;

use crate::{DEK_LEN, EncryptionError, KekProvider, WrappedDek};

/// Wraps with `active`; unwraps with whichever provider holds the key id.
/// Used to keep files under local keys readable while a key manager is the
/// active provider (`DASH_ENCRYPTION_PREVIOUS_KEY_FILES` with
/// `DASH_ENCRYPTION_PROVIDER=vault`), so `wal-inspect rewrap` can move them.
pub struct ChainedKekProvider {
    active: Arc<dyn KekProvider>,
    previous: Vec<Arc<dyn KekProvider>>,
}

impl ChainedKekProvider {
    pub fn new(active: Arc<dyn KekProvider>, previous: Vec<Arc<dyn KekProvider>>) -> Self {
        Self { active, previous }
    }
}

impl KekProvider for ChainedKekProvider {
    fn name(&self) -> &'static str {
        self.active.name()
    }

    fn active_key_id(&self) -> &str {
        self.active.active_key_id()
    }

    fn key_ids(&self) -> Vec<String> {
        let mut ids = self.active.key_ids();
        for provider in &self.previous {
            for id in provider.key_ids() {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids
    }

    fn wrap(&self, dek: &[u8; DEK_LEN], context: &[u8]) -> Result<WrappedDek, EncryptionError> {
        self.active.wrap(dek, context)
    }

    fn unwrap(
        &self,
        key_id: &str,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<Zeroizing<[u8; DEK_LEN]>, EncryptionError> {
        let holder = std::iter::once(&self.active)
            .chain(self.previous.iter())
            .find(|p| p.key_ids().iter().any(|id| id == key_id));
        match holder {
            Some(provider) => provider.unwrap(key_id, wrapped, context),
            None => Err(EncryptionError::UnknownKey {
                what: "data key".to_string(),
                key_id: key_id.to_string(),
                configured: self.key_ids().join(", "),
            }),
        }
    }
}

/// Context of the start-up check (never a real file id: those are 16 bytes).
const CHECK_CONTEXT: &[u8] = b"dash-startup-check";

/// Wraps and unwraps a random data key with the active key, so a wrong
/// address, token, key name or policy stops the service at start-up instead
/// of at the first file it creates.
pub fn check_provider(provider: &dyn KekProvider) -> Result<(), EncryptionError> {
    let mut dek = Zeroizing::new([0u8; DEK_LEN]);
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, dek.as_mut());
    let explain = |err: EncryptionError| {
        EncryptionError::Kms(format!(
            "start-up check of the {} provider (wrap and unwrap of a test data key with key id {}) failed: {err}",
            provider.name(),
            provider.active_key_id()
        ))
    };
    let wrapped = provider.wrap(&dek, CHECK_CONTEXT).map_err(explain)?;
    let back = provider
        .unwrap(&wrapped.key_id, &wrapped.bytes, CHECK_CONTEXT)
        .map_err(explain)?;
    if *back != *dek {
        return Err(explain(EncryptionError::Authentication(
            "test data key came back different".to_string(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalKekProvider;

    #[test]
    fn chain_wraps_with_the_active_provider_and_unwraps_with_any() {
        let old = Arc::new(LocalKekProvider::from_keys([1u8; 32], &[]).unwrap());
        let new = Arc::new(LocalKekProvider::from_keys([2u8; 32], &[]).unwrap());
        let old_wrapped = old.wrap(&[7u8; 32], b"f").unwrap();
        let chain = ChainedKekProvider::new(new.clone(), vec![old.clone()]);
        assert_eq!(chain.active_key_id(), new.active_key_id());
        assert_eq!(
            chain.key_ids(),
            vec![
                new.active_key_id().to_string(),
                old.active_key_id().to_string()
            ]
        );
        assert_eq!(
            *chain
                .unwrap(&old_wrapped.key_id, &old_wrapped.bytes, b"f")
                .unwrap(),
            [7u8; 32]
        );
        let wrapped = chain.wrap(&[8u8; 32], b"g").unwrap();
        assert_eq!(wrapped.key_id, new.active_key_id());
        assert!(matches!(
            chain.unwrap("local-0000000000000000", &wrapped.bytes, b"g"),
            Err(EncryptionError::UnknownKey { .. })
        ));
        check_provider(&chain).unwrap();
    }
}
