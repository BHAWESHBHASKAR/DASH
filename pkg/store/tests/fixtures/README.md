# Store test fixtures

`legacy-bincode-v1.hex` holds redb value bytes exactly as `bincode` 1.3.3
(`bincode::serialize`) wrote them, one `name=hex` line per stored type:
`Claim` (all optional fields set, and a minimal one), `Vec<Evidence>`,
`Vec<ClaimEdge>`, `Vec<f32>`, `BatchCommitMetadata` and `StoreIndexStats`.

They were generated once, with `bincode` 1.3.3 still in the dependency
tree, from the values listed in `pkg/store/src/value_codec.rs`
(`tests` module). `bincode` has since been removed, so the file cannot be
regenerated and must not be edited: it is the record of what older
releases left on disk.

Used by:

- `pkg/store/src/value_codec.rs` (unit tests): the legacy bytes decode to
  the original values, and the new encoder produces the same body bytes.
- `pkg/store/tests/disk_legacy_format.rs`: a redb file built from these
  bytes with the store's table layout loads through `DiskBackedStore` and
  `InMemoryStore::load_from_disk_and_wal`.
