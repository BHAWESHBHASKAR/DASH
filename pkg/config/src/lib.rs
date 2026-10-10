//! Typed configuration for DASH.
//!
//! One registry ([`registry::REGISTRY`]) lists every environment setting the
//! code reads: its canonical name, scope, value kind, default, description,
//! deprecated aliases. From it this crate derives
//!
//! * [`validate_env`]: startup validation (malformed values are errors,
//!   unknown `DASH_*` / `EME_*` variables are warnings with a suggestion);
//! * the TOML file overlay ([`overlay`]), selected with `DASH_CONFIG_FILE`;
//! * the generated configuration reference page ([`docs`]);
//! * the `dash-config` command line tool.
//!
//! Services call [`startup_check`] once at the top of `main`.

pub mod docs;
pub mod model;
pub mod overlay;
pub mod registry;
pub mod startup;
pub mod validate;

pub use model::{
    Alias, Entry, Honors, Kind, Scope, Service, Setting, all_names, canonical_names, eme_twin,
    lookup, lookup_file_key, settings,
};
pub use startup::{
    CONFIG_FILE_ENV, CONFIG_VALIDATION_ENV, FileData, Resolved, Source, StartupPlan,
    apply_to_process_env, plan_startup, resolve, snapshot_process_env, startup_check,
};
pub use validate::{
    EnvSource, Issue, ProcessEnv, Report, did_you_mean, edit_distance, validate_env,
};
