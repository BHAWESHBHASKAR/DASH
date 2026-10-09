//! P2 engine spike: measurement harness. One subcommand per experiment; JSON lines on stdout.
//!
//!   engine-spike vec    kind=exact|repo|f32|f16|i8 n=50000 threads=4 [eval=full|short] [keep=1]
//!   engine-spike filter n=200000 kind=i8
//!   engine-spike text   [docs=200000]
//!   engine-spike conc   kind=i8 n=200000
//!   engine-spike deletes n=50000
//!   engine-spike wal    [dir=...]
//!   engine-spike info

mod common;
mod conc;
mod deletes;
mod filtered;
mod repoann;
mod text;
mod vecbench;
mod walbench;

use common::*;

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cmd = argv.first().cloned().unwrap_or_default();
    let a = Args::parse(&argv[1.min(argv.len())..]);
    CLUSTERS.store(a.u("clusters", N_CLUSTERS), std::sync::atomic::Ordering::Relaxed);
    match cmd.as_str() {
        "vec" => vecbench::run(&a),
        "filter" => filtered::run(&a),
        "text" => text::run(&a),
        "conc" => conc::run(&a),
        "deletes" => deletes::run(&a),
        "wal" => walbench::run(&a),
        "info" => {
            emit(serde_json::json!({
                "usearch_version": usearch::version(),
                "hw_compiled": usearch::hardware_acceleration_compiled(),
                "hw_available": usearch::hardware_acceleration_available(),
                "rayon_threads": rayon::current_num_threads(),
            }));
        }
        _ => eprintln!("usage: engine-spike vec|filter|text|conc|deletes|wal|info key=value..."),
    }
}
