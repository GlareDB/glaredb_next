//! Bin for running the SLTs.

use clap::Parser;
use rayexec_error::{Result, ResultExt};
use rayexec_sqltest::run_test;
use std::fs;
use std::path::{Path, PathBuf};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Path to slts directory relative to this crate's root.
const SLTS_PATH: &str = "slts/";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Only test files that contain this filter string are run.
    // Positional
    #[clap(value_name = "FILTER")]
    pub filter: Option<String>,
}

impl Arguments {
    /// Filter file paths based on the filter provided as an argument.
    fn filter_paths(&self, mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
        paths.retain(|path| match self.filter.as_ref() {
            Some(filter) => path.to_string_lossy().contains(filter),
            None => true,
        });
        paths
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    let args = Arguments::parse();

    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    std::panic::set_hook(Box::new(|info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        println!("---- PANIC ----\nInfo: {}\n\nBacktrace:{}", info, backtrace);
        std::process::abort();
    }));

    let mut paths = Vec::new();
    find_files(Path::new(SLTS_PATH), &mut paths).unwrap();

    paths = args.filter_paths(paths);

    for path in paths {
        if let Err(e) = run_test(&path).await {
            println!("---- FAIL ----");
            println!("{e}");
            println!(
                "Rerun this SLT with '-p rayexec_sqltest {}'",
                path.to_string_lossy()
            );
            std::process::exit(1);
        }
    }
}

fn find_files(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir).context("read dir")? {
            let entry = entry.context("entry")?;
            let path = entry.path();
            if path.is_dir() {
                find_files(&path, paths)?;
            } else {
                paths.push(path.to_path_buf());
            }
        }
    }
    Ok(())
}
