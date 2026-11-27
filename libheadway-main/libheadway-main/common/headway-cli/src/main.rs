use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use headway::map_tiles::Bounds;
use headway::{HeadwayServer, LogLevel};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "headway-cli",
    version,
    about = "Offline PMTiles tileserver and region extractor built on libheadway"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve extracted PMTiles over the libheadway HTTP tileserver
    Serve {
        /// Directory holding tiles/system and tiles/user
        #[arg(long, env = "HEADWAY_STORAGE_DIR", default_value = "/data")]
        storage_dir: PathBuf,
        /// Planet-scale .pmtiles URL used for future region extracts
        #[arg(long, env = "HEADWAY_EXTRACT_SOURCE_URL", default_value = "")]
        source_url: String,
        /// Address to bind the tileserver to
        #[arg(long, env = "HEADWAY_BIND", default_value = "0.0.0.0:9123")]
        bind: String,
    },
    /// Extract a bounding box into a local .pmtiles file
    Extract {
        #[arg(long, env = "HEADWAY_STORAGE_DIR", default_value = "/data")]
        storage_dir: PathBuf,
        /// Planet-scale .pmtiles URL to extract from
        #[arg(long, env = "HEADWAY_EXTRACT_SOURCE_URL")]
        source_url: String,
        /// Destination .pmtiles path
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        min_lon: f64,
        #[arg(long)]
        min_lat: f64,
        #[arg(long)]
        max_lon: f64,
        #[arg(long)]
        max_lat: f64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    headway::enable_logging("ocal-headway-cli".to_string(), LogLevel::Info);

    match Cli::parse().command {
        Command::Serve {
            storage_dir,
            source_url,
            bind,
        } => {
            let storage = path_str(&storage_dir)?;
            let server = HeadwayServer::new(storage, &source_url)
                .await
                .context("opening headway storage")?;
            println!("headway tileserver listening on http://{bind}");
            server.start(&bind).await.context("serving tiles")?;
        }
        Command::Extract {
            storage_dir,
            source_url,
            output,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        } => {
            let storage = path_str(&storage_dir)?;
            let server = HeadwayServer::new(storage, &source_url)
                .await
                .context("opening headway storage")?;
            let bounds = Arc::new(Bounds::nesw(max_lat, max_lon, min_lat, min_lon));
            let plan = server
                .prepare_pmtiles_extract(bounds, None)
                .await
                .context("planning pmtiles extract")?;
            let planned_bytes = plan.tile_data_length();
            println!("planned extract: {planned_bytes} bytes");

            let record = server
                .extract_pmtiles_region(Arc::new(plan), None)
                .await
                .context("extracting pmtiles region")?;

            let extracted = storage_dir
                .join("tiles")
                .join("user")
                .join(record.file_name());
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            std::fs::copy(&extracted, &output).with_context(|| {
                format!(
                    "copying {} to {}",
                    extracted.display(),
                    output.display()
                )
            })?;

            let summary = serde_json::json!({
                "file": output.display().to_string(),
                "size_bytes": std::fs::metadata(&output)?.len(),
                "planned_tile_bytes": planned_bytes,
                "bounds": {
                    "min_lon": min_lon,
                    "min_lat": min_lat,
                    "max_lon": max_lon,
                    "max_lat": max_lat,
                },
            });
            println!("{summary}");
        }
    }
    Ok(())
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("storage path is not valid UTF-8: {}", path.display()))
}
