use anyhow::Result;
use clap::{Parser, Subcommand};
use schemars::schema_for;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "larue")]
#[command(about = "LaRue Civic Intelligence CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export canonical JSON Schemas to the ./schemas directory
    Schema {
        #[command(subcommand)]
        command: SchemaCommands,
    },
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// Export JSON Schema files for canonical types
    Export {
        /// Output directory (default: ./schemas)
        #[arg(long, default_value = "schemas")]
        out_dir: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Schema { command } => match command {
            SchemaCommands::Export { out_dir } => schema_export(out_dir),
        },
    }
}

fn schema_export(out_dir: PathBuf) -> Result<()> {
    fs::create_dir_all(&out_dir)?;

    // Export Artifact schema
    let artifact_schema = schema_for!(civic_core::schema::Artifact);
    let artifact_json = serde_json::to_string_pretty(&artifact_schema)?;
    fs::write(out_dir.join("Artifact.schema.json"), artifact_json)?;

    // Export SourceRef schema
    let source_schema = schema_for!(civic_core::schema::SourceRef);
    let source_json = serde_json::to_string_pretty(&source_schema)?;
    fs::write(out_dir.join("SourceRef.schema.json"), source_json)?;

    println!("Exported schemas to {}", out_dir.display());
    Ok(())
}
