use anyhow::{anyhow, Result};
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

    /// Ingest a single Artifact JSON file into SQLite
    Ingest {
        /// Path to an artifact JSON file matching the canonical schema
        artifact_json: PathBuf,

        /// SQLite DB path
        #[arg(long, default_value = "civic.db")]
        db: String,
    },
    /// Ingest a single Meeting JSON file into SQLite
    IngestMeeting {
        /// Path to a meeting JSON file matching the canonical schema
        meeting_json: PathBuf,

        /// SQLite DB path
        #[arg(long, default_value = "civic.db")]
        db: String,
    },
    /// Build/update an Obsidian vault from the SQLite database
    BuildVault {
        /// SQLite DB path
        #[arg(long, default_value = "civic.db")]
        db: String,

        /// Vault root directory
        #[arg(long, default_value = "vault")]
        vault: PathBuf,
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
        Commands::Ingest { artifact_json, db } => ingest_artifact(artifact_json, &db),
        Commands::IngestMeeting { meeting_json, db } => ingest_meeting(meeting_json, &db),
        Commands::BuildVault { db, vault } => build_vault(&db, vault),

    }
}

fn schema_export(out_dir: PathBuf) -> Result<()> {
    fs::create_dir_all(&out_dir)?;

    let artifact_schema = schema_for!(civic_core::schema::Artifact);
    fs::write(
        out_dir.join("Artifact.schema.json"),
        serde_json::to_string_pretty(&artifact_schema)?,
    )?;

    let source_schema = schema_for!(civic_core::schema::SourceRef);
    fs::write(
        out_dir.join("SourceRef.schema.json"),
        serde_json::to_string_pretty(&source_schema)?,
    )?;

    let body_schema = schema_for!(civic_core::schema::Body);
    fs::write(
        out_dir.join("Body.schema.json"),
        serde_json::to_string_pretty(&body_schema)?,
    )?;

    let official_schema = schema_for!(civic_core::schema::Official);
    fs::write(
        out_dir.join("Official.schema.json"),
        serde_json::to_string_pretty(&official_schema)?,
    )?;

    let meeting_schema = schema_for!(civic_core::schema::Meeting);
    fs::write(
        out_dir.join("Meeting.schema.json"),
        serde_json::to_string_pretty(&meeting_schema)?,
    )?;

    let motion_schema = schema_for!(civic_core::schema::Motion);
    fs::write(
        out_dir.join("Motion.schema.json"),
        serde_json::to_string_pretty(&motion_schema)?,
    )?;

    let vote_schema = schema_for!(civic_core::schema::Vote);
    fs::write(
        out_dir.join("Vote.schema.json"),
        serde_json::to_string_pretty(&vote_schema)?,
    )?;

    println!("Exported schemas to {}", out_dir.display());
    Ok(())
}

fn ingest_artifact(path: PathBuf, db_path: &str) -> Result<()> {
    let raw = fs::read_to_string(&path)?;
    let raw_json: serde_json::Value = serde_json::from_str(&raw)?;

    let artifact: civic_core::schema::Artifact =
        serde_json::from_value(raw_json.clone()).map_err(|e| anyhow!("Schema mismatch: {e}"))?;

    validate_artifact(&artifact)?;

    let conn = civic_core::db::open(db_path)?;
    civic_core::db::upsert_artifact(&conn, &artifact, &raw_json)?;

    println!(
        "Ingested artifact id={} into db={}",
        artifact.id,
        db_path
    );
    Ok(())
}

fn ingest_meeting(path: PathBuf, db_path: &str) -> Result<()> {
    let raw = fs::read_to_string(&path)?;
    let raw_json: serde_json::Value = serde_json::from_str(&raw)?;

    let meeting: civic_core::schema::Meeting =
        serde_json::from_value(raw_json.clone()).map_err(|e| anyhow!("Schema mismatch: {e}"))?;

    validate_meeting(&meeting)?;

    let mut conn = civic_core::db::open(db_path)?;
    civic_core::db::upsert_meeting_with_children(&mut conn, &meeting, &raw_json)?;

    println!("Ingested meeting id={} into db={}", meeting.id, db_path);
    Ok(())
}

// Keep validation lightweight for v1; expand later.
fn validate_artifact(a: &civic_core::schema::Artifact) -> Result<()> {
    if a.id.trim().is_empty() {
        return Err(anyhow!("Artifact.id must not be empty"));
    }
    if a.source.kind.trim().is_empty() {
        return Err(anyhow!("Artifact.source.kind must not be empty"));
    }
    if a.source.value.trim().is_empty() {
        return Err(anyhow!("Artifact.source.value must not be empty"));
    }
    if a.source.retrieved_at.trim().is_empty() {
        return Err(anyhow!("Artifact.source.retrieved_at must not be empty"));
    }
    Ok(())
}

fn validate_meeting(m: &civic_core::schema::Meeting) -> Result<()> {
    if m.id.trim().is_empty() {
        return Err(anyhow!("Meeting.id must not be empty"));
    }
    if m.body_id.trim().is_empty() {
        return Err(anyhow!("Meeting.body_id must not be empty"));
    }
    if m.started_at.trim().is_empty() {
        return Err(anyhow!("Meeting.started_at must not be empty"));
    }
    for artifact_id in &m.artifact_ids {
        if artifact_id.trim().is_empty() {
            return Err(anyhow!("Meeting.artifact_ids must not contain empty values"));
        }
    }
    for motion in &m.motions {
        if motion.id.trim().is_empty() {
            return Err(anyhow!("Motion.id must not be empty"));
        }
        if motion.meeting_id.trim().is_empty() {
            return Err(anyhow!("Motion.meeting_id must not be empty"));
        }
        if motion.meeting_id != m.id {
            return Err(anyhow!(
                "Motion.meeting_id {} does not match Meeting.id {}",
                motion.meeting_id,
                m.id
            ));
        }
        if motion.title.trim().is_empty() {
            return Err(anyhow!("Motion.title must not be empty"));
        }
        for vote in &motion.votes {
            if vote.motion_id.trim().is_empty() {
                return Err(anyhow!("Vote.motion_id must not be empty"));
            }
            if vote.motion_id != motion.id {
                return Err(anyhow!(
                    "Vote.motion_id {} does not match Motion.id {}",
                    vote.motion_id,
                    motion.id
                ));
            }
            if vote.official_id.trim().is_empty() {
                return Err(anyhow!("Vote.official_id must not be empty"));
            }
            if vote.value.trim().is_empty() {
                return Err(anyhow!("Vote.value must not be empty"));
            }
        }
    }
    Ok(())
}

// Build/update an Obsidian vault from the sqlite database. Will be expanded further.
fn build_vault(db_path: &str, vault: PathBuf) -> Result<()> {
    let conn = civic_core::db::open(db_path)?;
    obsidian::vault::build_vault(&conn, &vault)?;
    println!("Vault updated at {}", vault.display());
    Ok(())
}
