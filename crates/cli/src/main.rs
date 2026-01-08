use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use schemars::schema_for;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use time::format_description::well_known::Rfc3339;
use time::format_description::FormatItem;
use time::{Duration, OffsetDateTime};

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
    /// Ingest all Artifact JSON files in a directory into SQLite
    IngestDir {
        /// Directory containing artifact JSON files
        dir: PathBuf,

        /// Optional config file path
        #[arg(long)]
        config: Option<PathBuf>,

        /// SQLite DB path
        #[arg(long)]
        db: Option<String>,
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
        /// Optional config file path
        #[arg(long)]
        config: Option<PathBuf>,

        /// SQLite DB path
        #[arg(long)]
        db: Option<String>,

        /// Vault root directory
        #[arg(long)]
        vault: Option<PathBuf>,
    },
    /// Run the weekly pipeline: collect -> ingest-dir -> build-vault
    RunWeekly {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
    },
    /// Generate a weekly report (last 7 days) from the database
    ReportWeekly {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
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
        Commands::IngestDir { dir, config, db } => {
            let config = config.as_ref().map(load_config).transpose()?;
            let storage = resolve_storage(config.as_ref());
            let db_path = db.unwrap_or(storage.db_path);
            ingest_dir(dir, &db_path)
        }
        Commands::IngestMeeting { meeting_json, db } => ingest_meeting(meeting_json, &db),
        Commands::BuildVault { config, db, vault } => {
            let config = config.as_ref().map(load_config).transpose()?;
            let storage = resolve_storage(config.as_ref());
            let db_path = db.unwrap_or(storage.db_path);
            let vault_path = vault.unwrap_or(storage.vault_path);
            build_vault(&db_path, vault_path)
        }
        Commands::RunWeekly { config } => run_weekly(config),
        Commands::ReportWeekly { config } => report_weekly(config),
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    storage: Option<StorageConfig>,
    sources: Option<SourcesConfig>,
}

#[derive(Debug, Deserialize)]
struct StorageConfig {
    db_path: Option<String>,
    vault_path: Option<String>,
    out_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SourcesConfig {
    larue_fiscal_court: Option<SourceConfig>,
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    enabled: Option<bool>,
    base_url: Option<String>,
}

#[derive(Debug)]
struct ResolvedStorage {
    db_path: String,
    vault_path: PathBuf,
    out_dir: PathBuf,
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let raw = fs::read_to_string(path)?;
    let config = toml::from_str(&raw)?;
    Ok(config)
}

fn resolve_storage(config: Option<&Config>) -> ResolvedStorage {
    let storage = config.and_then(|cfg| cfg.storage.as_ref());
    let db_path = storage
        .and_then(|value| value.db_path.clone())
        .unwrap_or_else(|| "civic.db".to_string());
    let vault_path = storage
        .and_then(|value| value.vault_path.clone())
        .unwrap_or_else(|| "vault".to_string());
    let out_dir = storage
        .and_then(|value| value.out_dir.clone())
        .unwrap_or_else(|| "out".to_string());
    ResolvedStorage {
        db_path,
        vault_path: PathBuf::from(vault_path),
        out_dir: PathBuf::from(out_dir),
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

    let meeting_schema = schema_for!(civic_core::schema::Meeting);
    fs::write(
        out_dir.join("Meeting.schema.json"),
        serde_json::to_string_pretty(&meeting_schema)?,
    )?;

    println!("Exported schemas to {}", out_dir.display());
    Ok(())
}

fn ingest_artifact(path: PathBuf, db_path: &str) -> Result<()> {
    let raw = fs::read_to_string(&path)?;
    let raw_json: serde_json::Value = serde_json::from_str(&raw)?;
    let conn = civic_core::db::open(db_path)?;
    let artifact_id = ingest_artifact_json(&conn, raw_json)?;

    println!(
        "Ingested artifact id={} into db={}",
        artifact_id,
        db_path
    );
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

fn ingest_dir(dir: PathBuf, db_path: &str) -> Result<()> {
    if !dir.exists() {
        println!("No artifacts directory found at {}", dir.display());
        return Ok(());
    }

    let conn = civic_core::db::open(db_path)?;

    let mut ingested = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            skipped += 1;
            continue;
        }
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                failed += 1;
                eprintln!("Failed to read {}: {err}", path.display());
                continue;
            }
        };
        let raw_json: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(raw_json) => raw_json,
            Err(err) => {
                failed += 1;
                eprintln!("Failed to parse {}: {err}", path.display());
                continue;
            }
        };
        let artifact_id = match raw_json.get("id").and_then(|value| value.as_str()) {
            Some(value) => value,
            None => {
                failed += 1;
                eprintln!("Missing artifact id in {}", path.display());
                continue;
            }
        };
        if civic_core::db::artifact_exists(&conn, artifact_id)? {
            skipped += 1;
            continue;
        }
        match ingest_artifact_json(&conn, raw_json) {
            Ok(_) => ingested += 1,
            Err(err) => {
                failed += 1;
                eprintln!("Failed to ingest {}: {err}", path.display());
            }
        }
    }

    println!(
        "Ingested {} artifacts, {} failed, {} skipped in {}",
        ingested,
        failed,
        skipped,
        dir.display()
    );
    Ok(())
}

fn ingest_meeting(path: PathBuf, db_path: &str) -> Result<()> {
    let raw = fs::read_to_string(&path)?;
    let raw_json: serde_json::Value = serde_json::from_str(&raw)?;
    let meeting: civic_core::schema::Meeting =
        serde_json::from_value(raw_json.clone()).map_err(|e| anyhow!("Schema mismatch: {e}"))?;
    validate_meeting(&meeting)?;
    let conn = civic_core::db::open(db_path)?;
    civic_core::db::upsert_meeting(&conn, &meeting, &raw_json)?;
    println!("Ingested meeting id={} into db={}", meeting.id, db_path);
    Ok(())
}

fn validate_meeting(meeting: &civic_core::schema::Meeting) -> Result<()> {
    if meeting.id.trim().is_empty() {
        return Err(anyhow!("Meeting.id must not be empty"));
    }
    if meeting.body_id.trim().is_empty() {
        return Err(anyhow!("Meeting.body_id must not be empty"));
    }
    if meeting.started_at.trim().is_empty() {
        return Err(anyhow!("Meeting.started_at must not be empty"));
    }
    Ok(())
}

fn ingest_artifact_json(
    conn: &rusqlite::Connection,
    raw_json: serde_json::Value,
) -> Result<String> {
    let artifact: civic_core::schema::Artifact =
        serde_json::from_value(raw_json.clone()).map_err(|e| anyhow!("Schema mismatch: {e}"))?;

    validate_artifact(&artifact)?;
    civic_core::db::upsert_artifact(conn, &artifact, &raw_json)?;
    Ok(artifact.id)
}

fn ingest_meeting_dir(dir: PathBuf, db_path: &str) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    let conn = civic_core::db::open(db_path)?;
    let mut ingested = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            skipped += 1;
            continue;
        }
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                failed += 1;
                eprintln!("Failed to read meeting {}: {err}", path.display());
                continue;
            }
        };
        let raw_json: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(raw_json) => raw_json,
            Err(err) => {
                failed += 1;
                eprintln!("Failed to parse meeting {}: {err}", path.display());
                continue;
            }
        };
        let meeting_id = match raw_json.get("id").and_then(|value| value.as_str()) {
            Some(value) => value,
            None => {
                failed += 1;
                eprintln!("Missing meeting id in {}", path.display());
                continue;
            }
        };
        if civic_core::db::meeting_exists(&conn, meeting_id)? {
            skipped += 1;
            continue;
        }
        let meeting: civic_core::schema::Meeting = match serde_json::from_value(raw_json.clone()) {
            Ok(meeting) => meeting,
            Err(err) => {
                failed += 1;
                eprintln!("Meeting schema mismatch in {}: {err}", path.display());
                continue;
            }
        };
        if let Err(err) = validate_meeting(&meeting) {
            failed += 1;
            eprintln!("Meeting validation failed in {}: {err}", path.display());
            continue;
        }
        if let Err(err) = civic_core::db::upsert_meeting(&conn, &meeting, &raw_json) {
            failed += 1;
            eprintln!("Failed to ingest meeting {}: {err}", path.display());
            continue;
        }
        ingested += 1;
    }

    println!(
        "Ingested {} meetings, {} failed, {} skipped in {}",
        ingested,
        failed,
        skipped,
        dir.display()
    );
    Ok(())
}

// Build/update an Obsidian vault from the sqlite database. Will be expanded further.
fn build_vault(db_path: &str, vault: PathBuf) -> Result<()> {
    let conn = civic_core::db::open(db_path)?;
    obsidian::vault::build_vault(&conn, &vault)?;
    println!("Vault updated at {}", vault.display());
    Ok(())
}

fn run_weekly(config_path: PathBuf) -> Result<()> {
    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));

    let output = Command::new("python")
        .arg("workers/collectors/ky_public_notice_larue.py")
        .arg("--config")
        .arg(&config_path)
        .output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Collector failed with status {}", output.status);
        if !stdout.is_empty() {
            eprintln!("Collector stdout:\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("Collector stderr:\n{stderr}");
        }
        return Err(anyhow!("Collector exited with failure"));
    }

    let artifacts_dir = storage.out_dir.join("artifacts");
    ingest_dir(artifacts_dir, &storage.db_path)?;

    if fiscal_court_enabled(&config) {
        run_fiscal_court_collector(&config_path)?;
        let artifacts_dir = storage.out_dir.join("artifacts");
        ingest_dir(artifacts_dir.clone(), &storage.db_path)?;
        parse_meetings(&storage, artifacts_dir)?;
        let meetings_dir = storage.out_dir.join("meetings");
        ingest_meeting_dir(meetings_dir, &storage.db_path)?;
    }

    build_vault(&storage.db_path, storage.vault_path)?;
    report_weekly(config_path)?;
    Ok(())
}

fn fiscal_court_enabled(config: &Config) -> bool {
    config
        .sources
        .as_ref()
        .and_then(|sources| sources.larue_fiscal_court.as_ref())
        .and_then(|source| source.enabled)
        .unwrap_or(false)
}

fn run_fiscal_court_collector(config_path: &PathBuf) -> Result<()> {
    let output = Command::new("python")
        .arg("workers/collectors/larue_fiscal_court_agendas.py")
        .arg("--config")
        .arg(config_path)
        .output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Fiscal court collector failed with status {}", output.status);
        if !stdout.is_empty() {
            eprintln!("Collector stdout:\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("Collector stderr:\n{stderr}");
        }
        return Err(anyhow!("Fiscal court collector exited with failure"));
    }
    Ok(())
}

fn parse_meetings(storage: &ResolvedStorage, artifacts_dir: PathBuf) -> Result<()> {
    let snapshots_dir = storage.out_dir.join("snapshots");
    let meetings_dir = storage.out_dir.join("meetings");
    fs::create_dir_all(&meetings_dir)?;

    let mut snapshot_map = std::collections::HashMap::new();
    if snapshots_dir.exists() {
        for entry in fs::read_dir(&snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(stem) = path.file_stem().and_then(|value| value.to_str()) {
                snapshot_map.insert(stem.to_string(), path);
            }
        }
    }

    if !artifacts_dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(&artifacts_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let raw = fs::read_to_string(&path)?;
        let raw_json: serde_json::Value = serde_json::from_str(&raw)?;
        let tags = raw_json
            .get("tags")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let tag_set: Vec<String> = tags
            .iter()
            .filter_map(|value| value.as_str().map(|s| s.to_string()))
            .collect();
        if !tag_set.iter().any(|tag| tag == "meeting")
            || !tag_set.iter().any(|tag| tag == "fiscal_court")
        {
            continue;
        }

        let stem = match path.file_stem().and_then(|value| value.to_str()) {
            Some(stem) => stem.to_string(),
            None => continue,
        };
        let snapshot_path = match snapshot_map.get(&stem) {
            Some(path) => path,
            None => {
                eprintln!("Missing snapshot for artifact {}", path.display());
                continue;
            }
        };

        let output = Command::new("python")
            .arg("workers/parsers/parse_meeting_minutes.py")
            .arg("--artifact")
            .arg(&path)
            .arg("--snapshot")
            .arg(snapshot_path)
            .arg("--out-dir")
            .arg(&storage.out_dir)
            .output()?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!(
                "Meeting parser failed for {} with status {}",
                path.display(),
                output.status
            );
            if !stdout.is_empty() {
                eprintln!("Parser stdout:\n{stdout}");
            }
            if !stderr.is_empty() {
                eprintln!("Parser stderr:\n{stderr}");
            }
        }
    }
    Ok(())
}

fn report_weekly(config_path: PathBuf) -> Result<()> {
    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));
    let conn = civic_core::db::open(&storage.db_path)?;

    let now = OffsetDateTime::now_utc();
    let start = now - Duration::days(7);
    let date_format: &[FormatItem<'_>] = time::macros::format_description!("[year]-[month]-[day]");
    let date_str = now.format(date_format)?;
    let window_start = start.format(&Rfc3339)?;
    let window_end = now.format(&Rfc3339)?;

    let mut stmt = conn.prepare(
        r#"
        SELECT id, title, retrieved_at, source_value
        FROM artifacts
        WHERE datetime(retrieved_at) >= datetime(?1)
          AND datetime(retrieved_at) <= datetime(?2)
        ORDER BY retrieved_at ASC, id ASC
        "#,
    )?;

    let rows = stmt.query_map([window_start.as_str(), window_end.as_str()], |row| {
        Ok(ReportArtifactRow {
            id: row.get(0)?,
            title: row.get(1)?,
            retrieved_at: row.get(2)?,
            source_value: row.get(3)?,
        })
    })?;

    let mut artifacts = Vec::new();
    for row in rows {
        artifacts.push(row?);
    }

    let report_dir = storage.vault_path.join("Reports").join("Weekly");
    fs::create_dir_all(&report_dir)?;
    let report_path = report_dir.join(format!("{date_str}.md"));

    let mut markdown = String::new();
    markdown.push_str(&format!("# Weekly Report {date_str}\n\n"));
    markdown.push_str(&format!("Window: {window_start} to {window_end} UTC\n\n"));
    markdown.push_str(&format!("Total artifacts: {}\n\n", artifacts.len()));
    for artifact in &artifacts {
        let title = artifact
            .title
            .as_deref()
            .unwrap_or("(untitled)")
            .replace('\n', " ");
        markdown.push_str(&format!(
            "- [{title}]({}) — {}\n",
            artifact.source_value, artifact.retrieved_at
        ));
    }
    fs::write(&report_path, markdown)?;

    let report_json_dir = storage
        .out_dir
        .join("reports")
        .join("weekly");
    fs::create_dir_all(&report_json_dir)?;
    let report_json_path = report_json_dir.join(format!("{date_str}.json"));
    let json_payload = serde_json::json!({
        "date": date_str,
        "window_start": window_start,
        "window_end": window_end,
        "total": artifacts.len(),
        "artifacts": artifacts.iter().map(|artifact| {
            serde_json::json!({
                "id": artifact.id,
                "title": artifact.title,
                "retrieved_at": artifact.retrieved_at,
                "source_value": artifact.source_value,
            })
        }).collect::<Vec<_>>()
    });
    fs::write(&report_json_path, serde_json::to_string_pretty(&json_payload)?)?;

    println!("Weekly report written to {}", report_path.display());
    Ok(())
}

struct ReportArtifactRow {
    id: String,
    title: Option<String>,
    retrieved_at: String,
    source_value: String,
}
