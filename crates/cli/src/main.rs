use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use civic_core::scoring::{DecisionScore, LinkedArtifact, Rubric, ScoreResult, VoteChoice};
use schemars::schema_for;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::path::Path;
use std::process::Command;
use time::format_description::well_known::Rfc3339;
use time::format_description::FormatItem;
use time::{Date, Duration, OffsetDateTime, Time};

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
    /// Extract normalized text into Artifact JSONs
    ExtractText {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
    },
    /// Apply issue tagging to Artifact JSONs
    TagArtifacts {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
        /// Force re-tagging of previously tagged artifacts
        #[arg(long)]
        force: bool,
    },
    /// Ingest parsed decision JSON files into SQLite
    IngestDecisions {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
    },
    /// Score weekly decisions using the rubric
    ScoreWeekly {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
        /// Override report date (YYYY-MM-DD)
        #[arg(long)]
        date: Option<String>,
    },
    /// Generate a weekly report (last 7 days) from the database
    ReportWeekly {
        /// Config file path
        #[arg(long)]
        config: PathBuf,
    },
    /// Placeholder for weekly AI digest generation
    DigestWeekly,
    /// Placeholder for publishing artifacts (e.g., Web3/static)
    Publish,
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
        Commands::ExtractText { config } => extract_text(config),
        Commands::TagArtifacts { config, force } => tag_artifacts(config, force),
        Commands::IngestDecisions { config } => ingest_decisions(config),
        Commands::ScoreWeekly { config, date } => score_weekly(config, date),
        Commands::ReportWeekly { config } => report_weekly(config),
        Commands::DigestWeekly => digest_weekly(),
        Commands::Publish => publish_placeholder(),
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    storage: Option<StorageConfig>,
    sources: Option<SourcesConfig>,
    ai: Option<AiConfig>,
    publish: Option<PublishConfig>,
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
    wayback: Option<WaybackConfig>,
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    enabled: Option<bool>,
    base_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WaybackConfig {
    enabled: Option<bool>,
    urls: Option<Vec<String>>,
    rate_limit_seconds: Option<f32>,
    limit_per_run: Option<usize>,
    include_subpaths: Option<bool>,
    high_impact_url_keywords: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct AiConfig {
    enabled: Option<bool>,
    provider: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublishConfig {
    enabled: Option<bool>,
    provider: Option<String>,
}

#[derive(Debug)]
struct ResolvedStorage {
    db_path: String,
    vault_path: PathBuf,
    out_dir: PathBuf,
}

fn load_config(path: &PathBuf) -> Result<Config> {
    ensure_config_path(path)?;
    let raw = fs::read_to_string(path)?;
    let config = toml::from_str(&raw)?;
    warn_missing_config_keys(&config);
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

fn ensure_config_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow!(
            "Config file not found: {}. Tip: cp config.example.toml config.toml",
            path.display()
        ));
    }
    Ok(())
}

fn warn_missing_config_keys(config: &Config) {
    let mut missing = Vec::new();
    let storage = config.storage.as_ref();
    if storage
        .and_then(|value| value.db_path.as_ref())
        .is_none()
    {
        missing.push("storage.db_path");
    }
    if storage
        .and_then(|value| value.vault_path.as_ref())
        .is_none()
    {
        missing.push("storage.vault_path");
    }
    if storage
        .and_then(|value| value.out_dir.as_ref())
        .is_none()
    {
        missing.push("storage.out_dir");
    }
    if !missing.is_empty() {
        eprintln!(
            "Config missing keys in [storage]: {} (defaults will be used).",
            missing.join(", ")
        );
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
    ensure_config_path(&config_path)?;
    let python = find_python_interpreter()?;
    let collector_path = Path::new("workers/collectors/ky_public_notice_larue.py");
    if !collector_path.exists() {
        return Err(anyhow!(
            "Collector script not found: {}",
            collector_path.display()
        ));
    }

    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));

    let output = Command::new(&python)
        .arg(collector_path)
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

    if fiscal_court_enabled(&config) {
        run_fiscal_court_collector(&python, &config_path)?;
    }

    if wayback_enabled(&config) {
        run_wayback_collector(&python, &config_path)?;
    }

    let artifacts_dir = storage.out_dir.join("artifacts");
    ingest_dir(artifacts_dir.clone(), &storage.db_path)?;

    if let Err(err) = extract_text(config_path.clone()) {
        eprintln!("Warning: extract-text failed: {err}");
    }

    if let Err(err) = tag_artifacts(config_path.clone(), false) {
        eprintln!("Warning: tag-artifacts failed: {err}");
    }

    if let Err(err) = parse_meetings(&python, &config_path, &storage) {
        eprintln!("Warning: parse-meetings failed: {err}");
    }

    if let Err(err) = ingest_decisions(config_path.clone()) {
        eprintln!("Warning: ingest-decisions failed: {err}");
    }

    if let Err(err) = score_weekly(config_path.clone(), None) {
        eprintln!("Warning: score-weekly failed: {err}");
    }

    report_weekly(config_path.clone())?;
    build_vault(&storage.db_path, storage.vault_path)?;
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

fn wayback_enabled(config: &Config) -> bool {
    config
        .sources
        .as_ref()
        .and_then(|sources| sources.wayback.as_ref())
        .and_then(|source| source.enabled)
        .unwrap_or(false)
}

fn run_fiscal_court_collector(python: &str, config_path: &PathBuf) -> Result<()> {
    let collector_path = Path::new("workers/collectors/larue_fiscal_court_agendas.py");
    if !collector_path.exists() {
        return Err(anyhow!(
            "Collector script not found: {}",
            collector_path.display()
        ));
    }

    let output = Command::new(python)
        .arg(collector_path)
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

fn parse_meetings(
    python: &str,
    config_path: &PathBuf,
    storage: &ResolvedStorage,
) -> Result<()> {
    let parser_path = Path::new("workers/parsers/parse_meeting_minutes.py");
    if !parser_path.exists() {
        return Err(anyhow!(
            "Meeting parser script not found: {}",
            parser_path.display()
        ));
    }

    let artifacts_dir = storage.out_dir.join("artifacts");
    let output = Command::new(python)
        .arg(parser_path)
        .arg("--config")
        .arg(config_path)
        .arg("--artifacts")
        .arg(&artifacts_dir)
        .output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Meeting parser failed with status {}", output.status);
        if !stdout.is_empty() {
            eprintln!("Parser stdout:\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("Parser stderr:\n{stderr}");
        }
        return Err(anyhow!("Meeting parser exited with failure"));
    }
    Ok(())
}

fn run_wayback_collector(python: &str, config_path: &PathBuf) -> Result<()> {
    let collector_path = Path::new("workers/collectors/wayback_backfill.py");
    if !collector_path.exists() {
        return Err(anyhow!(
            "Collector script not found: {}",
            collector_path.display()
        ));
    }

    let output = Command::new(python)
        .arg(collector_path)
        .arg("--config")
        .arg(config_path)
        .output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Wayback collector failed with status {}", output.status);
        if !stdout.is_empty() {
            eprintln!("Collector stdout:\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("Collector stderr:\n{stderr}");
        }
        return Err(anyhow!("Wayback collector exited with failure"));
    }
    Ok(())
}

fn find_python_interpreter() -> Result<String> {
    match Command::new("python3").arg("--version").output() {
        Ok(_) => return Ok("python3".to_string()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(anyhow!("Failed to check python3: {err}"));
        }
    }

    match Command::new("python").arg("--version").output() {
        Ok(_) => Ok("python".to_string()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(anyhow!(
            "Python interpreter not found. Install python3 or ensure python is on PATH."
        )),
        Err(err) => Err(anyhow!("Failed to check python: {err}")),
    }
}

fn extract_text(config_path: PathBuf) -> Result<()> {
    ensure_config_path(&config_path)?;
    let python = find_python_interpreter()?;
    let extractor_path = Path::new("workers/parsers/extract_text.py");
    if !extractor_path.exists() {
        return Err(anyhow!(
            "Text extraction script not found: {}",
            extractor_path.display()
        ));
    }

    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));
    let artifacts_dir = storage.out_dir.join("artifacts");

    let output = Command::new(&python)
        .arg(extractor_path)
        .arg("--config")
        .arg(&config_path)
        .arg("--artifacts")
        .arg(&artifacts_dir)
        .output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Text extraction failed with status {}", output.status);
        if !stdout.is_empty() {
            eprintln!("Extractor stdout:\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("Extractor stderr:\n{stderr}");
        }
        return Err(anyhow!("Text extraction exited with failure"));
    }

    println!(
        "Text extraction completed for artifacts in {}",
        artifacts_dir.display()
    );
    Ok(())
}

fn tag_artifacts(config_path: PathBuf, force: bool) -> Result<()> {
    ensure_config_path(&config_path)?;
    let python = find_python_interpreter()?;
    let tagger_path = Path::new("workers/parsers/tag_artifacts.py");
    if !tagger_path.exists() {
        return Err(anyhow!(
            "Tagging script not found: {}",
            tagger_path.display()
        ));
    }

    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));
    let artifacts_dir = storage.out_dir.join("artifacts");

    let mut command = Command::new(&python);
    command
        .arg(tagger_path)
        .arg("--config")
        .arg(&config_path)
        .arg("--artifacts")
        .arg(&artifacts_dir);
    if force {
        command.arg("--force");
    }

    let output = command.output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Tagging failed with status {}", output.status);
        if !stdout.is_empty() {
            eprintln!("Tagger stdout:\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("Tagger stderr:\n{stderr}");
        }
        return Err(anyhow!("Tagging exited with failure"));
    }

    println!(
        "Tagging completed for artifacts in {}",
        artifacts_dir.display()
    );
    Ok(())
}

fn ingest_decisions(config_path: PathBuf) -> Result<()> {
    ensure_config_path(&config_path)?;
    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));
    let decisions_dir = storage.out_dir.join("decisions");

    if !decisions_dir.exists() {
        println!("No decisions directory found at {}", decisions_dir.display());
        return Ok(());
    }

    let mut decision_files: Vec<PathBuf> = fs::read_dir(&decisions_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    decision_files.sort();

    if decision_files.is_empty() {
        println!("No decision JSON files found in {}", decisions_dir.display());
        return Ok(());
    }

    let conn = civic_core::db::open(&storage.db_path)?;
    let mut ingested = 0usize;
    let mut failed = 0usize;

    for path in decision_files {
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
        let decision: civic_core::schema::DecisionBundle = match serde_json::from_value(raw_json.clone()) {
            Ok(decision) => decision,
            Err(err) => {
                failed += 1;
                eprintln!("Decision schema mismatch in {}: {err}", path.display());
                continue;
            }
        };

        if let Err(err) = civic_core::db::upsert_decision_meeting(
            &conn,
            &decision.meeting,
            &raw_json,
            &decision.motions,
        ) {
            failed += 1;
            eprintln!("Failed to ingest meeting {}: {err}", path.display());
            continue;
        }

        for motion in &decision.motions {
            let motion_json = serde_json::to_value(motion)?;
            if let Err(err) = civic_core::db::upsert_motion(&conn, motion, &motion_json) {
                failed += 1;
                eprintln!("Failed to ingest motion {}: {err}", motion.id);
            }
        }
        for vote in &decision.votes {
            let vote_json = serde_json::to_value(vote)?;
            if let Err(err) = civic_core::db::upsert_vote(&conn, vote, &vote_json) {
                failed += 1;
                eprintln!("Failed to ingest vote {}: {err}", vote.id);
            }
        }
        ingested += 1;
    }

    println!(
        "Ingested {} decision files, {} failed in {}",
        ingested,
        failed,
        decisions_dir.display()
    );
    Ok(())
}

fn score_weekly(config_path: PathBuf, date: Option<String>) -> Result<()> {
    ensure_config_path(&config_path)?;
    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));
    let rubric = Rubric::load_from_dir(Path::new("rubric"))?;

    let (_date_str, window_start, window_end) = resolve_window(date)?;
    let conn = civic_core::db::open(&storage.db_path)?;

    let meetings = load_meetings_in_window(&conn, &window_start, &window_end)?;
    if meetings.is_empty() {
        println!("motions_scored=0 votes_scored=0 insufficient=0 flagged=0");
        return Ok(());
    }

    let mut motion_scores: HashMap<String, ScoreResult> = HashMap::new();
    let mut scores_to_write: Vec<DecisionScore> = Vec::new();
    let mut motions_scored = 0usize;
    let mut votes_scored = 0usize;
    let mut insufficient = 0usize;
    let mut flagged = 0usize;
    let computed_at = window_end.clone();

    for meeting in &meetings {
        let artifacts = load_linked_artifacts(&conn, meeting)?;
        let motions = load_motions_for_meeting(&conn, &meeting.id)?;
        for motion in motions {
            let score = civic_core::scoring::compute_motion_score(
                &motion.text,
                &artifacts,
                &rubric,
            );
            if score.flags.iter().any(|flag| flag == "insufficient_evidence") {
                insufficient += 1;
            }
            if !score.flags.is_empty() {
                flagged += 1;
            }
            motions_scored += 1;
            motion_scores.insert(motion.id.clone(), score.clone());
            scores_to_write.push(DecisionScore {
                id: format!("motion:{}", motion.id),
                meeting_id: Some(meeting.id.clone()),
                motion_id: Some(motion.id.clone()),
                vote_id: None,
                overall_score: score.overall_score,
                axis_scores: score.axis_scores.clone(),
                constitutional_refs: score.constitutional_refs.clone(),
                evidence: score.evidence.clone(),
                confidence: score.confidence,
                flags: score.flags.clone(),
                computed_at: computed_at.clone(),
            });
        }

        let votes = load_votes_for_meeting(&conn, &meeting.id)?;
        for vote in votes {
            let Some(motion_score) = motion_scores.get(&vote.motion_id) else {
                continue;
            };
            let mut per_vote_scores = Vec::new();
            for (name, choice) in vote.choices {
                let mut score =
                    civic_core::scoring::compute_vote_score_with_motion(motion_score, choice, &rubric);
                score.evidence.push(format!("official:{name}"));
                let score_id = format!("vote:{}:{}", vote.id, slugify(&name));
                if score.flags.iter().any(|flag| flag == "insufficient_evidence") {
                    insufficient += 1;
                }
                if !score.flags.is_empty() {
                    flagged += 1;
                }
                votes_scored += 1;
                per_vote_scores.push((score_id, name, score));
            }

            for (score_id, name, score) in per_vote_scores {
                scores_to_write.push(DecisionScore {
                    id: score_id,
                    meeting_id: Some(meeting.id.clone()),
                    motion_id: Some(vote.motion_id.clone()),
                    vote_id: Some(vote.id.clone()),
                    overall_score: score.overall_score,
                    axis_scores: score.axis_scores.clone(),
                    constitutional_refs: score.constitutional_refs.clone(),
                    evidence: score.evidence.clone(),
                    confidence: score.confidence,
                    flags: score.flags.clone(),
                    computed_at: computed_at.clone(),
                });
            }
        }
    }

    for score in &scores_to_write {
        civic_core::db::upsert_decision_score(&conn, score)?;
    }

    let drift_flags = detect_drift(
        &conn,
        &rubric,
        &window_start,
        &window_end,
        &computed_at,
    )?;
    for score in drift_flags.updated_scores {
        civic_core::db::upsert_decision_score(&conn, &score)?;
    }

    println!(
        "motions_scored={} votes_scored={} insufficient={} flagged={}",
        motions_scored, votes_scored, insufficient, flagged
    );
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
        SELECT id, title, retrieved_at, source_value, tags_json
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
            tags_json: row.get(4)?,
        })
    })?;

    let mut artifacts = Vec::new();
    for row in rows {
        artifacts.push(row?);
    }

    let sort_key = |artifact: &&ReportArtifactRow| {
        (
            artifact.retrieved_at.clone(),
            artifact
                .title
                .clone()
                .unwrap_or_else(|| "(untitled)".to_string()),
        )
    };

    let report_dir = storage.vault_path.join("Reports").join("Weekly");
    fs::create_dir_all(&report_dir)?;
    let report_path = report_dir.join(format!("{date_str}.md"));

    let mut markdown = String::new();
    markdown.push_str(&format!("# Weekly Report {date_str}\n\n"));
    markdown.push_str(&format!("Window: {window_start} to {window_end} UTC\n\n"));
    let (mut high_impact, mut regular): (Vec<_>, Vec<_>) =
        artifacts.iter().partition(|artifact| artifact.is_high_impact());
    high_impact.sort_by_key(sort_key);
    regular.sort_by_key(sort_key);

    let decisions = load_decisions(&conn, &window_start, &window_end)?;
    let score_summary = load_score_summary(&conn, &window_start, &window_end)?;

    markdown.push_str(&format!("Total artifacts: {}\n\n", artifacts.len()));
    markdown.push_str("## High Impact\n\n");
    if high_impact.is_empty() {
        markdown.push_str("_No high impact artifacts in this window._\n\n");
    } else {
        for artifact in &high_impact {
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
        markdown.push('\n');
    }

    markdown.push_str("## All Artifacts\n\n");
    for artifact in &regular {
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
    markdown.push('\n');

    markdown.push_str("## Decisions This Week\n\n");
    if decisions.is_empty() {
        markdown.push_str("_No decisions parsed this week._\n");
    } else {
        for meeting in &decisions {
            markdown.push_str(&format!(
                "- {} — {}\n",
                meeting.started_at, meeting.body_name
            ));
            for motion in &meeting.motions {
                let outcome = motion
                    .result
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                markdown.push_str(&format!("  - {} ({})\n", motion.text, outcome));
            }
        }
    }
    markdown.push('\n');

    markdown.push_str("## Rubric Alignment (This Week)\n\n");
    if score_summary.total_scored == 0 {
        markdown.push_str("_No decision scores available this week._\n");
    } else {
        markdown.push_str(&format!(
            "- Average score: {:.1}\n",
            score_summary.average_score
        ));
        markdown.push_str(&format!(
            "- Insufficient evidence: {}\n",
            score_summary.insufficient_count
        ));
        if !score_summary.top_positive.is_empty() {
            markdown.push_str("- Top positive decisions:\n");
            for entry in &score_summary.top_positive {
                markdown.push_str(&format!(
                    "  - {} ({})\n",
                    entry.text, entry.overall_score
                ));
            }
        }
        if !score_summary.top_negative.is_empty() {
            markdown.push_str("- Top negative decisions:\n");
            for entry in &score_summary.top_negative {
                markdown.push_str(&format!(
                    "  - {} ({})\n",
                    entry.text, entry.overall_score
                ));
            }
        }
        if !score_summary.drift_flags.is_empty() {
            markdown.push_str("- Drift flags:\n");
            for flag in &score_summary.drift_flags {
                markdown.push_str(&format!("  - {flag}\n"));
            }
        }
    }
    fs::write(&report_path, markdown)?;

    let report_json_dir = storage
        .out_dir
        .join("reports")
        .join("weekly");
    fs::create_dir_all(&report_json_dir)?;
    let report_json_path = report_json_dir.join(format!("{date_str}.json"));
    let ordered_artifacts: Vec<&ReportArtifactRow> =
        high_impact.iter().chain(regular.iter()).copied().collect();
    let extracted_count = ordered_artifacts
        .iter()
        .filter(|artifact| artifact.is_text_extracted())
        .count();
    let mut issue_counts: BTreeMap<String, usize> = BTreeMap::new();
    for artifact in &artifacts {
        for tag in parse_tags_json(&artifact.tags_json) {
            if is_issue_tag(&tag) {
                *issue_counts.entry(tag).or_insert(0) += 1;
            }
        }
    }
    let mut issue_counts_vec: Vec<(String, usize)> = issue_counts.into_iter().collect();
    issue_counts_vec.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let issue_tag_counts = issue_counts_vec
        .into_iter()
        .take(10)
        .map(|(tag, count)| serde_json::json!({ "tag": tag, "count": count }))
        .collect::<Vec<_>>();

    let json_payload = serde_json::json!({
        "date": date_str,
        "window_start": window_start,
        "window_end": window_end,
        "total": artifacts.len(),
        "text_extracted_total": extracted_count,
        "issue_tag_counts": issue_tag_counts,
        "rubric_alignment": score_summary.to_json(),
        "decisions": decisions.iter().map(|meeting| {
            serde_json::json!({
                "meeting_id": meeting.id,
                "body_id": meeting.body_id,
                "body_name": meeting.body_name,
                "started_at": meeting.started_at,
                "motions": meeting.motions.iter().map(|motion| {
                    serde_json::json!({
                        "id": motion.id,
                        "text": motion.text,
                        "result": motion.result,
                    })
                }).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>(),
        "artifacts": ordered_artifacts.iter().map(|artifact| {
            serde_json::json!({
                "id": artifact.id,
                "title": artifact.title,
                "retrieved_at": artifact.retrieved_at,
                "source_value": artifact.source_value,
                "extracted": artifact.is_text_extracted(),
            })
        }).collect::<Vec<_>>()
    });
    fs::write(&report_json_path, serde_json::to_string_pretty(&json_payload)?)?;

    println!("Weekly report written to {}", report_path.display());
    Ok(())
}

fn digest_weekly() -> Result<()> {
    println!("digest-weekly is not implemented yet.");
    Ok(())
}

fn publish_placeholder() -> Result<()> {
    println!("publish is not implemented yet.");
    Ok(())
}

struct ReportArtifactRow {
    id: String,
    title: Option<String>,
    retrieved_at: String,
    source_value: String,
    tags_json: String,
}

struct ReportDecisionMotion {
    id: String,
    text: String,
    result: Option<String>,
}

struct ReportDecisionMeeting {
    id: String,
    body_id: String,
    body_name: String,
    started_at: String,
    motions: Vec<ReportDecisionMotion>,
}

struct MeetingWindowRow {
    id: String,
    body_id: String,
    started_at: String,
    artifact_ids_json: String,
}

struct MotionRow {
    id: String,
    text: String,
}

struct VoteRow {
    id: String,
    motion_id: String,
    ayes: Vec<String>,
    nays: Vec<String>,
    abstain: Vec<String>,
    choices: Vec<(String, VoteChoice)>,
}

struct DriftDetectionResult {
    updated_scores: Vec<DecisionScore>,
    drift_flags: Vec<String>,
}

struct ScoreDecisionEntry {
    text: String,
    overall_score: f64,
}

struct ScoreSummary {
    average_score: f64,
    total_scored: usize,
    insufficient_count: usize,
    top_positive: Vec<ScoreDecisionEntry>,
    top_negative: Vec<ScoreDecisionEntry>,
    drift_flags: Vec<String>,
}

impl ScoreSummary {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "average_score": self.average_score,
            "total_scored": self.total_scored,
            "insufficient_count": self.insufficient_count,
            "top_positive": self.top_positive.iter().map(|entry| {
                serde_json::json!({
                    "text": entry.text,
                    "overall_score": entry.overall_score,
                })
            }).collect::<Vec<_>>(),
            "top_negative": self.top_negative.iter().map(|entry| {
                serde_json::json!({
                    "text": entry.text,
                    "overall_score": entry.overall_score,
                })
            }).collect::<Vec<_>>(),
            "drift_flags": self.drift_flags,
        })
    }
}

impl ReportArtifactRow {
    fn is_high_impact(&self) -> bool {
        parse_tags_json(&self.tags_json)
            .iter()
            .any(|tag| tag == "high_impact")
    }

    fn is_text_extracted(&self) -> bool {
        parse_tags_json(&self.tags_json)
            .iter()
            .any(|tag| tag == "text_extracted")
    }
}

fn parse_tags_json(tags_json: &str) -> Vec<String> {
    serde_json::from_str(tags_json).unwrap_or_default()
}

fn resolve_window(date: Option<String>) -> Result<(String, String, String)> {
    let date_format: &[FormatItem<'_>] = time::macros::format_description!("[year]-[month]-[day]");
    let now = OffsetDateTime::now_utc();
    if let Some(date_value) = date {
        let parsed = time::Date::parse(&date_value, date_format)
            .map_err(|err| anyhow!("Invalid date {date_value}: {err}"))?;
        let end = parsed.next_day().unwrap_or(parsed);
        let end_dt = end.with_time(time::Time::MIDNIGHT).assume_utc();
        let start_dt = end_dt - Duration::days(7);
        let date_str = parsed.format(date_format)?;
        let window_start = start_dt.format(&Rfc3339)?;
        let window_end = end_dt.format(&Rfc3339)?;
        return Ok((date_str, window_start, window_end));
    }
    let date_str = now.format(date_format)?;
    let window_end = now.format(&Rfc3339)?;
    let window_start = (now - Duration::days(7)).format(&Rfc3339)?;
    Ok((date_str, window_start, window_end))
}

fn load_meetings_in_window(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
) -> Result<Vec<MeetingWindowRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, body_id, started_at, artifact_ids_json
        FROM meetings
        WHERE datetime(started_at) >= datetime(?1)
          AND datetime(started_at) <= datetime(?2)
        ORDER BY started_at ASC, id ASC
        "#,
    )?;
    let rows = stmt.query_map([window_start, window_end], |row| {
        Ok(MeetingWindowRow {
            id: row.get(0)?,
            body_id: row.get(1)?,
            started_at: row.get(2)?,
            artifact_ids_json: row.get(3)?,
        })
    })?;
    let mut meetings = Vec::new();
    for row in rows {
        meetings.push(row?);
    }
    Ok(meetings)
}

fn load_linked_artifacts(
    conn: &rusqlite::Connection,
    meeting: &MeetingWindowRow,
) -> Result<Vec<LinkedArtifact>> {
    let artifact_ids: Vec<String> =
        serde_json::from_str(&meeting.artifact_ids_json).unwrap_or_default();
    let mut artifacts = Vec::new();
    for artifact_id in artifact_ids {
        let mut stmt = conn.prepare(
            r#"
            SELECT id, tags_json
            FROM artifacts
            WHERE id = ?1
            "#,
        )?;
        let mut rows = stmt.query([artifact_id.as_str()])?;
        if let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let tags_json: String = row.get(1)?;
            artifacts.push(LinkedArtifact {
                id,
                tags: parse_tags_json(&tags_json),
            });
        }
    }
    Ok(artifacts)
}

fn load_motions_for_meeting(conn: &rusqlite::Connection, meeting_id: &str) -> Result<Vec<MotionRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, text
        FROM motions
        WHERE meeting_id = ?1
        ORDER BY motion_index ASC, id ASC
        "#,
    )?;
    let rows = stmt.query_map([meeting_id], |row| {
        Ok(MotionRow {
            id: row.get(0)?,
            text: row.get(1)?,
        })
    })?;
    let mut motions = Vec::new();
    for row in rows {
        motions.push(row?);
    }
    Ok(motions)
}

fn load_votes_for_meeting(conn: &rusqlite::Connection, meeting_id: &str) -> Result<Vec<VoteRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT votes.id, votes.motion_id, votes.ayes_json, votes.nays_json, votes.abstain_json
        FROM votes
        JOIN motions ON votes.motion_id = motions.id
        WHERE motions.meeting_id = ?1
        ORDER BY votes.id ASC
        "#,
    )?;
    let rows = stmt.query_map([meeting_id], |row| {
        let ayes_json: String = row.get(2)?;
        let nays_json: String = row.get(3)?;
        let abstain_json: String = row.get(4)?;
        let ayes: Vec<String> = serde_json::from_str(&ayes_json).unwrap_or_default();
        let nays: Vec<String> = serde_json::from_str(&nays_json).unwrap_or_default();
        let abstain: Vec<String> = serde_json::from_str(&abstain_json).unwrap_or_default();
        Ok(VoteRow {
            id: row.get(0)?,
            motion_id: row.get(1)?,
            ayes: ayes.clone(),
            nays: nays.clone(),
            abstain: abstain.clone(),
            choices: build_vote_choices(&ayes, &nays, &abstain),
        })
    })?;
    let mut votes = Vec::new();
    for row in rows {
        votes.push(row?);
    }
    Ok(votes)
}

fn build_vote_choices(
    ayes: &[String],
    nays: &[String],
    abstain: &[String],
) -> Vec<(String, VoteChoice)> {
    let mut choices = Vec::new();
    for name in ayes {
        choices.push((name.to_string(), VoteChoice::Aye));
    }
    for name in nays {
        choices.push((name.to_string(), VoteChoice::Nay));
    }
    for name in abstain {
        choices.push((name.to_string(), VoteChoice::Abstain));
    }
    choices.sort_by(|a, b| a.0.cmp(&b.0));
    choices
}

fn slugify(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch.to_ascii_lowercase() } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

fn detect_drift(
    conn: &rusqlite::Connection,
    rubric: &Rubric,
    window_start: &str,
    window_end: &str,
    computed_at: &str,
) -> Result<DriftDetectionResult> {
    let current_scores = load_vote_scores(conn, window_start, window_end)?;
    let mut updated_scores = Vec::new();
    let mut drift_flags = Vec::new();

    for (official, axis_scores) in current_scores {
        for (axis, current_avg) in axis_scores {
            let prior_scores = load_prior_vote_scores(
                conn,
                &official,
                &axis,
                window_start,
                rubric.bias_controls.drift_window,
            )?;
            if prior_scores.len() < rubric.bias_controls.drift_window {
                continue;
            }
            let prior_avg = average(&prior_scores);
            let deviation = current_avg - prior_avg;
            if deviation.abs() >= rubric.bias_controls.drift_threshold {
                let flag = format!("drift_detected:{axis}");
                drift_flags.push(format!("{official}:{flag}"));
                let drift_id = format!("drift:{}:{}:{}", slugify(&official), axis, window_end);
                civic_core::db::upsert_official_drift(
                    conn,
                    &drift_id,
                    &official,
                    &axis,
                    prior_avg,
                    current_avg,
                    deviation,
                    &[flag.clone()],
                    computed_at,
                )?;
                let scores = load_scores_for_official_in_window(conn, &official, window_start, window_end)?;
                for mut score in scores {
                    if !score.flags.contains(&flag) {
                        score.flags.push(flag.clone());
                    }
                    updated_scores.push(score);
                }
            }
        }
    }

    Ok(DriftDetectionResult {
        updated_scores,
        drift_flags,
    })
}

fn load_vote_scores(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
) -> Result<HashMap<String, HashMap<String, f64>>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT decision_scores.axis_json, decision_scores.evidence_json
        FROM decision_scores
        WHERE vote_id IS NOT NULL
          AND datetime(computed_at) >= datetime(?1)
          AND datetime(computed_at) <= datetime(?2)
        "#,
    )?;
    let rows = stmt.query_map([window_start, window_end], |row| {
        let axis_json: String = row.get(0)?;
        let evidence_json: String = row.get(1)?;
        let axis_scores: HashMap<String, f64> =
            serde_json::from_str(&axis_json).unwrap_or_default();
        let evidence: Vec<String> = serde_json::from_str(&evidence_json).unwrap_or_default();
        Ok((axis_scores, evidence))
    })?;

    let mut official_axes: HashMap<String, HashMap<String, Vec<f64>>> = HashMap::new();
    for row in rows {
        let (axis_scores, evidence) = row?;
        let official = extract_official(&evidence);
        let Some(official) = official else { continue };
        let axes = official_axes.entry(official).or_default();
        for (axis, score) in axis_scores {
            axes.entry(axis).or_default().push(score);
        }
    }

    let mut averages = HashMap::new();
    for (official, axes) in official_axes {
        let mut axis_avg = HashMap::new();
        for (axis, values) in axes {
            axis_avg.insert(axis, average(&values));
        }
        averages.insert(official, axis_avg);
    }
    Ok(averages)
}

fn load_prior_vote_scores(
    conn: &rusqlite::Connection,
    official: &str,
    axis: &str,
    window_start: &str,
    limit: usize,
) -> Result<Vec<f64>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT axis_json, evidence_json
        FROM decision_scores
        WHERE vote_id IS NOT NULL
          AND datetime(computed_at) < datetime(?1)
        ORDER BY computed_at DESC
        "#,
    )?;
    let rows = stmt.query_map([window_start], |row| {
        let axis_json: String = row.get(0)?;
        let evidence_json: String = row.get(1)?;
        Ok((axis_json, evidence_json))
    })?;
    let mut scores = Vec::new();
    for row in rows {
        let (axis_json, evidence_json) = row?;
        let evidence: Vec<String> = serde_json::from_str(&evidence_json).unwrap_or_default();
        if extract_official(&evidence).as_deref() != Some(official) {
            continue;
        }
        let axis_scores: HashMap<String, f64> =
            serde_json::from_str(&axis_json).unwrap_or_default();
        if let Some(score) = axis_scores.get(axis) {
            scores.push(*score);
        }
        if scores.len() >= limit {
            break;
        }
    }
    Ok(scores)
}

fn load_scores_for_official_in_window(
    conn: &rusqlite::Connection,
    official: &str,
    window_start: &str,
    window_end: &str,
) -> Result<Vec<DecisionScore>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, meeting_id, motion_id, vote_id, overall_score, axis_json, refs_json,
               evidence_json, confidence, flags_json, computed_at
        FROM decision_scores
        WHERE vote_id IS NOT NULL
          AND datetime(computed_at) >= datetime(?1)
          AND datetime(computed_at) <= datetime(?2)
        "#,
    )?;
    let rows = stmt.query_map([window_start, window_end], |row| {
        let axis_json: String = row.get(5)?;
        let refs_json: String = row.get(6)?;
        let evidence_json: String = row.get(7)?;
        let flags_json: String = row.get(9)?;
        let axis_scores: HashMap<String, f64> =
            serde_json::from_str(&axis_json).unwrap_or_default();
        let refs: Vec<String> = serde_json::from_str(&refs_json).unwrap_or_default();
        let evidence: Vec<String> = serde_json::from_str(&evidence_json).unwrap_or_default();
        let flags: Vec<String> = serde_json::from_str(&flags_json).unwrap_or_default();
        Ok(DecisionScore {
            id: row.get(0)?,
            meeting_id: row.get(1)?,
            motion_id: row.get(2)?,
            vote_id: row.get(3)?,
            overall_score: row.get(4)?,
            axis_scores,
            constitutional_refs: refs,
            evidence,
            confidence: row.get(8)?,
            flags,
            computed_at: row.get(10)?,
        })
    })?;
    let mut results = Vec::new();
    for row in rows {
        let score = row?;
        if extract_official(&score.evidence).as_deref() != Some(official) {
            continue;
        }
        results.push(score);
    }
    Ok(results)
}

fn extract_official(evidence: &[String]) -> Option<String> {
    evidence.iter().find_map(|item| {
        item.strip_prefix("official:").map(|value| value.to_string())
    })
}

fn average(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn load_decisions(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
) -> Result<Vec<ReportDecisionMeeting>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT meetings.id, meetings.body_id, meetings.started_at, bodies.name
        FROM meetings
        JOIN bodies ON meetings.body_id = bodies.id
        WHERE datetime(meetings.started_at) >= datetime(?1)
          AND datetime(meetings.started_at) <= datetime(?2)
        ORDER BY meetings.started_at ASC, meetings.id ASC
        "#,
    )?;

    let meetings = stmt.query_map([window_start, window_end], |row| {
        Ok(ReportDecisionMeeting {
            id: row.get(0)?,
            body_id: row.get(1)?,
            started_at: row.get(2)?,
            body_name: row.get(3)?,
            motions: Vec::new(),
        })
    })?;

    let mut results = Vec::new();
    for meeting in meetings {
        let mut meeting = meeting?;
        let mut motion_stmt = conn.prepare(
            r#"
            SELECT id, text, result
            FROM motions
            WHERE meeting_id = ?1
            ORDER BY motion_index ASC, id ASC
            "#,
        )?;
        let motions = motion_stmt.query_map([meeting.id.as_str()], |row| {
            Ok(ReportDecisionMotion {
                id: row.get(0)?,
                text: row.get(1)?,
                result: row.get(2)?,
            })
        })?;
        meeting.motions = motions.filter_map(|row| row.ok()).collect();
        results.push(meeting);
    }
    Ok(results)
}

fn load_score_summary(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
) -> Result<ScoreSummary> {
    let mut stmt = conn.prepare(
        r#"
        SELECT decision_scores.overall_score, decision_scores.flags_json, motions.text
        FROM decision_scores
        JOIN motions ON decision_scores.motion_id = motions.id
        JOIN meetings ON motions.meeting_id = meetings.id
        WHERE decision_scores.motion_id IS NOT NULL
          AND datetime(meetings.started_at) >= datetime(?1)
          AND datetime(meetings.started_at) <= datetime(?2)
        "#,
    )?;
    let rows = stmt.query_map([window_start, window_end], |row| {
        let flags_json: String = row.get(1)?;
        let flags: Vec<String> = serde_json::from_str(&flags_json).unwrap_or_default();
        Ok((row.get::<_, f64>(0)?, flags, row.get::<_, String>(2)?))
    })?;

    let mut scores = Vec::new();
    let mut insufficient_count = 0usize;
    for row in rows {
        let (score, flags, text) = row?;
        if flags.iter().any(|flag| flag == "insufficient_evidence") {
            insufficient_count += 1;
        }
        scores.push((score, text));
    }

    let total_scored = scores.len();
    let average_score = if total_scored == 0 {
        0.0
    } else {
        scores.iter().map(|(score, _)| score).sum::<f64>() / total_scored as f64
    };

    scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let top_negative = scores
        .iter()
        .take(3)
        .map(|(score, text)| ScoreDecisionEntry {
            text: text.clone(),
            overall_score: *score,
        })
        .collect::<Vec<_>>();
    let top_positive = scores
        .iter()
        .rev()
        .take(3)
        .map(|(score, text)| ScoreDecisionEntry {
            text: text.clone(),
            overall_score: *score,
        })
        .collect::<Vec<_>>();

    let drift_flags = load_drift_flags(conn, window_start, window_end)?;

    Ok(ScoreSummary {
        average_score,
        total_scored,
        insufficient_count,
        top_positive,
        top_negative,
        drift_flags,
    })
}

fn load_drift_flags(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT official_name, axis, deviation
        FROM official_drift
        WHERE datetime(computed_at) >= datetime(?1)
          AND datetime(computed_at) <= datetime(?2)
        ORDER BY computed_at DESC
        "#,
    )?;
    let rows = stmt.query_map([window_start, window_end], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
        ))
    })?;
    let mut flags = Vec::new();
    for row in rows {
        let (official, axis, deviation) = row?;
        flags.push(format!("{official}: drift_detected:{axis} ({deviation:.2})"));
    }
    Ok(flags)
}

fn is_issue_tag(tag: &str) -> bool {
    const ISSUE_TAGS: &[&str] = &[
        "zoning",
        "rezoning",
        "variance",
        "planning_commission",
        "budget",
        "tax",
        "bond",
        "appropriation",
        "contract",
        "bid",
        "procurement",
        "election",
        "clerk",
        "ballot",
        "school_board",
        "curriculum",
        "policy",
        "lawsuit",
        "settlement",
        "ordinance",
        "public_safety",
        "land_sale",
        "eminent_domain",
    ];
    ISSUE_TAGS.iter().any(|issue| *issue == tag)
}
