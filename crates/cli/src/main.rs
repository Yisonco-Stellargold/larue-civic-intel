use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use schemars::schema_for;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::path::Path;
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
