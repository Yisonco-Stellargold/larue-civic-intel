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
use time::{Duration, Month, OffsetDateTime};

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
    /// Export static site bundle
    ExportSite {
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
        Commands::ScoreWeekly { config, date } => score_weekly(config, date),
        Commands::ExportSite { config } => export_site(config),
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
    site: Option<SiteConfig>,
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

#[derive(Debug, Deserialize)]
struct SiteConfig {
    enable_commentary: Option<bool>,
    commentary_style: Option<String>,
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

    let mut entries = fs::read_dir(&dir)?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            skipped += 1;
            continue;
        }
        let filename = path.file_name().and_then(|value| value.to_str()).unwrap_or("");
        if filename.ends_with("_manifest.json")
            || filename.ends_with("_state.json")
            || filename.ends_with(".schema.json")
        {
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
        if let Err(err) = serde_json::from_value::<civic_core::schema::Artifact>(raw_json.clone()) {
            skipped += 1;
            eprintln!("Skipping non-artifact JSON {}: {err}", path.display());
            continue;
        }
        let artifact_id = match raw_json.get("id").and_then(|value| value.as_str()) {
            Some(value) => value,
            None => {
                skipped += 1;
                eprintln!("Skipping artifact without id in {}", path.display());
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
    if let Err(err) = export_site(config_path.clone()) {
        eprintln!("Warning: export-site failed: {err}");
    }
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

            for (score_id, _name, score) in per_vote_scores {
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

fn export_site(config_path: PathBuf) -> Result<()> {
    ensure_config_path(&config_path)?;
    let config = load_config(&config_path)?;
    let storage = resolve_storage(Some(&config));
    let site = resolve_site_config(config.site.as_ref());
    let rubric = Rubric::load_from_dir(Path::new("rubric")).ok();

    let mut reports = load_week_reports(&storage.out_dir)?;
    let (latest_date, window_start, window_end) = if let Some(report) = reports.last() {
        (
            report.date.clone(),
            report.window_start.clone(),
            report.window_end.clone(),
        )
    } else {
        resolve_window(None)?
    };
    if reports.is_empty() {
        reports.push(build_placeholder_report(&latest_date, &window_start, &window_end));
    }
    let latest_report = reports.last();

    let conn = civic_core::db::open(&storage.db_path)?;
    let mut official_stats = load_official_summaries(
        &conn,
        &window_start,
        &window_end,
        rubric.as_ref(),
        latest_report,
        &latest_date,
    )?;
    let previous_average = if reports.len() > 1 {
        let previous_report = &reports[reports.len() - 2];
        load_official_averages(&conn, &previous_report.window_start, &previous_report.window_end)?
    } else {
        HashMap::new()
    };

    for summary in &mut official_stats {
        summary.delta = summary.average_score
            - previous_average
                .get(&summary.name)
                .copied()
                .unwrap_or(summary.average_score);
        let prior_score = previous_average
            .get(&summary.name)
            .copied()
            .unwrap_or(summary.average_score);
        let prior_grade = score_to_grade(normalize_score(prior_score, rubric.as_ref().map(|rub| &rub.config)));
        summary.commentary = build_commentary_line(
            &summary.id,
            &latest_date,
            &summary.letter_grade,
            &prior_grade.1,
            summary.delta,
            !summary.drift_flags.is_empty(),
            &summary.top_issue_tags,
            &site,
        );
    }

    let site_dir = storage.out_dir.join("site");
    let assets_dir = site_dir.join("assets");
    let stockade_dir = site_dir.join("stockade");
    let officials_dir = site_dir.join("officials");
    let weeks_dir = site_dir.join("weeks");
    let reports_dir = site_dir.join("reports").join("weekly");
    let artifacts_dir = site_dir.join("artifacts");
    fs::create_dir_all(&assets_dir)?;
    fs::create_dir_all(&stockade_dir)?;
    fs::create_dir_all(&officials_dir)?;
    fs::create_dir_all(&weeks_dir)?;
    fs::create_dir_all(&reports_dir)?;
    fs::create_dir_all(&artifacts_dir)?;

    write_site_assets(&assets_dir)?;
    copy_report_jsons(&storage.out_dir, &reports_dir)?;
    export_artifact_jsons(&storage.out_dir, &artifacts_dir)?;

    let home_html = render_home_page(latest_report, &latest_date, &official_stats);
    fs::write(site_dir.join("index.html"), home_html)?;

    let stockade_html = render_stockade_page(&official_stats, &latest_date);
    fs::write(stockade_dir.join("index.html"), stockade_html)?;

    let officials_index = render_officials_index(&official_stats, &latest_date);
    fs::write(officials_dir.join("index.html"), officials_index)?;

    for official in &official_stats {
        let detail_html = render_official_detail(official, &latest_date);
        fs::write(
            officials_dir.join(format!("{}.html", official.id)),
            detail_html,
        )?;
    }

    for report in &reports {
        let week_html = render_week_page(report, &latest_date);
        fs::write(weeks_dir.join(format!("{}.html", report.date)), week_html)?;
    }

    println!("Site export completed at {}", site_dir.display());
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

struct WeekReport {
    date: String,
    window_start: String,
    window_end: String,
    issue_tag_counts: Vec<(String, usize)>,
    rubric_average: f64,
    decisions: Vec<WeekDecision>,
    artifacts: Vec<WeekArtifact>,
}

struct WeekDecision {
    body_name: String,
    started_at: String,
    motions: Vec<WeekMotion>,
}

struct WeekMotion {
    text: String,
    result: Option<String>,
}

struct WeekArtifact {
    title: String,
    source_value: String,
}

struct OfficialSummary {
    id: String,
    name: String,
    average_score: f64,
    axis_scores: HashMap<String, f64>,
    axis_scores_normalized: HashMap<String, f64>,
    letter_grade: String,
    numeric_grade: f64,
    delta: f64,
    drift_flags: Vec<String>,
    insufficient: bool,
    receipts: Vec<Receipt>,
    top_issue_tags: Vec<String>,
    commentary: Option<String>,
}

struct Receipt {
    meeting_date: String,
    motion_text: String,
    artifact_ids: Vec<String>,
    week_date: String,
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
        let parsed = parse_date_ymd(&date_value)?;
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

fn parse_date_ymd(date_value: &str) -> Result<time::Date> {
    let mut parts = date_value.split('-');
    let year_str = parts.next().unwrap_or("");
    let month_str = parts.next().unwrap_or("");
    let day_str = parts.next().unwrap_or("");
    if parts.next().is_some() || year_str.is_empty() || month_str.is_empty() || day_str.is_empty() {
        return Err(anyhow!(
            "Invalid date {date_value}: expected format YYYY-MM-DD"
        ));
    }
    let year: i32 = year_str
        .parse()
        .map_err(|err| anyhow!("Invalid date {date_value}: invalid year ({err})"))?;
    let month: u8 = month_str
        .parse()
        .map_err(|err| anyhow!("Invalid date {date_value}: invalid month ({err})"))?;
    let day: u8 = day_str
        .parse()
        .map_err(|err| anyhow!("Invalid date {date_value}: invalid day ({err})"))?;
    let month = Month::try_from(month)
        .map_err(|err| anyhow!("Invalid date {date_value}: invalid month ({err})"))?;
    time::Date::from_calendar_date(year, month, day)
        .map_err(|err| anyhow!("Invalid date {date_value}: {err}"))
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
    let order_by = if motions_has_index(conn)? {
        "ORDER BY motion_index ASC, id ASC"
    } else {
        "ORDER BY id ASC"
    };
    let mut stmt = conn.prepare(&format!(
        r#"
        SELECT id, text
        FROM motions
        WHERE meeting_id = ?1
        {order_by}
        "#
    ))?;
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

fn motions_has_index(conn: &rusqlite::Connection) -> Result<bool> {
    let mut stmt = conn.prepare("PRAGMA table_info(motions)")?;
    let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for column in columns {
        if column? == "motion_index" {
            return Ok(true);
        }
    }
    Ok(false)
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
            SELECT id, COALESCE(text, '') as text, result
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
        SELECT decision_scores.overall_score, decision_scores.flags_json, COALESCE(motions.text, '')
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

fn resolve_site_config(config: Option<&SiteConfig>) -> SiteConfig {
    SiteConfig {
        enable_commentary: Some(config.and_then(|value| value.enable_commentary).unwrap_or(true)),
        commentary_style: config
            .and_then(|value| value.commentary_style.clone())
            .or(Some("satire".to_string())),
    }
}

fn load_week_reports(out_dir: &Path) -> Result<Vec<WeekReport>> {
    let reports_dir = out_dir.join("reports").join("weekly");
    if !reports_dir.exists() {
        return Ok(Vec::new());
    }
    let mut reports: Vec<WeekReport> = Vec::new();
    let mut entries: Vec<PathBuf> = fs::read_dir(&reports_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    entries.sort();
    for path in entries {
        let raw = fs::read_to_string(&path)?;
        let value: serde_json::Value = serde_json::from_str(&raw)?;
        let Some(date) = value.get("date").and_then(|value| value.as_str()) else {
            continue;
        };
        let window_start = value
            .get("window_start")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();
        let window_end = value
            .get("window_end")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();
        let issue_tag_counts = value
            .get("issue_tag_counts")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let tag = item.get("tag")?.as_str()?.to_string();
                        let count = item.get("count")?.as_u64()? as usize;
                        Some((tag, count))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let rubric_average = value
            .get("rubric_alignment")
            .and_then(|value| value.get("average_score"))
            .and_then(|value| value.as_f64())
            .unwrap_or(0.0);
        let decisions = parse_week_decisions(&value);
        let artifacts = value
            .get("artifacts")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        Some(WeekArtifact {
                            title: item
                                .get("title")
                                .and_then(|value| value.as_str())
                                .unwrap_or("(untitled)")
                                .to_string(),
                            source_value: item
                                .get("source_value")
                                .and_then(|value| value.as_str())
                                .unwrap_or("")
                                .to_string(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        reports.push(WeekReport {
            date: date.to_string(),
            window_start,
            window_end,
            issue_tag_counts,
            rubric_average,
            decisions,
            artifacts,
        });
    }
    reports.sort_by(|a, b| a.date.cmp(&b.date));
    Ok(reports)
}

fn build_placeholder_report(date: &str, window_start: &str, window_end: &str) -> WeekReport {
    WeekReport {
        date: date.to_string(),
        window_start: window_start.to_string(),
        window_end: window_end.to_string(),
        issue_tag_counts: Vec::new(),
        rubric_average: 0.0,
        decisions: Vec::new(),
        artifacts: Vec::new(),
    }
}

fn parse_week_decisions(value: &serde_json::Value) -> Vec<WeekDecision> {
    let decisions = value.get("decisions").and_then(|value| value.as_array());
    let Some(decisions) = decisions else {
        return Vec::new();
    };
    decisions
        .iter()
        .map(|decision| {
            let body_name = decision
                .get("body_name")
                .and_then(|value| value.as_str())
                .unwrap_or("Unknown Body")
                .to_string();
            let started_at = decision
                .get("started_at")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let motions = decision
                .get("motions")
                .and_then(|value| value.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            Some(WeekMotion {
                                text: item
                                    .get("text")
                                    .and_then(|value| value.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                result: item
                                    .get("result")
                                    .and_then(|value| value.as_str())
                                    .map(|value| value.to_string()),
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            WeekDecision {
                body_name,
                started_at,
                motions,
            }
        })
        .collect()
}

fn load_official_summaries(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
    rubric: Option<&Rubric>,
    report: Option<&WeekReport>,
    week_date: &str,
) -> Result<Vec<OfficialSummary>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT decision_scores.overall_score, decision_scores.axis_json,
               decision_scores.flags_json, decision_scores.evidence_json,
               COALESCE(motions.text, ''), meetings.started_at, meetings.artifact_ids_json
        FROM decision_scores
        JOIN motions ON decision_scores.motion_id = motions.id
        JOIN meetings ON motions.meeting_id = meetings.id
        WHERE decision_scores.vote_id IS NOT NULL
          AND datetime(meetings.started_at) >= datetime(?1)
          AND datetime(meetings.started_at) <= datetime(?2)
        "#,
    )?;

    let rows = stmt.query_map([window_start, window_end], |row| {
        let overall_score: f64 = row.get(0)?;
        let axis_json: String = row.get(1)?;
        let flags_json: String = row.get(2)?;
        let evidence_json: String = row.get(3)?;
        let motion_text: String = row.get(4)?;
        let started_at: String = row.get(5)?;
        let artifact_ids_json: String = row.get(6)?;
        Ok((
            overall_score,
            axis_json,
            flags_json,
            evidence_json,
            motion_text,
            started_at,
            artifact_ids_json,
        ))
    })?;

    let mut data: HashMap<String, OfficialSummaryBuilder> = HashMap::new();
    for row in rows {
        let (
            overall_score,
            axis_json,
            flags_json,
            evidence_json,
            motion_text,
            started_at,
            artifact_ids_json,
        ) = row?;
        let evidence: Vec<String> = serde_json::from_str(&evidence_json).unwrap_or_default();
        let Some(official) = extract_official(&evidence) else {
            continue;
        };
        let axis_scores: HashMap<String, f64> =
            serde_json::from_str(&axis_json).unwrap_or_default();
        let flags: Vec<String> = serde_json::from_str(&flags_json).unwrap_or_default();
        let artifact_ids: Vec<String> =
            serde_json::from_str(&artifact_ids_json).unwrap_or_default();

        let entry = data
            .entry(official.clone())
            .or_insert_with(|| OfficialSummaryBuilder::new(&official, report, week_date));
        entry.overall_scores.push(overall_score);
        entry.axis_scores.push(axis_scores);
        entry.insufficient |= flags.iter().any(|flag| flag == "insufficient_evidence");
        entry.receipts.push(Receipt {
            meeting_date: started_at.clone(),
            motion_text: motion_text.clone(),
            artifact_ids,
            week_date: report
                .map(|rep| rep.date.clone())
                .unwrap_or_else(|| week_date.to_string()),
        });
    }

    let drift_flags = load_drift_flags(conn, window_start, window_end)?;
    let rubric_config = rubric.map(|value| &value.config);

    let mut summaries = Vec::new();
    for (_, builder) in data {
        summaries.push(builder.build(rubric_config, &drift_flags));
    }
    summaries.sort_by(|a, b| {
        b.average_score
            .partial_cmp(&a.average_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    Ok(summaries)
}

fn load_official_averages(
    conn: &rusqlite::Connection,
    window_start: &str,
    window_end: &str,
) -> Result<HashMap<String, f64>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT decision_scores.overall_score, decision_scores.evidence_json
        FROM decision_scores
        WHERE vote_id IS NOT NULL
          AND datetime(computed_at) >= datetime(?1)
          AND datetime(computed_at) <= datetime(?2)
        "#,
    )?;
    let rows = stmt.query_map([window_start, window_end], |row| {
        let score: f64 = row.get(0)?;
        let evidence_json: String = row.get(1)?;
        Ok((score, evidence_json))
    })?;
    let mut totals: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows {
        let (score, evidence_json) = row?;
        let evidence: Vec<String> = serde_json::from_str(&evidence_json).unwrap_or_default();
        let Some(official) = extract_official(&evidence) else { continue };
        totals.entry(official).or_default().push(score);
    }
    let mut averages = HashMap::new();
    for (official, scores) in totals {
        averages.insert(official, average(&scores));
    }
    Ok(averages)
}

fn export_artifact_jsons(out_dir: &Path, dest_dir: &Path) -> Result<()> {
    let artifacts_dir = out_dir.join("artifacts");
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
        let value: serde_json::Value = serde_json::from_str(&raw)?;
        let Some(id) = value.get("id").and_then(|value| value.as_str()) else {
            continue;
        };
        let dest = dest_dir.join(format!("{id}.json"));
        fs::write(dest, serde_json::to_string_pretty(&value)?)?;
    }
    Ok(())
}

fn copy_report_jsons(out_dir: &Path, dest_dir: &Path) -> Result<()> {
    let reports_dir = out_dir.join("reports").join("weekly");
    if !reports_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&reports_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let filename = path.file_name().and_then(|value| value.to_str()).unwrap_or("");
        fs::copy(&path, dest_dir.join(filename))?;
    }
    Ok(())
}

fn write_site_assets(assets_dir: &Path) -> Result<()> {
    let css = r#"
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: #0b0f14; color: #ecf1f6; line-height: 1.5; }
a { color: #8dc3ff; text-decoration: none; }
a:hover { color: #c2ddff; }
.site-header { background: #0f1620; border-bottom: 1px solid #1d2733; position: sticky; top: 0; z-index: 10; }
.nav-inner { display: flex; align-items: center; justify-content: space-between; gap: 1rem; padding: 0.8rem 1.5rem; max-width: 1200px; margin: 0 auto; }
.nav-brand { display: flex; align-items: center; gap: 0.75rem; font-weight: 700; letter-spacing: 0.02em; }
.nav-links { display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; }
.nav-links a { color: #c7d2df; font-size: 0.95rem; }
.nav-links a:hover { color: #ffffff; }
.nav-search { display: flex; align-items: center; gap: 0.5rem; background: #111923; border: 1px solid #243244; border-radius: 999px; padding: 0.35rem 0.75rem; min-width: 220px; }
.nav-search input { background: transparent; border: none; color: #d6e2f0; width: 100%; font-size: 0.85rem; }
.nav-search input:disabled { color: #708299; }
.container { max-width: 1200px; margin: 0 auto; padding: 2rem 1.5rem 3rem; }
.hero { background: linear-gradient(135deg, #1c2735 0%, #142030 55%, #0f1620 100%); border: 1px solid #1f2b3a; border-radius: 18px; padding: 1.5rem; display: grid; gap: 1.25rem; }
.hero-header { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 1rem; }
.hero-title { margin: 0; font-size: 1.6rem; }
.hero-subtitle { color: #9fb0c4; margin: 0.3rem 0 0; }
.stats-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 0.75rem; }
.stat { background: #121a25; border: 1px solid #1f2b3a; border-radius: 12px; padding: 0.9rem; }
.stat-label { font-size: 0.8rem; color: #9fb0c4; }
.stat-value { font-size: 1.3rem; font-weight: 600; margin-top: 0.2rem; }
.card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem; margin-top: 1.5rem; }
.card { background: #111923; border: 1px solid #1d2836; border-radius: 16px; padding: 1.2rem; box-shadow: 0 10px 30px rgba(6, 10, 16, 0.35); display: flex; flex-direction: column; gap: 0.8rem; }
.card-title { display: flex; align-items: center; gap: 0.6rem; font-size: 1.05rem; margin: 0; }
.chip-row { display: flex; flex-wrap: wrap; gap: 0.4rem; }
.chip { background: #1a2432; border: 1px solid #243244; padding: 0.2rem 0.55rem; border-radius: 999px; font-size: 0.75rem; color: #c8d5e4; }
.badge { padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; display: inline-flex; align-items: center; gap: 0.3rem; }
.badge.rising { background: rgba(68, 171, 99, 0.18); color: #7de7a5; border: 1px solid rgba(68, 171, 99, 0.4); }
.badge.falling { background: rgba(196, 69, 69, 0.18); color: #ff9c9c; border: 1px solid rgba(196, 69, 69, 0.4); }
.badge.drift { background: rgba(210, 140, 46, 0.2); color: #ffd18b; border: 1px solid rgba(210, 140, 46, 0.4); }
.badge.insufficient { background: rgba(115, 129, 148, 0.2); color: #b6c2d3; border: 1px solid rgba(115, 129, 148, 0.4); }
.badge.grade-a { background: rgba(78, 197, 139, 0.18); color: #7ff0b0; border: 1px solid rgba(78, 197, 139, 0.4); }
.badge.grade-b { background: rgba(119, 190, 255, 0.18); color: #9dd2ff; border: 1px solid rgba(119, 190, 255, 0.4); }
.badge.grade-c { background: rgba(240, 190, 78, 0.2); color: #ffd38a; border: 1px solid rgba(240, 190, 78, 0.4); }
.badge.grade-d { background: rgba(255, 140, 84, 0.18); color: #ffc2a3; border: 1px solid rgba(255, 140, 84, 0.4); }
.badge.grade-f { background: rgba(217, 80, 80, 0.18); color: #ffb3b3; border: 1px solid rgba(217, 80, 80, 0.4); }
.table-wrap { overflow-x: auto; border: 1px solid #1d2836; border-radius: 14px; margin-top: 1rem; }
table { width: 100%; border-collapse: collapse; font-size: 0.95rem; }
thead th { position: sticky; top: 0; background: #0f1620; color: #c4d2e3; text-align: left; padding: 0.75rem; border-bottom: 1px solid #1d2836; }
tbody tr:nth-child(even) { background: rgba(17, 25, 35, 0.6); }
td { padding: 0.75rem; border-bottom: 1px solid #1d2836; vertical-align: top; }
.sort-hint { font-size: 0.8rem; color: #8ea2b8; margin-top: 0.5rem; }
.score-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; }
.score-card { background: #121a25; border: 1px solid #1f2b3a; border-radius: 14px; padding: 1rem; }
.receipts details { background: #111923; border: 1px solid #1d2836; border-radius: 12px; padding: 0.75rem 1rem; margin-bottom: 0.6rem; }
.receipts summary { cursor: pointer; font-weight: 600; }
.clean-list { list-style: none; padding-left: 0; margin: 0; display: grid; gap: 0.6rem; }
.footer { border-top: 1px solid #1d2836; padding: 2rem 1.5rem; background: #0f1620; color: #9fb0c4; }
.footer-inner { max-width: 1200px; margin: 0 auto; display: flex; flex-direction: column; gap: 0.8rem; }
.footer-links { display: flex; flex-wrap: wrap; gap: 1rem; align-items: center; }
.btn { background: #ffcf56; color: #2b1a00; padding: 0.45rem 0.9rem; border-radius: 999px; font-weight: 700; }
.subtitle { color: #9fb0c4; }
.icon { width: 20px; height: 20px; fill: none; stroke: currentColor; stroke-width: 1.6; }
@media (max-width: 720px) {
  .nav-inner { flex-direction: column; align-items: stretch; }
  .nav-links { justify-content: space-between; }
  .nav-search { width: 100%; }
}
    "#;
    let js = r#"
document.querySelectorAll('th[data-sort]').forEach((header) => {
  header.addEventListener('click', () => {
    const table = header.closest('table');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const index = Array.from(header.parentNode.children).indexOf(header);
    const direction = header.dataset.direction === 'asc' ? 'desc' : 'asc';
    header.dataset.direction = direction;
    rows.sort((a, b) => {
      const aText = a.children[index].dataset.value || a.children[index].innerText;
      const bText = b.children[index].dataset.value || b.children[index].innerText;
      const aNum = parseFloat(aText);
      const bNum = parseFloat(bText);
      if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) {
        return direction === 'asc' ? aNum - bNum : bNum - aNum;
      }
      return direction === 'asc' ? aText.localeCompare(bText) : bText.localeCompare(aText);
    });
    rows.forEach((row) => tbody.appendChild(row));
  });
});
    "#;
    fs::write(assets_dir.join("style.css"), css.trim())?;
    fs::write(assets_dir.join("app.js"), js.trim())?;
    Ok(())
}

fn render_home_page(
    latest_report: Option<&WeekReport>,
    week_date: &str,
    officials: &[OfficialSummary],
) -> String {
    let avg_score = latest_report.map(|report| report.rubric_average).unwrap_or(0.0);
    let drift_count = officials.iter().filter(|official| !official.drift_flags.is_empty()).count();
    let flagged_count = officials
        .iter()
        .filter(|official| official.insufficient || !official.drift_flags.is_empty())
        .count();
    let top_tags = latest_report
        .map(|report| {
            report
                .issue_tag_counts
                .iter()
                .take(3)
                .map(|(tag, _)| tag.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let tag_chips = if top_tags.is_empty() {
        "<span class=\"subtitle\">No tags yet.</span>".to_string()
    } else {
        top_tags
            .into_iter()
            .map(|tag| format!("<span class=\"chip\">{tag}</span>"))
            .collect::<Vec<_>>()
            .join("")
    };

    let (avg_numeric, avg_grade) = score_to_grade(avg_score);
    let hero = format!(
        r#"
<section class="hero">
  <div class="hero-header">
    <div>
      <h1 class="hero-title">Weekly Brief</h1>
      <p class="hero-subtitle">Week of {week_date}</p>
    </div>
    <a class="btn" href="/weeks/{week_date}.html">View weekly page →</a>
  </div>
  <div class="stats-row">
    <div class="stat">
      <div class="stat-label">Artifacts ingested</div>
      <div class="stat-value">{artifact_count}</div>
    </div>
    <div class="stat">
      <div class="stat-label">Decisions recorded</div>
      <div class="stat-value">{decision_count}</div>
    </div>
    <div class="stat">
      <div class="stat-label">Flagged signals</div>
      <div class="stat-value">{flagged_count}</div>
    </div>
  </div>
</section>
"#,
        week_date = week_date,
        artifact_count = latest_report.map(|report| report.artifacts.len()).unwrap_or(0),
        decision_count = latest_report.map(|report| report.decisions.len()).unwrap_or(0),
        flagged_count = flagged_count
    );

    let body = format!(
        r#"
{nav}
<main class="container">
  {hero}
  <section>
    <h2>Governing body dashboards</h2>
    <div class="card-grid">
      <div class="card">
        <div class="card-title">{icon_court} Fiscal Court</div>
        <div>
          <span class="badge grade-{grade_class}">{avg_grade}</span>
          <span class="subtitle">Avg score {avg_numeric:.1}</span>
        </div>
        <div class="chip-row">
          <span class="chip">Drift alerts: {drift_count}</span>
        </div>
        <div class="chip-row">{tag_chips}</div>
        <a href="/stockade/index.html">View details →</a>
      </div>
      <div class="card">
        <div class="card-title">{icon_cap} Board of Education</div>
        <p class="subtitle">Placeholder until data exists.</p>
      </div>
      <div class="card">
        <div class="card-title">{icon_ballot} Elections / Clerk</div>
        <p class="subtitle">Placeholder until data exists.</p>
      </div>
    </div>
  </section>
</main>
{footer}
"#,
        nav = nav_html(week_date),
        footer = footer_html(week_date),
        hero = hero,
        icon_court = icon_court(),
        icon_cap = icon_cap(),
        icon_ballot = icon_ballot(),
        avg_numeric = avg_numeric,
        avg_grade = avg_grade,
        grade_class = grade_class(&avg_grade),
        drift_count = drift_count,
        tag_chips = tag_chips
    );
    html_page("LaRue Civic Intel", &body)
}

fn render_stockade_page(officials: &[OfficialSummary], week_date: &str) -> String {
    let rows = officials
        .iter()
        .map(|official| {
            let trend_badge = if official.delta >= 5.0 {
                format!(
                    "<span class=\"badge rising\">{} Rising</span>",
                    icon_trend_up()
                )
            } else if official.delta <= -5.0 {
                format!(
                    "<span class=\"badge falling\">{} Falling</span>",
                    icon_trend_down()
                )
            } else {
                String::new()
            };
            let drift_badge = if !official.drift_flags.is_empty() {
                format!("<span class=\"badge drift\">{} Drift</span>", icon_alert())
            } else {
                String::new()
            };
            let insufficient_badge = if official.insufficient {
                format!(
                    "<span class=\"badge insufficient\">{} Insufficient</span>",
                    icon_info()
                )
            } else {
                String::new()
            };
            let tags = if official.top_issue_tags.is_empty() {
                "-".to_string()
            } else {
                official
                    .top_issue_tags
                    .iter()
                    .map(|tag| format!("<span class=\"chip\">{tag}</span>"))
                    .collect::<Vec<_>>()
                    .join("")
            };
            let grade_class = grade_class(&official.letter_grade);
            format!(
                r#"<tr>
<td><a href="/officials/{id}.html">{name}</a></td>
<td data-value="{numeric:.1}">{numeric:.1}</td>
<td><span class="badge grade-{grade_class}">{grade}</span></td>
<td data-value="{delta:.1}">{delta:.1}</td>
<td><div class="chip-row">{trend}{drift}{insufficient}</div></td>
<td><div class="chip-row">{tags}</div></td>
</tr>"#,
                id = official.id,
                name = official.name,
                numeric = official.numeric_grade,
                grade = official.letter_grade,
                grade_class = grade_class,
                delta = official.delta,
                trend = trend_badge,
                drift = drift_badge,
                insufficient = insufficient_badge,
                tags = tags
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let body = format!(
        r#"
{nav}
<main class="container">
  <h2>Public Stockade</h2>
  <p class="subtitle">Leaderboard sorted by current score. Click headers to sort.</p>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th data-sort>Name</th>
          <th data-sort>Score</th>
          <th>Grade</th>
          <th data-sort>Delta</th>
          <th>Status</th>
          <th>Top Issues</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </div>
  <div class="sort-hint">Tip: click column headers to sort.</div>
</main>
{footer}
<script src="/assets/app.js"></script>
    "#
    ,
        nav = nav_html(week_date),
        footer = footer_html(week_date)
    );
    html_page("Public Stockade", &body)
}

fn render_officials_index(officials: &[OfficialSummary], week_date: &str) -> String {
    let list = officials
        .iter()
        .map(|official| {
            let grade_class = grade_class(&official.letter_grade);
            format!(
                "<li><a href=\"/officials/{id}.html\">{name}</a> <span class=\"badge grade-{grade_class}\">{grade}</span> <span class=\"subtitle\">{score:.1}</span></li>",
                id = official.id,
                name = official.name,
                grade = official.letter_grade,
                grade_class = grade_class,
                score = official.numeric_grade
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let body = format!(
        r#"
{nav}
<main class="container">
  <h2>Officials</h2>
  <div class="card">
    <ul class="clean-list">
      {list}
    </ul>
  </div>
</main>
{footer}
    "#
    ,
        nav = nav_html(week_date),
        footer = footer_html(week_date)
    );
    html_page("Officials", &body)
}

fn render_official_detail(official: &OfficialSummary, week_date: &str) -> String {
    let axis_rows = official
        .axis_scores_normalized
        .iter()
        .map(|(axis, score)| {
            let (numeric, letter) = score_to_grade(*score);
            format!(
                "<tr><td>{axis}</td><td>{letter}</td><td>{numeric:.1}</td></tr>"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let trend = if official.delta >= 5.0 {
        format!("{} Rising", icon_trend_up())
    } else if official.delta <= -5.0 {
        format!("{} Falling", icon_trend_down())
    } else {
        format!("{} Stable", icon_info())
    };

    let mut flags = Vec::new();
    if !official.drift_flags.is_empty() {
        flags.push(format!("<span class=\"badge drift\">{} Drift</span>", icon_alert()));
    }
    if official.insufficient {
        flags.push(format!(
            "<span class=\"badge insufficient\">{} Insufficient evidence</span>",
            icon_info()
        ));
    }
    let flags = if flags.is_empty() {
        "<span class=\"subtitle\">No flags</span>".to_string()
    } else {
        flags.join("")
    };

    let receipts = if official.receipts.is_empty() {
        "<p class=\"subtitle\">No receipts recorded.</p>".to_string()
    } else {
        official
            .receipts
            .iter()
            .map(|receipt| {
                let artifacts = if receipt.artifact_ids.is_empty() {
                    "_No artifacts_".to_string()
                } else {
                    receipt
                        .artifact_ids
                        .iter()
                        .map(|id| format!("<a href=\"/artifacts/{id}.json\">{id}</a>"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                format!(
                    r#"<details>
  <summary>{date}: {text}</summary>
  <div class="subtitle">Week: <a href="/weeks/{week}.html">{week}</a></div>
  <div class="subtitle">Artifacts: {artifacts}</div>
</details>"#,
                    date = receipt.meeting_date,
                    text = receipt.motion_text,
                    week = receipt.week_date,
                    artifacts = artifacts
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let commentary = official
        .commentary
        .as_deref()
        .unwrap_or("No commentary generated.");

    let grade_class = grade_class(&official.letter_grade);
    let body = format!(
        r#"
{nav}
<main class="container">
  <h2>{name}</h2>
  <div class="score-grid">
    <div class="score-card">
      <div class="subtitle">Overall score</div>
      <div class="stat-value">{numeric:.1}</div>
      <span class="badge grade-{grade_class}">{grade}</span>
    </div>
    <div class="score-card">
      <div class="subtitle">Trend</div>
      <div class="stat-value">{delta:.1}</div>
      <span class="badge">{trend}</span>
    </div>
    <div class="score-card">
      <div class="subtitle">Flags</div>
      <div class="chip-row">{flags}</div>
    </div>
  </div>

  <section>
    <h3>Per-axis grades</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Axis</th><th>Grade</th><th>Score</th></tr></thead>
        <tbody>{axis_rows}</tbody>
      </table>
    </div>
  </section>

  <section>
    <h3>Receipts</h3>
    <div class="receipts">{receipts}</div>
  </section>

  <section class="card">
    <h3>Commentary</h3>
    <p>{commentary}</p>
    <p class="subtitle">Satire/opinion based on this project’s rubric scoring.</p>
  </section>
</main>
{footer}
    "#,
        name = official.name,
        grade = official.letter_grade,
        grade_class = grade_class,
        numeric = official.numeric_grade,
        axis_rows = axis_rows,
        receipts = receipts,
        commentary = commentary,
        delta = official.delta,
        trend = trend,
        flags = flags
    );
    html_page(&format!("Official {}", official.name), &body)
}

fn render_week_page(report: &WeekReport, week_date: &str) -> String {
    let issue_tags = if report.issue_tag_counts.is_empty() {
        "_No issue tags._".to_string()
    } else {
        report
            .issue_tag_counts
            .iter()
            .map(|(tag, count)| format!("{tag} ({count})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let decisions = if report.decisions.is_empty() {
        "_No decisions recorded._".to_string()
    } else {
        report
            .decisions
            .iter()
            .map(|decision| {
                let motions = decision
                    .motions
                    .iter()
                    .map(|motion| {
                        let outcome = motion
                            .result
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string());
                        format!("<li>{} ({})</li>", motion.text, outcome)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    "<div class=\"card\"><h4>{}</h4><ul>{}</ul></div>",
                    decision.body_name, motions
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let artifacts = if report.artifacts.is_empty() {
        "_No artifacts._".to_string()
    } else {
        report
            .artifacts
            .iter()
            .map(|artifact| {
                format!(
                    "<li><a href=\"{url}\">{title}</a></li>",
                    url = artifact.source_value,
                    title = artifact.title
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let body = format!(
        r#"
{nav}
<main class="container">
  <h2>Week of {date}</h2>
  <p class="subtitle">Window: {start} to {end}</p>
  <section class="card">
    <h3>High-impact artifacts</h3>
    <ul>{artifacts}</ul>
  </section>
  <section>
    <h3>Decisions This Week</h3>
    <div class="card-grid">{decisions}</div>
  </section>
  <section class="card">
    <h3>Rubric Alignment</h3>
    <p>Average score: {avg:.1}</p>
    <p>Issue tags: {issue_tags}</p>
    <p><a href="/reports/weekly/{date}.json">Raw report JSON</a></p>
  </section>
</main>
{footer}
    "#,
        nav = nav_html(week_date),
        footer = footer_html(week_date),
        date = report.date,
        start = report.window_start,
        end = report.window_end,
        artifacts = artifacts,
        decisions = decisions,
        avg = report.rubric_average,
        issue_tags = issue_tags
    );
    html_page(&format!("Week {}", report.date), &body)
}

fn html_page(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <link rel="stylesheet" href="/assets/style.css" />
</head>
<body>
{body}
</body>
</html>
"#
    )
}

fn nav_html(week_date: &str) -> String {
    format!(
        r#"
<header class="site-header">
  <div class="nav-inner">
    <div class="nav-brand">
      {icon_logo}
      <span>LaRue Civic Intel</span>
    </div>
    <nav class="nav-links">
      <a href="/">Home</a>
      <a href="/stockade/index.html">Stockade</a>
      <a href="/officials/index.html">Officials</a>
      <a href="/weeks/{week_date}.html">Latest Week</a>
    </nav>
    <div class="nav-search" aria-disabled="true">
      {icon_search}
      <input type="text" placeholder="Search (coming soon)" disabled />
    </div>
  </div>
</header>
"#,
        week_date = week_date,
        icon_logo = icon_court(),
        icon_search = icon_search()
    )
}

fn footer_html(week_date: &str) -> String {
    format!(
        r#"
<footer class="footer">
  <div class="footer-inner">
    <div class="footer-links">
      <a class="btn" href="https://github.com/sponsors/Yisonco-Stellargold">Sponsor</a>
      <a href="https://github.com/Yisonco-Stellargold/larue-civic-intel">Repository</a>
      <a href="/reports/weekly/{week_date}.json">Latest report JSON</a>
    </div>
    <div class="subtitle">Rubric-based scoring; commentary is opinion/satire. Always consult primary sources.</div>
  </div>
</footer>
"#,
        week_date = week_date
    )
}

fn icon_court() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M3 21h18M5 21V9l7-4 7 4v12M9 21v-6h6v6M2 9l10-5 10 5"/></svg>"#
}

fn icon_cap() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M2 9l10-5 10 5-10 5-10-5Z"/><path d="M6 12v4c0 1.5 3 3 6 3s6-1.5 6-3v-4"/></svg>"#
}

fn icon_ballot() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 4h12v6H4z"/><path d="M8 14h12v6H8z"/><path d="M16 4l6 6M16 10l6-6"/></svg>"#
}

fn icon_search() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3-3"/></svg>"#
}

fn icon_trend_up() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 16l6-6 4 4 6-6"/><path d="M14 8h6v6"/></svg>"#
}

fn icon_trend_down() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 8l6 6 4-4 6 6"/><path d="M14 16h6v-6"/></svg>"#
}

fn icon_alert() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l9 16H3l9-16Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>"#
}

fn icon_info() -> &'static str {
    r#"<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 10v6"/><path d="M12 7h.01"/></svg>"#
}

fn grade_class(grade: &str) -> &'static str {
    match grade.chars().next().unwrap_or('F') {
        'A' => "a",
        'B' => "b",
        'C' => "c",
        'D' => "d",
        _ => "f",
    }
}

fn build_commentary_line(
    official_id: &str,
    week_date: &str,
    grade: &str,
    prior_grade: &str,
    delta: f64,
    has_drift: bool,
    tags: &[String],
    site: &SiteConfig,
) -> Option<String> {
    if site.enable_commentary == Some(false) {
        return None;
    }
    let style = site.commentary_style.clone().unwrap_or_else(|| "satire".to_string());
    let seed = format!("{official_id}:{week_date}:{style}");
    let grade_drop = grade_rank(prior_grade) - grade_rank(grade);
    let grade_rise = grade_rank(grade) - grade_rank(prior_grade);
    let templates = if delta <= -10.0 || grade_drop >= 1 {
        vec![
            "This week’s voting record earned a {grade}—not exactly a masterclass in restraint.",
            "A {grade} this week. The numbers did the talking.",
            "Scores slid to {grade}; the rubric isn’t feeling inspired.",
        ]
    } else if delta >= 10.0 || grade_rise >= 1 {
        vec![
            "Solid climb to a {grade}; keep it up and the trend becomes a pattern.",
            "A jump to {grade}. Momentum looks real this week.",
            "Score gains landed at {grade}; credit where it’s due.",
        ]
    } else {
        vec![
            "Steady at {grade}; the next votes will decide the direction.",
            "Holding at {grade}. Consistency is the story for now.",
            "No major shifts: {grade} with room to move.",
        ]
    };
    let mut template = templates[stable_hash(&seed) as usize % templates.len()];
    if style == "neutral" {
        template = "Current grade is {grade}; see the weekly report for details.";
    }
    let mut line = template.replace("{grade}", grade);
    if has_drift {
        line.push_str(" Drift alerts are active.");
    }
    if !tags.is_empty() {
        line.push_str(&format!(" Top issues: {}.", tags.join(", ")));
    }
    Some(line)
}

fn stable_hash(value: &str) -> u64 {
    let mut hash: u64 = 14695981039346656037;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

fn score_to_grade(score: f64) -> (f64, String) {
    let numeric = score.clamp(0.0, 100.0);
    let grade = match numeric {
        n if n >= 97.0 => "A+",
        n if n >= 93.0 => "A",
        n if n >= 90.0 => "A-",
        n if n >= 87.0 => "B+",
        n if n >= 83.0 => "B",
        n if n >= 80.0 => "B-",
        n if n >= 77.0 => "C+",
        n if n >= 73.0 => "C",
        n if n >= 70.0 => "C-",
        n if n >= 67.0 => "D+",
        n if n >= 63.0 => "D",
        n if n >= 60.0 => "D-",
        _ => "F",
    };
    (numeric, grade.to_string())
}

fn grade_rank(grade: &str) -> i32 {
    match grade {
        "A+" => 12,
        "A" => 11,
        "A-" => 10,
        "B+" => 9,
        "B" => 8,
        "B-" => 7,
        "C+" => 6,
        "C" => 5,
        "C-" => 4,
        "D+" => 3,
        "D" => 2,
        "D-" => 1,
        _ => 0,
    }
}

struct OfficialSummaryBuilder {
    id: String,
    name: String,
    overall_scores: Vec<f64>,
    axis_scores: Vec<HashMap<String, f64>>,
    receipts: Vec<Receipt>,
    insufficient: bool,
    top_issue_tags: Vec<String>,
}

impl OfficialSummaryBuilder {
    fn new(name: &str, report: Option<&WeekReport>, _week_date: &str) -> Self {
        let id = slugify(name);
        let top_issue_tags = report
            .map(|value| {
                value
                    .issue_tag_counts
                    .iter()
                    .take(3)
                    .map(|(tag, _)| tag.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self {
            id,
            name: name.to_string(),
            overall_scores: Vec::new(),
            axis_scores: Vec::new(),
            receipts: Vec::new(),
            insufficient: false,
            top_issue_tags,
        }
    }

    fn build(
        self,
        rubric_config: Option<&civic_core::scoring::RubricConfig>,
        drift_flags: &[String],
    ) -> OfficialSummary {
        let average_score = average(&self.overall_scores);
        let axis_scores = average_axis_scores(&self.axis_scores);
        let axis_scores_normalized = axis_scores
            .iter()
            .map(|(axis, score)| (axis.clone(), normalize_score(*score, rubric_config)))
            .collect::<HashMap<_, _>>();
        let numeric_score = normalize_score(average_score, rubric_config);
        let (numeric_grade, letter_grade) = score_to_grade(numeric_score);
        let drift = drift_flags
            .iter()
            .filter(|flag| flag.starts_with(&self.name))
            .cloned()
            .collect::<Vec<_>>();
        OfficialSummary {
            id: self.id,
            name: self.name,
            average_score,
            axis_scores,
            axis_scores_normalized,
            letter_grade,
            numeric_grade,
            delta: 0.0,
            drift_flags: drift,
            insufficient: self.insufficient,
            receipts: self.receipts,
            top_issue_tags: self.top_issue_tags,
            commentary: None,
        }
    }
}

fn normalize_score(score: f64, rubric_config: Option<&civic_core::scoring::RubricConfig>) -> f64 {
    let Some(config) = rubric_config else {
        return score.clamp(0.0, 100.0);
    };
    let floor = config.general.score_floor;
    let ceiling = config.general.score_ceiling;
    if (ceiling - floor).abs() < f64::EPSILON {
        return config.general.neutral_score;
    }
    let normalized = ((score - floor) / (ceiling - floor)) * 100.0;
    normalized.clamp(0.0, 100.0)
}

fn average_axis_scores(values: &[HashMap<String, f64>]) -> HashMap<String, f64> {
    let mut totals: HashMap<String, Vec<f64>> = HashMap::new();
    for map in values {
        for (axis, value) in map {
            totals.entry(axis.clone()).or_default().push(*value);
        }
    }
    let mut averages = HashMap::new();
    for (axis, scores) in totals {
        averages.insert(axis, average(&scores));
    }
    averages
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
