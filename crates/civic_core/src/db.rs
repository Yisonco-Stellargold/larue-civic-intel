use crate::schema::{Artifact, Body, Meeting};
use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::Value;

pub fn open(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    init(&conn)?;
    Ok(conn)
}

fn init(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS bodies (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          kind TEXT NOT NULL,
          jurisdiction TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS artifacts (
          id TEXT PRIMARY KEY,
          source_kind TEXT NOT NULL,
          source_value TEXT NOT NULL,
          retrieved_at TEXT NOT NULL,
          title TEXT,
          content_type TEXT,
          body_text TEXT,
          tags_json TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE INDEX IF NOT EXISTS idx_artifacts_retrieved_at ON artifacts(retrieved_at);

        CREATE TABLE IF NOT EXISTS meetings (
          id TEXT PRIMARY KEY,
          body_id TEXT NOT NULL,
          started_at TEXT NOT NULL,
          artifact_ids_json TEXT NOT NULL,
          motions_json TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE INDEX IF NOT EXISTS idx_meetings_started_at ON meetings(started_at);
        "#,
    )?;
    seed_bodies(conn)?;
    Ok(())
}

fn seed_bodies(conn: &Connection) -> Result<()> {
    let body = Body {
        id: "larue-fiscal-court".to_string(),
        name: "LaRue County Fiscal Court".to_string(),
        kind: "fiscal_court".to_string(),
        jurisdiction: "LaRue County, KY".to_string(),
    };
    conn.execute(
        r#"
        INSERT OR IGNORE INTO bodies (id, name, kind, jurisdiction)
        VALUES (?1, ?2, ?3, ?4)
        "#,
        params![body.id, body.name, body.kind, body.jurisdiction],
    )?;
    Ok(())
}

pub fn upsert_artifact(conn: &Connection, artifact: &Artifact, raw_json: &Value) -> Result<()> {
    let tags_json = serde_json::to_string(&artifact.tags)?;
    let raw_json_str = serde_json::to_string(raw_json)?;

    conn.execute(
        r#"
        INSERT INTO artifacts (
          id, source_kind, source_value, retrieved_at,
          title, content_type, body_text, tags_json, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(id) DO UPDATE SET
          source_kind=excluded.source_kind,
          source_value=excluded.source_value,
          retrieved_at=excluded.retrieved_at,
          title=excluded.title,
          content_type=excluded.content_type,
          body_text=excluded.body_text,
          tags_json=excluded.tags_json,
          raw_json=excluded.raw_json
        "#,
        params![
            artifact.id,
            artifact.source.kind,
            artifact.source.value,
            artifact.source.retrieved_at,
            artifact.title,
            artifact.content_type,
            artifact.body_text,
            tags_json,
            raw_json_str
        ],
    )?;

    Ok(())
}

pub fn artifact_exists(conn: &Connection, id: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT 1 FROM artifacts WHERE id = ?1 LIMIT 1")?;
    Ok(stmt.exists(params![id])?)
}

pub fn upsert_meeting(conn: &Connection, meeting: &Meeting, raw_json: &Value) -> Result<()> {
    let artifact_ids_json = serde_json::to_string(&meeting.artifact_ids)?;
    let motions_json = serde_json::to_string(&meeting.motions)?;
    let raw_json_str = serde_json::to_string(raw_json)?;

    conn.execute(
        r#"
        INSERT INTO meetings (
          id, body_id, started_at, artifact_ids_json, motions_json, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(id) DO UPDATE SET
          body_id=excluded.body_id,
          started_at=excluded.started_at,
          artifact_ids_json=excluded.artifact_ids_json,
          motions_json=excluded.motions_json,
          raw_json=excluded.raw_json
        "#,
        params![
            meeting.id,
            meeting.body_id,
            meeting.started_at,
            artifact_ids_json,
            motions_json,
            raw_json_str
        ],
    )?;
    Ok(())
}

pub fn meeting_exists(conn: &Connection, id: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT 1 FROM meetings WHERE id = ?1 LIMIT 1")?;
    Ok(stmt.exists(params![id])?)
}
