use crate::schema::{Artifact, Body, Meeting, Motion, Official, Vote};
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

        CREATE TABLE IF NOT EXISTS bodies (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          kind TEXT NOT NULL,
          jurisdiction TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS officials (
          id TEXT PRIMARY KEY,
          full_name TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS meetings (
          id TEXT PRIMARY KEY,
          body_id TEXT NOT NULL,
          started_at TEXT NOT NULL,
          ended_at TEXT,
          artifact_ids_json TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS motions (
          id TEXT PRIMARY KEY,
          meeting_id TEXT NOT NULL,
          title TEXT NOT NULL,
          description TEXT,
          result TEXT,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS votes (
          motion_id TEXT NOT NULL,
          official_id TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (motion_id, official_id)
        );
        "#,
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

pub fn upsert_body(conn: &Connection, body: &Body, raw_json: &Value) -> Result<()> {
    let raw_json_str = serde_json::to_string(raw_json)?;
    conn.execute(
        r#"
        INSERT INTO bodies (
          id, name, kind, jurisdiction, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(id) DO UPDATE SET
          name=excluded.name,
          kind=excluded.kind,
          jurisdiction=excluded.jurisdiction,
          raw_json=excluded.raw_json
        "#,
        params![
            body.id,
            body.name,
            body.kind,
            body.jurisdiction,
            raw_json_str
        ],
    )?;
    Ok(())
}

pub fn upsert_official(conn: &Connection, official: &Official, raw_json: &Value) -> Result<()> {
    let raw_json_str = serde_json::to_string(raw_json)?;
    conn.execute(
        r#"
        INSERT INTO officials (
          id, full_name, raw_json
        )
        VALUES (?1, ?2, ?3)
        ON CONFLICT(id) DO UPDATE SET
          full_name=excluded.full_name,
          raw_json=excluded.raw_json
        "#,
        params![official.id, official.full_name, raw_json_str],
    )?;
    Ok(())
}

pub fn upsert_meeting_with_children(
    conn: &mut Connection,
    meeting: &Meeting,
    raw_json: &Value,
) -> Result<()> {
    let tx = conn.transaction()?;
    let artifact_ids_json = serde_json::to_string(&meeting.artifact_ids)?;
    let raw_json_str = serde_json::to_string(raw_json)?;

    tx.execute(
        r#"
        INSERT INTO meetings (
          id, body_id, started_at, ended_at, artifact_ids_json, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(id) DO UPDATE SET
          body_id=excluded.body_id,
          started_at=excluded.started_at,
          ended_at=excluded.ended_at,
          artifact_ids_json=excluded.artifact_ids_json,
          raw_json=excluded.raw_json
        "#,
        params![
            meeting.id,
            meeting.body_id,
            meeting.started_at,
            meeting.ended_at,
            artifact_ids_json,
            raw_json_str
        ],
    )?;

    for motion in &meeting.motions {
        upsert_motion(&tx, motion)?;
        for vote in &motion.votes {
            upsert_vote(&tx, vote)?;
        }
    }

    tx.commit()?;
    Ok(())
}

fn upsert_motion(conn: &Connection, motion: &Motion) -> Result<()> {
    let raw_json_str = serde_json::to_string(motion)?;
    conn.execute(
        r#"
        INSERT INTO motions (
          id, meeting_id, title, description, result, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(id) DO UPDATE SET
          meeting_id=excluded.meeting_id,
          title=excluded.title,
          description=excluded.description,
          result=excluded.result,
          raw_json=excluded.raw_json
        "#,
        params![
            motion.id,
            motion.meeting_id,
            motion.title,
            motion.description,
            motion.result,
            raw_json_str
        ],
    )?;
    Ok(())
}

fn upsert_vote(conn: &Connection, vote: &Vote) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO votes (
          motion_id, official_id, value
        )
        VALUES (?1, ?2, ?3)
        ON CONFLICT(motion_id, official_id) DO UPDATE SET
          value=excluded.value
        "#,
        params![vote.motion_id, vote.official_id, vote.value],
    )?;
    Ok(())
}
