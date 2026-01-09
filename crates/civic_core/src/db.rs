use crate::schema::{Artifact, Body, DecisionMeeting, DecisionMotion, DecisionVote, Meeting};
use crate::scoring::DecisionScore;
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

        CREATE TABLE IF NOT EXISTS motions (
          id TEXT PRIMARY KEY,
          meeting_id TEXT NOT NULL,
          motion_index INTEGER NOT NULL,
          text TEXT NOT NULL,
          moved_by TEXT,
          seconded_by TEXT,
          result TEXT,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE INDEX IF NOT EXISTS idx_motions_meeting_id ON motions(meeting_id);

        CREATE TABLE IF NOT EXISTS votes (
          id TEXT PRIMARY KEY,
          motion_id TEXT NOT NULL,
          vote_type TEXT,
          outcome TEXT,
          ayes_json TEXT NOT NULL,
          nays_json TEXT NOT NULL,
          abstain_json TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          inserted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE INDEX IF NOT EXISTS idx_votes_motion_id ON votes(motion_id);

        CREATE TABLE IF NOT EXISTS decision_scores (
          id TEXT PRIMARY KEY,
          meeting_id TEXT,
          motion_id TEXT,
          vote_id TEXT,
          overall_score REAL NOT NULL,
          axis_json TEXT NOT NULL,
          refs_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          confidence REAL NOT NULL,
          flags_json TEXT NOT NULL,
          computed_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_decision_scores_meeting_id ON decision_scores(meeting_id);
        CREATE INDEX IF NOT EXISTS idx_decision_scores_motion_id ON decision_scores(motion_id);
        CREATE INDEX IF NOT EXISTS idx_decision_scores_vote_id ON decision_scores(vote_id);

        CREATE TABLE IF NOT EXISTS official_drift (
          id TEXT PRIMARY KEY,
          official_name TEXT NOT NULL,
          axis TEXT NOT NULL,
          prior_average REAL NOT NULL,
          current_average REAL NOT NULL,
          deviation REAL NOT NULL,
          flags_json TEXT NOT NULL,
          computed_at TEXT NOT NULL
        );
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

pub fn upsert_decision_meeting(
    conn: &Connection,
    meeting: &DecisionMeeting,
    raw_json: &Value,
    motions: &[DecisionMotion],
) -> Result<()> {
    let artifact_ids_json = serde_json::to_string(&meeting.artifact_ids)?;
    let motion_summaries: Vec<crate::schema::Motion> = motions
        .iter()
        .map(|motion| crate::schema::Motion {
            text: motion.text.clone(),
            result: motion.result.clone(),
        })
        .collect();
    let motions_json = serde_json::to_string(&motion_summaries)?;
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

pub fn upsert_motion(
    conn: &Connection,
    motion: &DecisionMotion,
    raw_json: &Value,
) -> Result<()> {
    let raw_json_str = serde_json::to_string(raw_json)?;
    conn.execute(
        r#"
        INSERT INTO motions (
          id, meeting_id, motion_index, text, moved_by, seconded_by, result, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(id) DO UPDATE SET
          meeting_id=excluded.meeting_id,
          motion_index=excluded.motion_index,
          text=excluded.text,
          moved_by=excluded.moved_by,
          seconded_by=excluded.seconded_by,
          result=excluded.result,
          raw_json=excluded.raw_json
        "#,
        params![
            motion.id,
            motion.meeting_id,
            motion.index as i64,
            motion.text,
            motion.moved_by,
            motion.seconded_by,
            motion.result,
            raw_json_str
        ],
    )?;
    Ok(())
}

pub fn upsert_vote(
    conn: &Connection,
    vote: &DecisionVote,
    raw_json: &Value,
) -> Result<()> {
    let raw_json_str = serde_json::to_string(raw_json)?;
    let ayes_json = serde_json::to_string(&vote.ayes)?;
    let nays_json = serde_json::to_string(&vote.nays)?;
    let abstain_json = serde_json::to_string(&vote.abstain)?;
    conn.execute(
        r#"
        INSERT INTO votes (
          id, motion_id, vote_type, outcome, ayes_json, nays_json, abstain_json, raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(id) DO UPDATE SET
          motion_id=excluded.motion_id,
          vote_type=excluded.vote_type,
          outcome=excluded.outcome,
          ayes_json=excluded.ayes_json,
          nays_json=excluded.nays_json,
          abstain_json=excluded.abstain_json,
          raw_json=excluded.raw_json
        "#,
        params![
            vote.id,
            vote.motion_id,
            vote.vote_type,
            vote.outcome,
            ayes_json,
            nays_json,
            abstain_json,
            raw_json_str
        ],
    )?;
    Ok(())
}

pub fn upsert_decision_score(conn: &Connection, score: &DecisionScore) -> Result<()> {
    let axis_json = serde_json::to_string(&score.axis_scores)?;
    let refs_json = serde_json::to_string(&score.constitutional_refs)?;
    let evidence_json = serde_json::to_string(&score.evidence)?;
    let flags_json = serde_json::to_string(&score.flags)?;

    conn.execute(
        r#"
        INSERT INTO decision_scores (
          id, meeting_id, motion_id, vote_id, overall_score, axis_json, refs_json,
          evidence_json, confidence, flags_json, computed_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ON CONFLICT(id) DO UPDATE SET
          meeting_id=excluded.meeting_id,
          motion_id=excluded.motion_id,
          vote_id=excluded.vote_id,
          overall_score=excluded.overall_score,
          axis_json=excluded.axis_json,
          refs_json=excluded.refs_json,
          evidence_json=excluded.evidence_json,
          confidence=excluded.confidence,
          flags_json=excluded.flags_json,
          computed_at=excluded.computed_at
        "#,
        params![
            score.id,
            score.meeting_id,
            score.motion_id,
            score.vote_id,
            score.overall_score,
            axis_json,
            refs_json,
            evidence_json,
            score.confidence,
            flags_json,
            score.computed_at
        ],
    )?;
    Ok(())
}

pub fn upsert_official_drift(
    conn: &Connection,
    id: &str,
    official_name: &str,
    axis: &str,
    prior_average: f64,
    current_average: f64,
    deviation: f64,
    flags: &[String],
    computed_at: &str,
) -> Result<()> {
    let flags_json = serde_json::to_string(flags)?;
    conn.execute(
        r#"
        INSERT INTO official_drift (
          id, official_name, axis, prior_average, current_average, deviation, flags_json, computed_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(id) DO UPDATE SET
          official_name=excluded.official_name,
          axis=excluded.axis,
          prior_average=excluded.prior_average,
          current_average=excluded.current_average,
          deviation=excluded.deviation,
          flags_json=excluded.flags_json,
          computed_at=excluded.computed_at
        "#,
        params![
            id,
            official_name,
            axis,
            prior_average,
            current_average,
            deviation,
            flags_json,
            computed_at
        ],
    )?;
    Ok(())
}
