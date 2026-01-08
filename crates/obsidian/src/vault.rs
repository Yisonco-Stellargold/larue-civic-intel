use anyhow::Result;
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};

pub struct VaultPaths {
    pub root: PathBuf,
    pub index_dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub meetings_dir: PathBuf,
}

impl VaultPaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            index_dir: root.join("00_Index"),
            artifacts_dir: root.join("Artifacts"),
            meetings_dir: root.join("Meetings"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<()> {
        fs::create_dir_all(&self.index_dir)?;
        fs::create_dir_all(&self.artifacts_dir)?;
        fs::create_dir_all(&self.meetings_dir)?;
        Ok(())
    }
}

pub fn build_vault(conn: &Connection, vault_root: &Path) -> Result<()> {
    let paths = VaultPaths::new(vault_root);
    paths.ensure()?;

    // 1) Write artifact notes
    let mut stmt = conn.prepare(
        r#"
        SELECT id, source_kind, source_value, retrieved_at, title, content_type, body_text, tags_json
        FROM artifacts
        ORDER BY retrieved_at DESC
        "#,
    )?;

    let rows = stmt.query_map([], |row| {
        Ok(ArtifactRow {
            id: row.get(0)?,
            source_kind: row.get(1)?,
            source_value: row.get(2)?,
            retrieved_at: row.get(3)?,
            title: row.get(4)?,
            content_type: row.get(5)?,
            body_text: row.get(6)?,
            tags_json: row.get(7)?,
        })
    })?;

    let mut index_lines: Vec<String> = Vec::new();
    index_lines.push("# MOC - Artifacts".to_string());
    index_lines.push(String::new());
    index_lines.push("This index is generated. Do not edit manually.".to_string());
    index_lines.push(String::new());

    for r in rows {
        let a = r?;
        write_artifact_note(&paths, &a)?;
        index_lines.push(format!("- [[Artifacts/{}|{}]]", a.id, a.index_title()));
    }

    // 2) Write MOC
    let moc_path = paths.index_dir.join("MOC - Artifacts.md");
    fs::write(moc_path, index_lines.join("\n"))?;

    // 3) Write meeting notes
    let mut stmt = conn.prepare(
        r#"
        SELECT id, body_id, started_at, artifact_ids_json, motions_json
        FROM meetings
        ORDER BY started_at DESC
        "#,
    )?;

    let rows = stmt.query_map([], |row| {
        Ok(MeetingRow {
            id: row.get(0)?,
            body_id: row.get(1)?,
            started_at: row.get(2)?,
            artifact_ids_json: row.get(3)?,
            motions_json: row.get(4)?,
        })
    })?;

    let mut meeting_index: Vec<String> = Vec::new();
    meeting_index.push("# MOC - Meetings".to_string());
    meeting_index.push(String::new());
    meeting_index.push("This index is generated. Do not edit manually.".to_string());
    meeting_index.push(String::new());

    for r in rows {
        let m = r?;
        write_meeting_note(&paths, &m)?;
        meeting_index.push(format!(
            "- [[Meetings/{}|{}]]",
            m.id,
            m.index_title()
        ));
    }

    let meeting_moc_path = paths.index_dir.join("MOC - Meetings.md");
    fs::write(meeting_moc_path, meeting_index.join("\n"))?;

    Ok(())
}

#[derive(Debug)]
struct ArtifactRow {
    id: String,
    source_kind: String,
    source_value: String,
    retrieved_at: String,
    title: Option<String>,
    content_type: Option<String>,
    body_text: Option<String>,
    tags_json: String,
}

impl ArtifactRow {
    fn index_title(&self) -> String {
        self.title.clone().unwrap_or_else(|| self.id.clone())
    }
}

fn write_artifact_note(paths: &VaultPaths, a: &ArtifactRow) -> Result<()> {
    let note_path = paths.artifacts_dir.join(format!("{}.md", a.id));

    // Minimal frontmatter for later search/sorting
    let mut md = String::new();
    md.push_str("---\n");
    md.push_str(&format!("id: {}\n", a.id));
    md.push_str(&format!("retrieved_at: {}\n", a.retrieved_at));
    md.push_str(&format!("source_kind: {}\n", a.source_kind));
    md.push_str("source_value: |\n");
    md.push_str(&indent_yaml_block(&a.source_value));
    if let Some(ct) = &a.content_type {
        md.push_str(&format!("content_type: {}\n", ct));
    }
    md.push_str("tags_json: |\n");
    md.push_str(&indent_yaml_block(&a.tags_json));
    md.push_str("---\n\n");

    md.push_str(&format!("# {}\n\n", a.title.clone().unwrap_or_else(|| a.id.clone())));

    md.push_str("## Source\n");
    md.push_str(&format!("- Kind: `{}`\n", a.source_kind));
    md.push_str(&format!("- Value: {}\n", a.source_value));
    md.push_str(&format!("- Retrieved: `{}`\n\n", a.retrieved_at));

    md.push_str("## Extracted Text\n");
    match &a.body_text {
        Some(t) if !t.trim().is_empty() => {
            md.push_str(t);
            md.push('\n');
        }
        _ => {
            md.push_str("_No extracted text available._\n");
        }
    }

    fs::write(note_path, md)?;
    Ok(())
}

#[derive(Debug)]
struct MeetingRow {
    id: String,
    body_id: String,
    started_at: String,
    artifact_ids_json: String,
    motions_json: String,
}

impl MeetingRow {
    fn index_title(&self) -> String {
        format!("{} ({})", self.body_id, self.started_at)
    }
}

fn write_meeting_note(paths: &VaultPaths, meeting: &MeetingRow) -> Result<()> {
    let note_path = paths.meetings_dir.join(format!("{}.md", meeting.id));

    let mut md = String::new();
    md.push_str("---\n");
    md.push_str(&format!("id: {}\n", meeting.id));
    md.push_str(&format!("body_id: {}\n", meeting.body_id));
    md.push_str(&format!("started_at: {}\n", meeting.started_at));
    md.push_str("artifact_ids_json: |\n");
    md.push_str(&indent_yaml_block(&meeting.artifact_ids_json));
    md.push_str("motions_json: |\n");
    md.push_str(&indent_yaml_block(&meeting.motions_json));
    md.push_str("---\n\n");

    md.push_str(&format!("# Meeting {}\n\n", meeting.id));
    md.push_str(&format!("- Body: `{}`\n", meeting.body_id));
    md.push_str(&format!("- Started: `{}`\n", meeting.started_at));

    fs::write(note_path, md)?;
    Ok(())
}

fn indent_yaml_block(s: &str) -> String {
    // YAML block scalar requires indentation; keep it simple
    let mut out = String::new();
    for line in s.lines() {
        out.push_str("  ");
        out.push_str(line);
        out.push('\n');
    }
    if s.ends_with('\n') == false {
        // ensure trailing newline inside block
        // already added per line; this is fine
    }
    out
}
