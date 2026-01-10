use anyhow::Result;
use rusqlite::Connection;
use serde_json;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use time::{Duration, OffsetDateTime};
use time::format_description::FormatItem;
use time::format_description::well_known::Rfc3339;

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

    let mut issue_counts: BTreeMap<String, usize> = BTreeMap::new();

    for r in rows {
        let a = r?;
        write_artifact_note(&paths, &a)?;
        index_lines.push(format!("- [[Artifacts/{}|{}]]", a.id, a.index_title()));
        update_issue_counts(&a.tags_json, &mut issue_counts);
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

    // 4) Write decision meeting notes
    write_decision_meeting_notes(conn, &paths)?;

    // 5) Write weekly score report
    write_score_report(conn, &paths)?;

    // 6) Write reports MOC
    write_reports_moc(&paths)?;

    // 7) Write issue MOC
    let mut issue_lines: Vec<String> = Vec::new();
    issue_lines.push("# MOC - Issues".to_string());
    issue_lines.push(String::new());
    issue_lines.push("This index is generated. Do not edit manually.".to_string());
    issue_lines.push(String::new());
    issue_lines.push("## Weekly Reports".to_string());
    issue_lines.push(String::new());

    let reports_dir = paths.root.join("Reports").join("Weekly");
    if reports_dir.exists() {
        let mut report_links: Vec<String> = fs::read_dir(&reports_dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
                    return None;
                }
                let stem = path.file_stem()?.to_str()?.to_string();
                Some(format!("- [[Reports/Weekly/{stem}|{stem}]]"))
            })
            .collect();
        report_links.sort();
        if report_links.is_empty() {
            issue_lines.push("_No weekly reports found._".to_string());
        } else {
            issue_lines.extend(report_links);
        }
    } else {
        issue_lines.push("_No weekly reports found._".to_string());
    }

    issue_lines.push(String::new());
    issue_lines.push("## Issue Tags".to_string());
    issue_lines.push(String::new());

    let mut issue_counts_vec: Vec<(String, usize)> = issue_counts.into_iter().collect();
    issue_counts_vec.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    if issue_counts_vec.is_empty() {
        issue_lines.push("_No issue tags found._".to_string());
    } else {
        for (tag, count) in issue_counts_vec {
            issue_lines.push(format!("- {tag} ({count})"));
        }
    }

    let issue_moc_path = paths.index_dir.join("MOC - Issues.md");
    fs::write(issue_moc_path, issue_lines.join("\n"))?;

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

#[derive(Debug)]
struct DecisionMeetingRow {
    id: String,
    body_id: String,
    body_name: String,
    started_at: String,
    artifact_ids_json: String,
}

#[derive(Debug)]
struct DecisionMotionRow {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    meeting_id: String,
    text: String,
    result: Option<String>,
    #[allow(dead_code)]
    index: i64,
}

fn write_decision_meeting_notes(conn: &Connection, paths: &VaultPaths) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT meetings.id, meetings.body_id, meetings.started_at, meetings.artifact_ids_json, bodies.name
        FROM meetings
        JOIN bodies ON meetings.body_id = bodies.id
        ORDER BY meetings.started_at DESC, meetings.id DESC
        "#,
    )?;
    let meetings = stmt.query_map([], |row| {
        Ok(DecisionMeetingRow {
            id: row.get(0)?,
            body_id: row.get(1)?,
            started_at: row.get(2)?,
            artifact_ids_json: row.get(3)?,
            body_name: row.get(4)?,
        })
    })?;

    for row in meetings {
        let meeting = row?;
        let date = meeting
            .started_at
            .split('T')
            .next()
            .unwrap_or(&meeting.started_at);
        let filename = format!("{date}-{}.md", meeting.body_id);
        let note_path = paths.meetings_dir.join(filename);

        let mut motion_stmt = conn.prepare(
            r#"
            SELECT id, meeting_id, text, result, motion_index
            FROM motions
            WHERE meeting_id = ?1
            ORDER BY motion_index ASC, id ASC
            "#,
        )?;
        let motions = motion_stmt.query_map([meeting.id.as_str()], |row| {
            Ok(DecisionMotionRow {
                id: row.get(0)?,
                meeting_id: row.get(1)?,
                text: row.get(2)?,
                result: row.get(3)?,
                index: row.get(4)?,
            })
        })?;

        let mut md = String::new();
        md.push_str("---\n");
        md.push_str(&format!("id: {}\n", meeting.id));
        md.push_str(&format!("body_id: {}\n", meeting.body_id));
        md.push_str(&format!("body_name: {}\n", meeting.body_name));
        md.push_str(&format!("started_at: {}\n", meeting.started_at));
        md.push_str("artifact_ids_json: |\n");
        md.push_str(&indent_yaml_block(&meeting.artifact_ids_json));
        md.push_str("---\n\n");

        md.push_str(&format!("# {} — {}\n\n", meeting.body_name, date));
        md.push_str("## Motions\n");

        let mut has_motions = false;
        for motion in motions {
            let motion = motion?;
            has_motions = true;
            let result = motion.result.unwrap_or_else(|| "unknown".to_string());
            md.push_str(&format!(
                "- {} ({})\n",
                motion.text.trim(),
                result
            ));
        }
        if !has_motions {
            md.push_str("_No motions recorded._\n");
        }

        md.push_str("\n## Source Artifacts\n");
        let artifact_ids: Vec<String> = serde_json::from_str(&meeting.artifact_ids_json)
            .unwrap_or_default();
        if artifact_ids.is_empty() {
            md.push_str("_No source artifacts recorded._\n");
        } else {
            for artifact_id in artifact_ids {
                md.push_str(&format!("- [[Artifacts/{artifact_id}|{artifact_id}]]\n"));
            }
        }

        fs::write(note_path, md)?;
    }

    Ok(())
}

fn write_score_report(conn: &Connection, paths: &VaultPaths) -> Result<()> {
    let now = OffsetDateTime::now_utc();
    let start = now - Duration::days(7);
    let date_format: &[FormatItem<'_>] = time::macros::format_description!("[year]-[month]-[day]");
    let date_str = now.format(date_format)?;
    let window_start = start.format(&Rfc3339)?;
    let window_end = now.format(&Rfc3339)?;

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
    let rows = stmt.query_map([window_start.as_str(), window_end.as_str()], |row| {
        let flags_json: String = row.get(1)?;
        let flags: Vec<String> = serde_json::from_str(&flags_json).unwrap_or_default();
        Ok((row.get::<_, f64>(0)?, flags, row.get::<_, String>(2)?))
    })?;

    let mut scores = Vec::new();
    let mut insufficient = 0usize;
    for row in rows {
        let (score, flags, text) = row?;
        if flags.iter().any(|flag| flag == "insufficient_evidence") {
            insufficient += 1;
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
    let top_negative = scores.iter().take(3).collect::<Vec<_>>();
    let top_positive = scores.iter().rev().take(3).collect::<Vec<_>>();

    let drift_flags = load_drift_flags(conn, &window_start, &window_end)?;

    let report_dir = paths.root.join("Reports").join("Weekly");
    fs::create_dir_all(&report_dir)?;
    let report_path = report_dir.join(format!("{date_str}-scores.md"));

    let mut md = String::new();
    md.push_str(&format!("# Rubric Scores {date_str}\n\n"));
    md.push_str(&format!("Window: {window_start} to {window_end} UTC\n\n"));
    if total_scored == 0 {
        md.push_str("_No decision scores available this week._\n");
    } else {
        md.push_str(&format!("- Average score: {:.1}\n", average_score));
        md.push_str(&format!("- Insufficient evidence: {insufficient}\n"));
        if !top_positive.is_empty() {
            md.push_str("\n## Top Positive\n");
            for (score, text) in top_positive {
                md.push_str(&format!("- {text} ({score:.1})\n"));
            }
        }
        if !top_negative.is_empty() {
            md.push_str("\n## Top Negative\n");
            for (score, text) in top_negative {
                md.push_str(&format!("- {text} ({score:.1})\n"));
            }
        }
        if !drift_flags.is_empty() {
            md.push_str("\n## Drift Flags\n");
            for flag in drift_flags {
                md.push_str(&format!("- {flag}\n"));
            }
        }
    }

    fs::write(report_path, md)?;
    Ok(())
}

fn write_reports_moc(paths: &VaultPaths) -> Result<()> {
    let mut report_lines = Vec::new();
    report_lines.push("# MOC - Reports".to_string());
    report_lines.push(String::new());
    report_lines.push("This index is generated. Do not edit manually.".to_string());
    report_lines.push(String::new());

    let reports_dir = paths.root.join("Reports").join("Weekly");
    if reports_dir.exists() {
        let mut report_links: Vec<String> = fs::read_dir(&reports_dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
                    return None;
                }
                let stem = path.file_stem()?.to_str()?.to_string();
                Some(format!("- [[Reports/Weekly/{stem}|{stem}]]"))
            })
            .collect();
        report_links.sort();
        if report_links.is_empty() {
            report_lines.push("_No weekly reports found._".to_string());
        } else {
            report_lines.extend(report_links);
        }
    } else {
        report_lines.push("_No weekly reports found._".to_string());
    }

    let moc_path = paths.index_dir.join("MOC - Reports.md");
    fs::write(moc_path, report_lines.join("\n"))?;
    Ok(())
}

fn load_drift_flags(conn: &Connection, window_start: &str, window_end: &str) -> Result<Vec<String>> {
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

fn update_issue_counts(tags_json: &str, issue_counts: &mut BTreeMap<String, usize>) {
    let tags: Vec<String> = serde_json::from_str(tags_json).unwrap_or_default();
    for tag in tags {
        if is_issue_tag(&tag) {
            *issue_counts.entry(tag).or_insert(0) += 1;
        }
    }
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
