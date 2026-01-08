use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceRef {
    pub kind: String,        // e.g. "url", "file", "rss", "public_notice"
    pub value: String,       // e.g. "https://..."
    pub retrieved_at: String // ISO-8601 timestamp (UTC recommended)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Artifact {
    pub id: String,                // stable identifier you assign
    pub source: SourceRef,
    pub title: Option<String>,
    pub body_text: Option<String>, // extracted plain text (if available)
    pub content_type: Option<String>, // "text/html", "application/pdf", etc.
    pub tags: Vec<String>,         // lightweight labels from collectors/parsers
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Body {
    pub id: String,
    pub name: String,
    pub kind: String,          // e.g. "fiscal_court", "school_board"
    pub jurisdiction: String, // e.g. "LaRue County, KY"
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Official {
    pub id: String,
    pub full_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Meeting {
    pub id: String,
    pub body_id: String,
    pub started_at: String,       // ISO-8601
    pub ended_at: Option<String>, // ISO-8601
    pub artifact_ids: Vec<String>, // evidence references to Artifact.id
    pub motions: Vec<Motion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Motion {
    pub id: String,
    pub meeting_id: String,
    pub title: String,
    pub description: Option<String>,
    pub result: Option<String>, // e.g. "passed", "failed"
    pub votes: Vec<Vote>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Vote {
    pub motion_id: String,
    pub official_id: String,
    pub value: String, // "yes" | "no" | "abstain" | "absent"
}
