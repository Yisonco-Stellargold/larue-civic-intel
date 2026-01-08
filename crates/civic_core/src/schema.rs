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
