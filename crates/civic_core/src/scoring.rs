use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ScoreResult {
    pub overall_score: f64,
    pub axis_scores: HashMap<String, f64>,
    pub constitutional_refs: Vec<String>,
    pub evidence: Vec<String>,
    pub confidence: f64,
    pub flags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DecisionScore {
    pub id: String,
    pub meeting_id: Option<String>,
    pub motion_id: Option<String>,
    pub vote_id: Option<String>,
    pub overall_score: f64,
    pub axis_scores: HashMap<String, f64>,
    pub constitutional_refs: Vec<String>,
    pub evidence: Vec<String>,
    pub confidence: f64,
    pub flags: Vec<String>,
    pub computed_at: String,
}

#[derive(Debug, Clone)]
pub struct LinkedArtifact {
    pub id: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Rubric {
    pub config: RubricConfig,
    pub axis_weights: HashMap<String, f64>,
    pub scoring_rules: ScoringRules,
    pub evidence_rules: EvidenceRules,
    pub bias_controls: BiasControls,
    pub us_constitution: HashMap<String, Vec<String>>,
    pub ky_constitution: HashMap<String, Vec<String>>,
    pub rubric_tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RubricConfig {
    pub general: RubricGeneral,
    pub evidence: RubricEvidence,
    pub output: RubricOutput,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RubricGeneral {
    pub score_floor: f64,
    pub score_ceiling: f64,
    pub neutral_score: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RubricEvidence {
    pub minimum_confidence: f64,
    pub unknown_penalty: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RubricOutput {
    pub rounding: u32,
    pub include_axis_breakdown: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct WeightsFile {
    axis_weights: HashMap<String, f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct TagsFile {
    tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ScoringRules {
    pub vote_yes_effect: VoteEffect,
    pub vote_no_effect: VoteEffect,
    pub abstain_penalty: f64,
    pub absent_penalty: f64,
    pub unknown_motion_penalty: f64,
}

#[derive(Debug, Clone)]
pub enum VoteEffect {
    Inherit,
    Invert,
}

#[derive(Debug, Clone, Deserialize)]
struct ScoringRulesFile {
    rules: HashMap<String, ScoringRuleEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScoringRuleEntry {
    effect: Option<String>,
    penalty: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct EvidenceRules {
    pub minimum_confidence: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct EvidenceRulesFile {
    requirements: EvidenceRequirements,
}

#[derive(Debug, Clone, Deserialize)]
struct EvidenceRequirements {
    motion_scoring: EvidenceMotionRequirements,
}

#[derive(Debug, Clone, Deserialize)]
struct EvidenceMotionRequirements {
    minimum_confidence: f64,
}

#[derive(Debug, Clone)]
pub struct BiasControls {
    pub spending_bias_penalty: f64,
    pub drift_threshold: f64,
    pub drift_window: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct BiasControlsFile {
    controls: HashMap<String, BiasControlEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct BiasControlEntry {
    penalty: Option<f64>,
    #[allow(dead_code)]
    modifier: Option<f64>,
    threshold: Option<f64>,
    window: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConstitutionMapEntry {
    amendments: Option<Vec<i32>>,
    sections: Option<Vec<String>>,
    principles: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConstitutionMapFile {
    #[serde(flatten)]
    axes: HashMap<String, ConstitutionMapEntry>,
}

impl Rubric {
    pub fn load_from_dir(path: &Path) -> Result<Self> {
        let config_path = path.join("rubric_config.toml");
        let config_str = fs::read_to_string(&config_path)?;
        let config: RubricConfig = toml::from_str(&config_str)?;

        let weights_path = path.join("weights.yaml");
        let weights_str = fs::read_to_string(&weights_path)?;
        let weights: WeightsFile = serde_yaml::from_str(&weights_str)?;

        let scoring_rules_path = path.join("scoring_rules.yaml");
        let scoring_rules_str = fs::read_to_string(&scoring_rules_path)?;
        let scoring_rules_file: ScoringRulesFile = serde_yaml::from_str(&scoring_rules_str)?;
        let scoring_rules = parse_scoring_rules(scoring_rules_file)?;

        let evidence_rules_path = path.join("evidence_rules.yaml");
        let evidence_rules_str = fs::read_to_string(&evidence_rules_path)?;
        let evidence_file: EvidenceRulesFile = serde_yaml::from_str(&evidence_rules_str)?;

        let bias_controls_path = path.join("bias_controls.yaml");
        let bias_controls_str = fs::read_to_string(&bias_controls_path)?;
        let bias_file: BiasControlsFile = serde_yaml::from_str(&bias_controls_str)?;
        let bias_controls = parse_bias_controls(&bias_file);

        let tags_path = path.join("tags.yaml");
        let tags_str = fs::read_to_string(&tags_path)?;
        let tags_file: TagsFile = serde_yaml::from_str(&tags_str)?;

        let us_constitution = load_constitution_map(&path.join("us_constitution_map.yaml"))?;
        let ky_constitution = load_constitution_map(&path.join("kentucky_constitution_map.yaml"))?;

        Ok(Self {
            config,
            axis_weights: weights.axis_weights,
            scoring_rules,
            evidence_rules: EvidenceRules {
                minimum_confidence: evidence_file.requirements.motion_scoring.minimum_confidence,
            },
            bias_controls,
            us_constitution,
            ky_constitution,
            rubric_tags: tags_file.tags,
        })
    }
}

fn parse_scoring_rules(file: ScoringRulesFile) -> Result<ScoringRules> {
    let vote_yes = file
        .rules
        .get("vote_yes")
        .and_then(|entry| entry.effect.as_deref())
        .unwrap_or("inherit");
    let vote_no = file
        .rules
        .get("vote_no")
        .and_then(|entry| entry.effect.as_deref())
        .unwrap_or("invert");
    let abstain = file
        .rules
        .get("abstain")
        .and_then(|entry| entry.penalty)
        .unwrap_or(0.0);
    let absent = file
        .rules
        .get("absent")
        .and_then(|entry| entry.penalty)
        .unwrap_or(0.0);
    let unknown = file
        .rules
        .get("unknown_motion")
        .and_then(|entry| entry.penalty)
        .unwrap_or(0.0);

    Ok(ScoringRules {
        vote_yes_effect: parse_vote_effect(vote_yes)?,
        vote_no_effect: parse_vote_effect(vote_no)?,
        abstain_penalty: abstain,
        absent_penalty: absent,
        unknown_motion_penalty: unknown,
    })
}

fn parse_vote_effect(value: &str) -> Result<VoteEffect> {
    match value {
        "inherit" => Ok(VoteEffect::Inherit),
        "invert" => Ok(VoteEffect::Invert),
        _ => Err(anyhow!("Unknown vote effect: {value}")),
    }
}

fn parse_bias_controls(file: &BiasControlsFile) -> BiasControls {
    let spending = file
        .controls
        .get("spending_bias")
        .and_then(|entry| entry.penalty)
        .unwrap_or(0.0);
    let drift_threshold = file
        .controls
        .get("drift_threshold")
        .and_then(|entry| entry.threshold)
        .unwrap_or(2.0);
    let drift_window = file
        .controls
        .get("drift_window")
        .and_then(|entry| entry.window)
        .unwrap_or(20);
    BiasControls {
        spending_bias_penalty: spending,
        drift_threshold,
        drift_window,
    }
}

fn load_constitution_map(path: &PathBuf) -> Result<HashMap<String, Vec<String>>> {
    let raw = fs::read_to_string(path)?;
    let parsed: ConstitutionMapFile = serde_yaml::from_str(&raw)?;
    let mut map = HashMap::new();
    for (axis, entry) in parsed.axes {
        let mut refs = Vec::new();
        if let Some(amendments) = entry.amendments {
            refs.extend(amendments.into_iter().map(|value| format!("Amendment {value}")));
        }
        if let Some(sections) = entry.sections {
            refs.extend(sections.into_iter().map(|value| format!("Section {value}")));
        }
        if let Some(principles) = entry.principles {
            refs.extend(principles.into_iter().map(|value| format!("Principle {value}")));
        }
        map.insert(axis, refs);
    }
    Ok(map)
}

pub fn compute_motion_score(
    motion_text: &str,
    linked_artifacts: &[LinkedArtifact],
    rubric: &Rubric,
) -> ScoreResult {
    let (issue_tags, evidence) = collect_issue_tags(linked_artifacts, rubric);
    let mut axis_scores: HashMap<String, f64> = HashMap::new();
    let mut flags = Vec::new();
    let mut evidence_list = evidence;

    let mut confidence = if issue_tags.is_empty() {
        0.0
    } else {
        rubric.evidence_rules.minimum_confidence
    };

    apply_tag_axis_scores(
        &issue_tags,
        motion_text,
        rubric,
        &mut axis_scores,
        &mut evidence_list,
    );

    let mut overall_score = weighted_overall(&axis_scores, &rubric.axis_weights);

    if axis_scores.values().all(|value| value.abs() < f64::EPSILON) {
        flags.push("insufficient_evidence".to_string());
        overall_score = rubric.config.general.neutral_score;
        confidence = 0.0;
    }

    overall_score = clamp_score(
        overall_score,
        rubric.config.general.score_floor,
        rubric.config.general.score_ceiling,
    );
    overall_score = round_score(overall_score, rubric.config.output.rounding);

    for value in axis_scores.values_mut() {
        *value = round_score(*value, rubric.config.output.rounding);
    }

    let constitutional_refs = build_constitution_refs(&axis_scores, rubric);

    ScoreResult {
        overall_score,
        axis_scores,
        constitutional_refs,
        evidence: evidence_list,
        confidence,
        flags,
    }
}

pub fn compute_vote_score(vote: &Value, rubric: &Rubric) -> ScoreResult {
    let mut score = ScoreResult {
        overall_score: rubric.config.general.neutral_score,
        axis_scores: HashMap::new(),
        constitutional_refs: Vec::new(),
        evidence: vec!["vote_without_motion".to_string()],
        confidence: 0.0,
        flags: vec!["insufficient_evidence".to_string()],
    };

    let vote_type = vote.get("vote_type").and_then(|value| value.as_str());
    let outcome = vote.get("outcome").and_then(|value| value.as_str());
    if vote_type.is_none() || outcome.is_none() {
        return score;
    }

    score.evidence.push("vote_recorded".to_string());
    score
}

pub fn compute_vote_score_with_motion(
    motion_score: &ScoreResult,
    vote_choice: VoteChoice,
    rubric: &Rubric,
) -> ScoreResult {
    let mut axis_scores = motion_score.axis_scores.clone();
    let evidence = vec![format!("vote_choice:{vote_choice}")];
    let mut flags = Vec::new();

    match vote_choice {
        VoteChoice::Aye => apply_vote_effect(&mut axis_scores, &rubric.scoring_rules.vote_yes_effect),
        VoteChoice::Nay => apply_vote_effect(&mut axis_scores, &rubric.scoring_rules.vote_no_effect),
        VoteChoice::Abstain => {
            flags.push("abstain".to_string());
            apply_flat_penalty(&mut axis_scores, rubric.scoring_rules.abstain_penalty);
        }
        VoteChoice::Absent => {
            flags.push("absent".to_string());
            apply_flat_penalty(&mut axis_scores, rubric.scoring_rules.absent_penalty);
        }
    }

    let mut overall_score = weighted_overall(&axis_scores, &rubric.axis_weights);
    overall_score = clamp_score(
        overall_score,
        rubric.config.general.score_floor,
        rubric.config.general.score_ceiling,
    );
    overall_score = round_score(overall_score, rubric.config.output.rounding);

    for value in axis_scores.values_mut() {
        *value = round_score(*value, rubric.config.output.rounding);
    }

    let constitutional_refs = build_constitution_refs(&axis_scores, rubric);

    if axis_scores.values().all(|value| value.abs() < f64::EPSILON) {
        flags.push("insufficient_evidence".to_string());
    }

    ScoreResult {
        overall_score,
        axis_scores,
        constitutional_refs,
        evidence,
        confidence: 1.0,
        flags,
    }
}

#[derive(Debug, Clone, Copy)]
pub enum VoteChoice {
    Aye,
    Nay,
    Abstain,
    Absent,
}

impl std::fmt::Display for VoteChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            VoteChoice::Aye => "aye",
            VoteChoice::Nay => "nay",
            VoteChoice::Abstain => "abstain",
            VoteChoice::Absent => "absent",
        };
        write!(f, "{value}")
    }
}

fn collect_issue_tags(
    linked_artifacts: &[LinkedArtifact],
    rubric: &Rubric,
) -> (Vec<String>, Vec<String>) {
    let mut tags = Vec::new();
    let mut evidence = Vec::new();
    for artifact in linked_artifacts {
        for tag in &artifact.tags {
            if is_issue_tag(tag) && !tags.contains(tag) {
                tags.push(tag.to_string());
                evidence.push(format!("tag:{tag}"));
            }
            if rubric.rubric_tags.iter().any(|rubric_tag| rubric_tag == tag) {
                evidence.push(format!("rubric_tag:{tag}"));
            }
        }
    }
    (tags, evidence)
}

fn apply_tag_axis_scores(
    issue_tags: &[String],
    motion_text: &str,
    rubric: &Rubric,
    axis_scores: &mut HashMap<String, f64>,
    evidence: &mut Vec<String>,
) {
    let spending_keywords = ["appropriation", "budget", "tax", "bond", "contract", "bid"];
    let lowered = motion_text.to_lowercase();
    for tag in issue_tags {
        let axes = tag_axes(tag);
        for axis in axes {
            let entry = axis_scores.entry(axis.to_string()).or_insert(0.0);
            if axis == "fiscal_restraint"
                && spending_keywords.iter().any(|keyword| lowered.contains(keyword))
            {
                *entry += rubric.bias_controls.spending_bias_penalty;
                evidence.push(format!("spending_bias:{tag}"));
            }
        }
    }
}

fn tag_axes(tag: &str) -> Vec<&'static str> {
    match tag {
        "budget" | "tax" | "bond" | "appropriation" | "contract" | "bid" | "procurement" => {
            vec!["fiscal_restraint"]
        }
        "zoning" | "rezoning" | "variance" | "land_sale" | "eminent_domain" => {
            vec!["property_rights"]
        }
        "transparency" | "ordinance" => vec!["transparency"],
        _ => Vec::new(),
    }
}

fn weighted_overall(axis_scores: &HashMap<String, f64>, weights: &HashMap<String, f64>) -> f64 {
    axis_scores
        .iter()
        .map(|(axis, score)| score * weights.get(axis).copied().unwrap_or(1.0))
        .sum()
}

fn clamp_score(value: f64, floor: f64, ceiling: f64) -> f64 {
    value.max(floor).min(ceiling)
}

fn round_score(value: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn build_constitution_refs(axis_scores: &HashMap<String, f64>, rubric: &Rubric) -> Vec<String> {
    let mut refs = Vec::new();
    for (axis, score) in axis_scores {
        if score.abs() < f64::EPSILON {
            continue;
        }
        if let Some(us_refs) = rubric.us_constitution.get(axis) {
            refs.extend(us_refs.iter().map(|value| format!("US {value}")));
        }
        if let Some(ky_refs) = rubric.ky_constitution.get(axis) {
            refs.extend(ky_refs.iter().map(|value| format!("KY {value}")));
        }
    }
    refs.sort();
    refs.dedup();
    refs
}

fn apply_vote_effect(axis_scores: &mut HashMap<String, f64>, effect: &VoteEffect) {
    match effect {
        VoteEffect::Inherit => {}
        VoteEffect::Invert => {
            for value in axis_scores.values_mut() {
                *value *= -1.0;
            }
        }
    }
}

fn apply_flat_penalty(axis_scores: &mut HashMap<String, f64>, penalty: f64) {
    if axis_scores.is_empty() {
        return;
    }
    for value in axis_scores.values_mut() {
        *value += penalty;
    }
}

fn is_issue_tag(tag: &str) -> bool {
    matches!(
        tag,
        "zoning"
            | "rezoning"
            | "variance"
            | "planning_commission"
            | "budget"
            | "tax"
            | "bond"
            | "appropriation"
            | "contract"
            | "bid"
            | "procurement"
            | "election"
            | "clerk"
            | "ballot"
            | "school_board"
            | "curriculum"
            | "policy"
            | "lawsuit"
            | "settlement"
            | "ordinance"
            | "public_safety"
            | "land_sale"
            | "eminent_domain"
            | "transparency"
    )
}
