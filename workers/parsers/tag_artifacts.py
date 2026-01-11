import argparse
import json
import re
import sys
from pathlib import Path
import tomllib

DEFAULT_OUT_DIR = "out"

DEFAULT_RULES = {
    "zoning": [
        "zoning",
        "zoning map",
        "zone change",
        "zoning amendment",
    ],
    "rezoning": [
        "rezoning",
        "rezone",
        "zone change",
        "map amendment",
    ],
    "variance": [
        "variance",
        "board of adjustment",
    ],
    "planning_commission": [
        "planning commission",
        "planning & zoning",
        "planning and zoning",
    ],
    "budget": [
        "budget",
        "fiscal year budget",
        "annual budget",
    ],
    "tax": [
        "tax",
        "property tax",
        "tax rate",
        "millage",
    ],
    "bond": [
        "bond",
        "bond issuance",
        "bond counsel",
    ],
    "appropriation": [
        "appropriation",
        "appropriations",
        "appropriated",
    ],
    "contract": [
        "contract",
        "agreement",
        "service agreement",
    ],
    "bid": [
        "bid",
        "bids",
        "request for bids",
        "invitation to bid",
    ],
    "procurement": [
        "procurement",
        "purchasing",
        "purchase order",
    ],
    "election": [
        "election",
        "election day",
        "election results",
    ],
    "clerk": [
        "county clerk",
        "clerk",
        "clerk's office",
    ],
    "ballot": [
        "ballot",
        "ballot measure",
        "ballot question",
    ],
    "school_board": [
        "school board",
        "board of education",
    ],
    "curriculum": [
        "curriculum",
        "instructional materials",
    ],
    "policy": [
        "policy",
        "policy update",
        "policy revision",
    ],
    "lawsuit": [
        "lawsuit",
        "litigation",
        "complaint",
    ],
    "settlement": [
        "settlement",
        "settle",
        "settlement agreement",
    ],
    "ordinance": [
        "ordinance",
        "ordinance amendment",
    ],
    "public_safety": [
        "public safety",
        "fire department",
        "emergency services",
        "police department",
    ],
    "land_sale": [
        "land sale",
        "real property sale",
        "surplus property",
    ],
    "eminent_domain": [
        "eminent domain",
        "condemnation",
    ],
}

DEFAULT_BROAD_TAGS = {"tax", "budget", "policy"}


def read_config(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def get_nested(config: dict, *keys, default=None):
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def parse_tags_yaml(path: Path) -> tuple[list[str], dict[str, list[str]]]:
    tags: list[str] = []
    rules: dict[str, list[str]] = {}
    if not path.exists():
        return tags, rules

    current_section = None
    current_tag = None

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("tags:"):
            current_section = "tags"
            current_tag = None
            continue
        if stripped.startswith("rules:"):
            current_section = "rules"
            current_tag = None
            continue
        if current_section == "tags" and stripped.startswith("-"):
            tag = stripped.lstrip("-").strip().strip("'\"")
            if tag:
                tags.append(tag)
            continue
        if current_section == "rules":
            if stripped.startswith("-") and current_tag:
                phrase = stripped.lstrip("-").strip().strip("'\"")
                if phrase:
                    rules.setdefault(current_tag, []).append(phrase)
                continue
            if ":" in stripped:
                key, _ = stripped.split(":", 1)
                current_tag = key.strip().strip("'\"")
                rules.setdefault(current_tag, [])
                continue

    return tags, rules


def normalize_text(text: str) -> str:
    return " ".join(text.split())


def build_phrase_pattern(phrase: str) -> re.Pattern:
    escaped = re.escape(phrase)
    escaped = escaped.replace("\\ ", "\\s+")
    return re.compile(rf"\b{escaped}\b", re.IGNORECASE)


def is_strong_phrase(phrase: str) -> bool:
    return " " in phrase or "-" in phrase


def count_phrase_hits(text: str, phrases: list[str]) -> tuple[int, list[str], bool]:
    hits = 0
    matched: list[str] = []
    strong_match = False
    for phrase in phrases:
        if not phrase:
            continue
        pattern = build_phrase_pattern(phrase)
        matches = list(pattern.finditer(text))
        if matches:
            hits += len(matches)
            if phrase not in matched:
                matched.append(phrase)
            if is_strong_phrase(phrase):
                strong_match = True
    return hits, matched, strong_match


def load_rules(config: dict) -> tuple[dict[str, list[str]], dict]:
    rules = {tag: phrases[:] for tag, phrases in DEFAULT_RULES.items()}
    tags_yaml, rules_yaml = parse_tags_yaml(Path("rubric/tags.yaml"))
    if rules_yaml:
        rules.update(rules_yaml)
    if tags_yaml:
        rules = {tag: rules[tag] for tag in rules if tag in tags_yaml}

    tagging_config = config.get("tagging", {}) if isinstance(config, dict) else {}
    config_rules = tagging_config.get("keywords") or tagging_config.get("rules")
    if isinstance(config_rules, dict):
        for tag, phrases in config_rules.items():
            if isinstance(phrases, list):
                rules[tag] = [str(p) for p in phrases if str(p).strip()]

    return rules, tagging_config


def ensure_tags(artifact: dict) -> list[str]:
    tags = artifact.get("tags")
    if isinstance(tags, list):
        return tags
    artifact["tags"] = []
    return artifact["tags"]


def add_tag(artifact: dict, tag: str) -> None:
    tags = ensure_tags(artifact)
    if tag not in tags:
        tags.append(tag)


def write_artifact(path: Path, artifact: dict) -> None:
    path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run() -> int:
    parser = argparse.ArgumentParser(description="Tag artifacts with issue categories.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--force", action="store_true", help="Re-tag even if tagged before.")
    args = parser.parse_args()

    config = read_config(args.config)
    tagging_config = get_nested(config, "tagging", default={})
    enabled = True
    if isinstance(tagging_config, dict):
        enabled = tagging_config.get("enabled", True)
    if not enabled:
        print("Tagging disabled via config.")
        return 0

    out_dir_value = get_nested(config, "storage", "out_dir", default=DEFAULT_OUT_DIR)
    out_dir = Path(out_dir_value)
    artifacts_dir = args.artifacts or (out_dir / "artifacts")

    if not artifacts_dir.exists():
        print(f"Artifacts directory not found: {artifacts_dir}")
        return 0

    artifact_paths = sorted(artifacts_dir.glob("*.json"), key=lambda path: path.name)
    if not artifact_paths:
        print(f"No artifact JSON files found in {artifacts_dir}.")
        return 0

    rules, tagging_config = load_rules(config)
    min_hits_default = int(tagging_config.get("min_hits_default", 1))
    min_hits_broad = int(tagging_config.get("min_hits_broad", 2))
    broad_tags = set(tagging_config.get("broad_tags", list(DEFAULT_BROAD_TAGS)))
    tag_min_hits = tagging_config.get("tag_min_hits", {})

    processed = 0
    tagged = 0
    skipped = 0
    forced = 0

    for artifact_path in artifact_paths:
        try:
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"Skipping invalid JSON {artifact_path.name}: {exc}")
            skipped += 1
            continue

        body_text = artifact.get("body_text")
        if not isinstance(body_text, str) or not body_text.strip():
            skipped += 1
            continue

        tags = ensure_tags(artifact)
        already_tagged = "issue_tagged" in tags
        if already_tagged and not args.force:
            skipped += 1
            continue
        if already_tagged and args.force:
            forced += 1

        processed += 1
        normalized = normalize_text(body_text)

        issue_tags: list[str] = []
        evidence: dict[str, list[str]] = {}

        for tag, phrases in rules.items():
            if not phrases:
                continue
            hits, matched, strong_match = count_phrase_hits(normalized, phrases)
            if not hits:
                continue
            min_hits = min_hits_default
            if tag in tag_min_hits:
                min_hits = int(tag_min_hits[tag])
            elif tag in broad_tags:
                min_hits = min_hits_broad
            if strong_match:
                min_hits = 1
            if hits >= min_hits:
                issue_tags.append(tag)
                evidence[tag] = sorted(matched)[:5]

        issue_tags = sorted(set(issue_tags))
        for tag in issue_tags:
            add_tag(artifact, tag)
        add_tag(artifact, "issue_tagged")

        artifact["issue_tags"] = issue_tags
        if issue_tags:
            artifact["tag_evidence"] = evidence
        else:
            artifact.pop("tag_evidence", None)

        write_artifact(artifact_path, artifact)

        if issue_tags:
            tagged += 1

    print(
        f"processed={processed} tagged={tagged} skipped={skipped} forced={forced}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
