import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
import tomllib

DEFAULT_OUT_DIR = "out"
TARGET_ISSUE_TAGS = {
    "ordinance",
    "budget",
    "contract",
    "bid",
    "rezoning",
    "variance",
    "tax",
    "bond",
}

MOTION_PATTERNS = [
    re.compile(r"motion by (?P<mover>[^,]+),?\s+second by (?P<seconder>[^.]+)", re.I),
    re.compile(
        r"moved by (?P<mover>[^,]+) and seconded by (?P<seconder>[^.]+)",
        re.I,
    ),
    re.compile(r"upon motion(?: of)? (?P<mover>[^,]+)?", re.I),
]

VOICE_VOTE_PATTERNS = {
    "passed": ["motion carried", "motion passed", "approved"],
    "failed": ["motion failed", "failed motion", "motion fails"],
}

ROLL_CALL_LABELS = {"aye": "ayes", "nay": "nays", "abstain": "abstain"}


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


def read_artifact(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


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


def stable_meeting_id(body_id: str, date_str: str) -> str:
    return f"{body_id}:{date_str.replace('-', '')}"


def stable_motion_id(meeting_id: str, index: int) -> str:
    return f"{meeting_id}:motion:{index:02d}"


def stable_vote_id(motion_id: str) -> str:
    return f"{motion_id}:vote"


def parse_date(text: str) -> str | None:
    match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
    if match:
        year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return format_date(year, month, day)

    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if match:
        month, day, year = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return format_date(year, month, day)

    month_names = (
        "January|February|March|April|May|June|July|August|September|October|November|December"
    )
    match = re.search(rf"({month_names})\s+(\d{{1,2}}),\s*(\d{{4}})", text)
    if match:
        month = datetime.strptime(match.group(1), "%B").month
        day = int(match.group(2))
        year = int(match.group(3))
        return format_date(year, month, day)

    return None


def format_date(year: int, month: int, day: int) -> str | None:
    try:
        value = datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None
    return value.date().isoformat()


def meeting_type(text: str) -> str | None:
    lowered = text.lower()
    if "special meeting" in lowered:
        return "special"
    if "work session" in lowered:
        return "work_session"
    if "regular meeting" in lowered:
        return "regular"
    return None


def normalize_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def detect_motions(lines: list[str]) -> list[dict]:
    motions = []
    for idx, line in enumerate(lines):
        motion_match = None
        for pattern in MOTION_PATTERNS:
            motion_match = pattern.search(line)
            if motion_match:
                break
        if not motion_match:
            continue

        remainder = line[motion_match.end() :].strip(" .:-")
        motion_text = remainder
        if not motion_text and idx + 1 < len(lines):
            next_line = lines[idx + 1].strip()
            if next_line and not any(pattern.search(next_line) for pattern in MOTION_PATTERNS):
                motion_text = next_line

        motions.append(
            {
                "line_index": idx,
                "text": motion_text,
                "moved_by": motion_match.groupdict().get("mover"),
                "seconded_by": motion_match.groupdict().get("seconder"),
            }
        )
    return motions


def detect_vote(block_text: str) -> dict | None:
    lowered = block_text.lower()
    vote = {
        "vote_type": None,
        "outcome": None,
        "ayes": [],
        "nays": [],
        "abstain": [],
    }

    roll_call_lines = []
    for line in block_text.splitlines():
        if any(label in line.lower() for label in ["aye", "nay", "abstain"]):
            roll_call_lines.append(line)
    if roll_call_lines:
        vote["vote_type"] = "roll_call"
        for line in roll_call_lines:
            for label, key in ROLL_CALL_LABELS.items():
                if label in line.lower():
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        names = [name.strip() for name in parts[1].split(",") if name.strip()]
                        vote[key] = names
        if vote["ayes"] or vote["nays"] or vote["abstain"]:
            if len(vote["ayes"]) > len(vote["nays"]):
                vote["outcome"] = "passed"
            elif len(vote["nays"]) > len(vote["ayes"]):
                vote["outcome"] = "failed"
        return vote

    for outcome, phrases in VOICE_VOTE_PATTERNS.items():
        if any(phrase in lowered for phrase in phrases):
            vote["vote_type"] = "voice"
            vote["outcome"] = outcome
            return vote

    return None


def parse_meeting(artifact_id: str, body_text: str) -> tuple[dict, list[dict], list[dict]] | None:
    date_str = parse_date(body_text)
    if not date_str:
        return None

    meeting_id = stable_meeting_id("larue-fiscal-court", date_str)
    meeting = {
        "id": meeting_id,
        "body_id": "larue-fiscal-court",
        "body_name": "LaRue County Fiscal Court",
        "started_at": f"{date_str}T00:00:00Z",
        "meeting_type": meeting_type(body_text),
        "artifact_ids": [artifact_id],
    }

    lines = normalize_lines(body_text)
    motions_raw = detect_motions(lines)
    motions = []
    votes = []

    for index, motion in enumerate(motions_raw):
        motion_id = stable_motion_id(meeting_id, index + 1)
        motion_text = motion.get("text") or ""
        motion_record = {
            "id": motion_id,
            "meeting_id": meeting_id,
            "index": index + 1,
            "text": motion_text,
            "moved_by": clean_person(motion.get("moved_by")),
            "seconded_by": clean_person(motion.get("seconded_by")),
            "result": None,
        }

        next_index = motions_raw[index + 1]["line_index"] if index + 1 < len(motions_raw) else None
        block_lines = lines[motion["line_index"] : next_index] if next_index else lines[motion["line_index"] :]
        block_text = "\n".join(block_lines)
        vote = detect_vote(block_text)
        if vote:
            if vote.get("outcome"):
                motion_record["result"] = vote["outcome"]
            vote_record = {
                "id": stable_vote_id(motion_id),
                "motion_id": motion_id,
                "vote_type": vote.get("vote_type"),
                "outcome": vote.get("outcome"),
                "ayes": vote.get("ayes", []),
                "nays": vote.get("nays", []),
                "abstain": vote.get("abstain", []),
            }
            votes.append(vote_record)

        motions.append(motion_record)

    return meeting, motions, votes


def clean_person(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().strip(".-")


def write_decision(out_dir: Path, artifact_id: str, meeting: dict, motions: list[dict], votes: list[dict]) -> None:
    decisions_dir = out_dir / "decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "meeting": meeting,
        "motions": motions,
        "votes": votes,
    }
    path = decisions_dir / f"meeting_{artifact_id}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse meeting minutes into decisions JSON.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--force", action="store_true", help="Re-parse already parsed artifacts.")
    args = parser.parse_args()

    config = read_config(args.config)
    out_dir_value = get_nested(config, "storage", "out_dir", default=DEFAULT_OUT_DIR)
    out_dir = Path(out_dir_value)

    artifacts_dir = args.artifacts
    if not artifacts_dir.exists():
        print(f"Artifacts directory not found: {artifacts_dir}")
        return 0

    artifact_paths = sorted(artifacts_dir.glob("*.json"), key=lambda path: path.name)
    if not artifact_paths:
        print(f"No artifact JSON files found in {artifacts_dir}.")
        return 0

    artifacts_checked = 0
    meetings_parsed = 0
    motions_found = 0
    votes_found = 0

    for artifact_path in artifact_paths:
        try:
            artifact = read_artifact(artifact_path)
        except json.JSONDecodeError as exc:
            print(f"Skipping invalid JSON {artifact_path.name}: {exc}")
            continue
        tags = ensure_tags(artifact)
        if "issue_tagged" not in tags:
            continue
        issue_tags = artifact.get("issue_tags", [])
        if not isinstance(issue_tags, list):
            continue
        if not any(tag in TARGET_ISSUE_TAGS for tag in issue_tags):
            continue
        if "meeting_parsed" in tags and not args.force:
            continue
        body_text = artifact.get("body_text")
        if not isinstance(body_text, str) or not body_text.strip():
            continue

        artifacts_checked += 1
        parsed = parse_meeting(artifact.get("id", ""), body_text)
        if not parsed:
            continue
        meeting, motions, votes = parsed

        write_decision(out_dir, artifact.get("id", ""), meeting, motions, votes)
        add_tag(artifact, "meeting_parsed")
        artifact["parsed_meeting_id"] = meeting["id"]
        artifact_path.write_text(
            json.dumps(artifact, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        meetings_parsed += 1
        motions_found += len(motions)
        votes_found += len(votes)

    print(
        f"artifacts_checked={artifacts_checked} meetings_parsed={meetings_parsed} "
        f"motions_found={motions_found} votes_found={votes_found}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
