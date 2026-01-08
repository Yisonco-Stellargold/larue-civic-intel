import argparse
import hashlib
import json
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
import re

from pypdf import PdfReader

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self.parts.append(text)

    def get_text(self) -> str:
        return "\n".join(self.parts)


def read_artifact(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def extract_text(snapshot_path: Path) -> str:
    if snapshot_path.suffix.lower() == ".pdf":
        reader = PdfReader(str(snapshot_path))
        pages = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        return "\n".join(pages)

    if snapshot_path.suffix.lower() in {".html", ".htm"}:
        parser = TextExtractor()
        parser.feed(snapshot_path.read_text(encoding="utf-8", errors="replace"))
        return parser.get_text()

    return snapshot_path.read_text(encoding="utf-8", errors="replace")


def parse_date_from_text(text: str) -> datetime | None:
    for match in re.finditer(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text):
        year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            continue

    month_names = "|".join(MONTHS.keys())
    for match in re.finditer(
        rf"({month_names})\s+(\d{{1,2}}),\s+(\d{{4}})",
        text,
        flags=re.IGNORECASE,
    ):
        month_name = match.group(1).lower()
        month = MONTHS.get(month_name)
        day = int(match.group(2))
        year = int(match.group(3))
        if month is None:
            continue
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def parse_date_from_filename(snapshot_path: Path) -> datetime | None:
    return parse_date_from_text(snapshot_path.stem)


def infer_started_at(text: str, snapshot_path: Path, retrieved_at: str) -> str:
    parsed = parse_date_from_text(text) or parse_date_from_filename(snapshot_path)
    if parsed is None:
        try:
            retrieved = datetime.fromisoformat(retrieved_at.replace("Z", "+00:00"))
            parsed = datetime(
                retrieved.year,
                retrieved.month,
                retrieved.day,
                tzinfo=timezone.utc,
            )
        except ValueError:
            parsed = datetime.now(timezone.utc)
    return parsed.replace(hour=0, minute=0, second=0, microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def detect_motions(text: str) -> list[dict]:
    motions = []
    for line in text.splitlines():
        lower = line.lower()
        if "roll call" in lower or "roll-call" in lower:
            motions.append({"text": line.strip(), "result": None})
    return motions


def meeting_id_from_url(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return f"larue_fiscal_court_meeting:{digest}"


def write_meeting(out_dir: Path, meeting: dict) -> None:
    meetings_dir = out_dir / "meetings"
    meetings_dir.mkdir(parents=True, exist_ok=True)
    meeting_path = meetings_dir / f"{meeting['id']}.json"
    meeting_path.write_text(
        json.dumps(meeting, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse a meeting snapshot into Meeting JSON.")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    artifact = read_artifact(args.artifact)
    snapshot_path = args.snapshot
    text = extract_text(snapshot_path)

    source = artifact.get("source", {})
    source_url = source.get("value", "")
    retrieved_at = source.get("retrieved_at", "")

    meeting = {
        "id": meeting_id_from_url(source_url),
        "body_id": "larue-fiscal-court",
        "started_at": infer_started_at(text, snapshot_path, retrieved_at),
        "artifact_ids": [artifact.get("id", "")],
        "motions": detect_motions(text),
    }

    write_meeting(args.out_dir, meeting)


if __name__ == "__main__":
    main()
