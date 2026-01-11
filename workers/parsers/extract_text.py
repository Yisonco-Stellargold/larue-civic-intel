import argparse
import importlib
import importlib.util
import json
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
import tomllib

DEFAULT_OUT_DIR = "out"

BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "div",
    "dl",
    "dt",
    "dd",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
}

SKIP_TAGS = {"script", "style", "noscript"}

EXTENSION_MIME = {
    "html": "text/html",
    "htm": "text/html",
    "txt": "text/plain",
    "pdf": "application/pdf",
}


class HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in SKIP_TAGS:
            self._skip_depth += 1
            return
        if tag in BLOCK_TAGS:
            self._append_break()

    def handle_endtag(self, tag: str) -> None:
        if tag in SKIP_TAGS:
            if self._skip_depth > 0:
                self._skip_depth -= 1
            return
        if tag in BLOCK_TAGS:
            self._append_break()

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = data.strip()
        if text:
            self.parts.append(text)

    def _append_break(self) -> None:
        if not self.parts or self.parts[-1] != "\n":
            self.parts.append("\n")

    def get_text(self) -> str:
        merged: list[str] = []
        for part in self.parts:
            if part == "\n":
                if merged and merged[-1] != "\n":
                    merged.append("\n")
                elif not merged:
                    merged.append("\n")
                continue
            if merged and merged[-1] not in {"\n", " "}:
                merged.append(" ")
            merged.append(part)
        return "".join(merged)


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


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in text.split("\n"):
        collapsed = " ".join(line.split())
        if collapsed:
            lines.append(collapsed)
    return "\n".join(lines)


def extract_html_text(snapshot_path: Path) -> str:
    parser = HTMLTextExtractor()
    parser.feed(snapshot_path.read_text(encoding="utf-8", errors="replace"))
    return normalize_text(parser.get_text())


def extract_plain_text(snapshot_path: Path) -> str:
    return normalize_text(snapshot_path.read_text(encoding="utf-8", errors="replace"))


def extract_pdf_text(snapshot_path: Path) -> tuple[str | None, str | None]:
    if importlib.util.find_spec("pypdf") is None:
        return None, "pypdf not available"
    PdfReader = importlib.import_module("pypdf").PdfReader
    try:
        reader = PdfReader(str(snapshot_path))
        pages = [page.extract_text() or "" for page in reader.pages]
    except Exception as exc:  # noqa: BLE001
        return None, f"pypdf failed: {exc}"
    return normalize_text("\n".join(pages)), None


def resolve_content_type(artifact: dict, snapshot_path: Path) -> str:
    content_type = (artifact.get("content_type") or "").strip().lower()
    if content_type:
        content_type = content_type.split(";")[0].strip().lower()
    if not content_type or "/" not in content_type:
        ext = snapshot_path.suffix.lower().lstrip(".")
        if ext in EXTENSION_MIME:
            content_type = EXTENSION_MIME[ext]
    return content_type


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


def add_note(artifact: dict, message: str) -> None:
    notes = artifact.get("notes")
    if notes is None:
        artifact["notes"] = [message]
        return
    if isinstance(notes, list):
        notes.append(message)
        return
    if isinstance(notes, str):
        artifact["notes"] = [notes, message]
        return
    artifact["notes"] = [message]


def should_skip(artifact: dict) -> bool:
    body_text = artifact.get("body_text")
    if isinstance(body_text, str) and body_text.strip():
        return True
    return False


def build_snapshot_index(snapshots_dir: Path) -> dict[str, Path]:
    if not snapshots_dir.exists():
        return {}
    paths = sorted(
        [path for path in snapshots_dir.rglob("*") if path.is_file()],
        key=lambda path: path.as_posix(),
    )
    index: dict[str, Path] = {}
    for path in paths:
        stem = path.stem
        if stem not in index:
            index[stem] = path
    return index


def write_artifact(path: Path, artifact: dict) -> None:
    path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def extracted_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run() -> int:
    parser = argparse.ArgumentParser(
        description="Extract normalized text from artifact snapshots."
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()

    config = read_config(args.config)
    out_dir_value = get_nested(config, "storage", "out_dir", default=DEFAULT_OUT_DIR)
    out_dir = Path(out_dir_value)
    snapshots_dir = out_dir / "snapshots"
    artifacts_dir = args.artifacts

    if not artifacts_dir.exists():
        print(f"Artifacts directory not found: {artifacts_dir}")
        return 0

    artifact_paths = sorted(artifacts_dir.glob("*.json"), key=lambda path: path.name)
    if not artifact_paths:
        print(f"No artifact JSON files found in {artifacts_dir}.")
        return 0

    snapshot_index = build_snapshot_index(snapshots_dir)

    for artifact_path in artifact_paths:
        try:
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"Skipping invalid JSON {artifact_path.name}: {exc}")
            continue

        if should_skip(artifact):
            continue

        artifact_id = artifact.get("id")
        snapshot_path = None
        if isinstance(artifact_id, str):
            snapshot_path = snapshot_index.get(artifact_id)

        if snapshot_path is None:
            add_tag(artifact, "extract_failed")
            add_note(artifact, f"Snapshot not found for artifact id {artifact_id}.")
            write_artifact(artifact_path, artifact)
            continue

        content_type = resolve_content_type(artifact, snapshot_path)

        if content_type == "text/html":
            text = extract_html_text(snapshot_path)
        elif content_type == "text/plain":
            text = extract_plain_text(snapshot_path)
        elif content_type == "application/pdf":
            text, note = extract_pdf_text(snapshot_path)
            if text is None:
                add_tag(artifact, "pdf_extract_todo")
                add_note(
                    artifact,
                    note or "PDF extraction requires an optional dependency.",
                )
                write_artifact(artifact_path, artifact)
                continue
        else:
            add_tag(artifact, "unextractable")
            add_note(artifact, f"Unsupported content_type: {content_type or 'unknown'}.")
            write_artifact(artifact_path, artifact)
            continue

        artifact["body_text"] = text
        add_tag(artifact, "text_extracted")
        artifact["extracted_at"] = extracted_timestamp()
        write_artifact(artifact_path, artifact)

    return 0


if __name__ == "__main__":
    sys.exit(run())
