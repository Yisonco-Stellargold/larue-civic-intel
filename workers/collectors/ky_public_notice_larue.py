import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import re
import tomllib

DEFAULT_OUT_DIR = "out"
DEFAULT_QUERY = "Larue"
DEFAULT_TAGS = ["public_notice", "larue", "ky"]
STATE_LIMIT = 5000
STATE_FILENAME = "ky_public_notice_state.json"


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


def resolve_settings(config: dict) -> tuple[Path, str, list[str]]:
    out_dir_value = get_nested(config, "storage", "out_dir", default=DEFAULT_OUT_DIR)
    query = get_nested(
        config,
        "sources",
        "ky_public_notice",
        "query",
        default=DEFAULT_QUERY,
    )
    tags = get_nested(
        config,
        "sources",
        "ky_public_notice",
        "tags",
        default=DEFAULT_TAGS,
    )
    if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
        tags = DEFAULT_TAGS
    return Path(out_dir_value), str(query), tags


def write_outputs(
    out_dir: Path,
    query: str,
    tags: list[str],
    artifacts_dir: Path,
    snapshots_dir: Path,
    state_dir: Path,
) -> tuple[int, int, int, int]:
    payload = {"query": query, "tags": tags}
    (artifacts_dir / "ky_public_notice_manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (snapshots_dir / "ky_public_notice_snapshot.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    state_path = state_dir / STATE_FILENAME
    seen_ids = load_state(state_path)
    if not query.strip():
        return 0, 0, 0, len(seen_ids)

    artifact_id = f"ky_public_notice:{query.lower()}"
    if artifact_id in seen_ids:
        return 1, 0, 1, len(seen_ids)

    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    artifact = {
        "id": artifact_id,
        "source": {
            "kind": "public_notice",
            "value": f"ky_public_notice:{query}",
            "retrieved_at": retrieved_at,
        },
        "title": f"KY Public Notice: {query}",
        "body_text": None,
        "content_type": "application/json",
        "tags": tags,
    }
    artifact_filename = f"ky_public_notice_{slugify(artifact_id)}.json"
    (artifacts_dir / artifact_filename).write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    seen_ids.append(artifact_id)
    save_state(state_path, seen_ids)
    return 1, 1, 0, len(seen_ids)


def load_state(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    seen_ids = data.get("seen_ids", [])
    if not isinstance(seen_ids, list):
        return []
    return [str(value) for value in seen_ids]


def save_state(path: Path, seen_ids: list[str]) -> None:
    trimmed = seen_ids[-STATE_LIMIT:]
    payload = {"seen_ids": trimmed}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect KY public notices for LaRue.")
    parser.add_argument("--config", type=Path, help="Path to a config TOML file.")
    args = parser.parse_args()

    config = {}
    if args.config:
        config = read_config(args.config)

    out_dir, query, tags = resolve_settings(config)
    artifacts_dir = out_dir / "artifacts"
    snapshots_dir = out_dir / "snapshots"
    state_dir = out_dir / "state"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    found, new, skipped, state_size = write_outputs(
        out_dir, query, tags, artifacts_dir, snapshots_dir, state_dir
    )
    if found == 0:
        print(f"No notices found for query \"{query}\".")
        print(f"Summary: found={found} new={new} skipped={skipped} state_size={state_size}")
        return
    print(f"Summary: found={found} new={new} skipped={skipped} state_size={state_size}")


if __name__ == "__main__":
    main()
