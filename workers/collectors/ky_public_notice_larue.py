import argparse
import json
from pathlib import Path
import tomllib

DEFAULT_OUT_DIR = "out"
DEFAULT_QUERY = "Larue"
DEFAULT_TAGS = ["public_notice", "larue", "ky"]


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


def write_outputs(out_dir: Path, query: str, tags: list[str]) -> None:
    artifacts_dir = out_dir / "artifacts"
    snapshots_dir = out_dir / "snapshots"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    payload = {"query": query, "tags": tags}
    (artifacts_dir / "ky_public_notice_manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (snapshots_dir / "ky_public_notice_snapshot.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect KY public notices for LaRue.")
    parser.add_argument("--config", type=Path, help="Path to a config TOML file.")
    args = parser.parse_args()

    config = {}
    if args.config:
        config = read_config(args.config)

    out_dir, query, tags = resolve_settings(config)
    write_outputs(out_dir, query, tags)


if __name__ == "__main__":
    main()
