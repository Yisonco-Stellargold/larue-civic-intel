import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
import tomllib

DEFAULT_OUT_DIR = "out"
DEFAULT_RATE_LIMIT_SECONDS = 1.0
DEFAULT_LIMIT_PER_RUN = 200
STATE_LIMIT = 10000
STATE_FILENAME = "wayback_state.json"
CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"

CONTENT_TYPE_EXTENSIONS = {
    "text/html": ".html",
    "application/pdf": ".pdf",
    "text/plain": ".txt",
    "application/json": ".json",
}


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


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"last_processed": {}, "seen_ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"last_processed": {}, "seen_ids": []}
    if not isinstance(data, dict):
        return {"last_processed": {}, "seen_ids": []}
    data.setdefault("last_processed", {})
    data.setdefault("seen_ids", [])
    if not isinstance(data["last_processed"], dict):
        data["last_processed"] = {}
    if not isinstance(data["seen_ids"], list):
        data["seen_ids"] = []
    return data


def save_state(path: Path, state: dict) -> None:
    seen_ids = state.get("seen_ids", [])
    if isinstance(seen_ids, list) and len(seen_ids) > STATE_LIMIT:
        state["seen_ids"] = seen_ids[-STATE_LIMIT:]
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def stable_id(original_url: str, timestamp: str) -> str:
    digest = hashlib.sha256(f"{original_url}|{timestamp}".encode("utf-8")).hexdigest()
    return f"wayback:{digest}"


def archived_url(original_url: str, timestamp: str) -> str:
    return f"https://web.archive.org/web/{timestamp}/{original_url}"


def determine_extension(content_type: str, fallback_url: str) -> str:
    if not content_type:
        parsed = urlparse(fallback_url)
        suffix = Path(parsed.path).suffix
        return suffix or ".bin"
    mime = content_type.split(";")[0].strip().lower()
    return CONTENT_TYPE_EXTENSIONS.get(mime, Path(urlparse(fallback_url).path).suffix or ".bin")


def derive_tags(urls: list[str], original_url: str) -> list[str]:
    tags = ["wayback", "history"]
    lowered = original_url.lower()
    if any("larue" in url.lower() for url in urls) or "larue" in lowered:
        tags.append("larue")
    return tags


def cdx_query(url: str, start: str | None, end: str | None, limit: int) -> list[dict[str, Any]]:
    params = [
        f"url={quote(url)}",
        "output=json",
        "fl=timestamp,original,mimetype,statuscode",
        "filter=statuscode:200",
        "collapse=digest",
        f"limit={limit}",
    ]
    if start:
        params.append(f"from={start}")
    if end:
        params.append(f"to={end}")
    query = "&".join(params)
    request_url = f"{CDX_ENDPOINT}?{query}"
    with urlopen(request_url) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not data:
        return []
    headers = data[0]
    rows = []
    for row in data[1:]:
        entry = dict(zip(headers, row))
        rows.append(entry)
    return rows


def download_snapshot(url: str, destination: Path) -> str:
    request = Request(url, headers={"User-Agent": "larue-civic-intel/1.0"})
    with urlopen(request) as response:
        content_type = response.headers.get("Content-Type", "")
        destination.write_bytes(response.read())
    return content_type


def run() -> None:
    parser = argparse.ArgumentParser(description="Wayback Machine historical backfill collector.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    config = read_config(args.config)
    enabled = get_nested(config, "sources", "wayback", "enabled", default=False)
    if not enabled:
        return

    out_dir_value = get_nested(config, "storage", "out_dir", default=DEFAULT_OUT_DIR)
    urls = get_nested(config, "sources", "wayback", "urls", default=[])
    rate_limit = float(
        get_nested(
            config,
            "sources",
            "wayback",
            "rate_limit_seconds",
            default=DEFAULT_RATE_LIMIT_SECONDS,
        )
    )
    limit_per_run = int(
        get_nested(
            config,
            "sources",
            "wayback",
            "limit_per_run",
            default=DEFAULT_LIMIT_PER_RUN,
        )
    )
    if args.limit is not None:
        limit_per_run = args.limit

    out_dir = Path(out_dir_value)
    artifacts_dir = out_dir / "artifacts"
    snapshots_dir = out_dir / "snapshots" / "wayback"
    state_dir = out_dir / "state"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    state_path = state_dir / STATE_FILENAME
    state = load_state(state_path)
    seen_ids: list[str] = state.get("seen_ids", [])
    last_processed: dict[str, str] = state.get("last_processed", {})

    if not isinstance(urls, list) or not urls:
        print("No Wayback URLs configured.")
        return

    total_found = 0
    total_new = 0
    total_skipped = 0

    remaining = limit_per_run

    for base_url in urls:
        if remaining <= 0:
            break
        if not isinstance(base_url, str) or not base_url.strip():
            continue

        query_url = base_url
        if base_url.endswith("/"):
            query_url = f"{base_url}*"

        start = args.start
        if args.resume and not args.start:
            start = last_processed.get(base_url)
        captures = cdx_query(query_url, start, args.end, remaining)
        if not captures:
            continue

        for capture in captures:
            timestamp = capture.get("timestamp", "")
            original_url = capture.get("original", base_url)
            if not timestamp or not original_url:
                continue
            total_found += 1
            artifact_id = stable_id(original_url, timestamp)
            if artifact_id in seen_ids:
                total_skipped += 1
                continue

            snapshot_url = archived_url(original_url, timestamp)
            snapshot_ext = ".bin"
            snapshot_path = snapshots_dir / f"{artifact_id}{snapshot_ext}"
            content_type = ""
            try:
                content_type = download_snapshot(snapshot_url, snapshot_path)
                snapshot_ext = determine_extension(content_type, original_url)
                if snapshot_path.suffix != snapshot_ext:
                    final_path = snapshot_path.with_suffix(snapshot_ext)
                    snapshot_path.rename(final_path)
                    snapshot_path = final_path
            except Exception as exc:
                print(f"Failed to download {snapshot_url}: {exc}")
                continue

            retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            )
            artifact = {
                "id": artifact_id,
                "source": {
                    "kind": "wayback",
                    "value": snapshot_url,
                    "retrieved_at": retrieved_at,
                },
                "title": f"Wayback snapshot: {original_url} @ {timestamp}",
                "body_text": None,
                "content_type": content_type.split(";")[0] if content_type else None,
                "tags": derive_tags(urls, original_url),
            }
            artifact_path = artifacts_dir / f"{artifact_id}.json"
            artifact_path.write_text(
                json.dumps(artifact, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

            seen_ids.append(artifact_id)
            total_new += 1
            last_processed[base_url] = timestamp
            remaining -= 1
            if remaining <= 0:
                break
            time.sleep(rate_limit)

    state["seen_ids"] = seen_ids
    state["last_processed"] = last_processed
    save_state(state_path, state)

    print(
        "Wayback summary: "
        f"found={total_found} new={total_new} skipped={total_skipped} "
        f"state_size={len(state.get('seen_ids', []))}"
    )


if __name__ == "__main__":
    run()
