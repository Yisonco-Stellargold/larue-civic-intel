import argparse
import hashlib
import json
import time
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
import tomllib

DEFAULT_OUT_DIR = "out"
DEFAULT_RATE_LIMIT_SECONDS = 1.0
DEFAULT_LIMIT_PER_RUN = 200
STATE_LIMIT = 5000
STATE_FILENAME = "wayback_state.json"
CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"

CONTENT_TYPE_EXTENSIONS = {
    "text/html": ".html",
    "application/pdf": ".pdf",
    "text/plain": ".txt",
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
        return {"urls": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"urls": {}}
    if not isinstance(data, dict):
        return {"urls": {}}
    data.setdefault("urls", {})
    if not isinstance(data["urls"], dict):
        data["urls"] = {}
    return data


def save_state(path: Path, state: dict) -> None:
    urls = state.get("urls", {})
    for url_state in urls.values():
        seen_ids = url_state.get("seen_ids", [])
        if isinstance(seen_ids, list) and len(seen_ids) > STATE_LIMIT:
            url_state["seen_ids"] = seen_ids[-STATE_LIMIT:]
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def stable_id(original_url: str, timestamp: str) -> str:
    return hashlib.sha256(f"{original_url}{timestamp}".encode("utf-8")).hexdigest()[:16]


def change_id(original_url: str, timestamp: str) -> str:
    return hashlib.sha256(f"{original_url}change{timestamp}".encode("utf-8")).hexdigest()[:16]


def archived_url(original_url: str, timestamp: str) -> str:
    return f"https://web.archive.org/web/{timestamp}/{original_url}"


def determine_extension(content_type: str, fallback_url: str) -> str:
    if not content_type:
        return ".bin"
    mime = content_type.split(";")[0].strip().lower()
    return CONTENT_TYPE_EXTENSIONS.get(mime, ".bin")


def derive_tags(original_url: str, keywords: list[str], include_change: bool) -> list[str]:
    tags = ["wayback", "history"]
    if include_change:
        tags.append("change")
    lowered = original_url.lower()
    for keyword in keywords:
        if keyword.lower() in lowered:
            tags.append("high_impact")
            break
    return tags


def cdx_query(
    url: str,
    start: str | None,
    end: str | None,
    limit: int,
    match_type: str | None,
    sort: str | None,
    throttle: "RateLimiter",
) -> list[dict[str, Any]]:
    params = [
        f"url={quote(url)}",
        "output=json",
        "fl=timestamp,original,mimetype,statuscode,digest",
        "filter=statuscode:200",
        f"limit={limit}",
    ]
    if match_type:
        params.append(f"matchType={match_type}")
    if sort:
        params.append(f"sort={sort}")
    if start:
        params.append(f"from={start}")
    if end:
        params.append(f"to={end}")
    query = "&".join(params)
    request_url = f"{CDX_ENDPOINT}?{query}"
    try:
        throttle.wait()
        with urlopen(request_url) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        print(f"Failed to query CDX for {url}: {exc}")
        return []
    if not data:
        return []
    headers = data[0]
    rows = []
    for row in data[1:]:
        entry = dict(zip(headers, row))
        rows.append(entry)
    return rows


def download_snapshot(url: str, destination: Path, throttle: "RateLimiter") -> tuple[str | None, bytes]:
    request = Request(url, headers={"User-Agent": "larue-civic-intel/1.0"})
    throttle.wait()
    with urlopen(request) as response:
        content_type = response.headers.get("Content-Type")
        payload = response.read()
        destination.write_bytes(payload)
    return content_type, payload


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def get_url_state(state: dict, url: str) -> dict:
    urls = state.setdefault("urls", {})
    url_state = urls.setdefault(
        url,
        {
            "last_processed": None,
            "last_hash": None,
            "last_snapshot_url": None,
            "seen_ids": [],
        },
    )
    url_state.setdefault("last_processed", None)
    url_state.setdefault("last_hash", None)
    url_state.setdefault("last_snapshot_url", None)
    url_state.setdefault("seen_ids", [])
    if not isinstance(url_state["seen_ids"], list):
        url_state["seen_ids"] = []
    return url_state


class RateLimiter:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self._last_call = 0.0

    def wait(self) -> None:
        if self.interval_seconds <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.interval_seconds:
            time.sleep(self.interval_seconds - elapsed)
        self._last_call = time.monotonic()


def run() -> int:
    parser = argparse.ArgumentParser(description="Wayback Machine historical backfill collector.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    config = read_config(args.config)
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
    include_subpaths = get_nested(
        config, "sources", "wayback", "include_subpaths", default=False
    )
    keywords = get_nested(
        config, "sources", "wayback", "high_impact_url_keywords", default=[]
    )
    if not isinstance(keywords, list):
        keywords = []

    if args.limit is not None:
        limit_per_run = args.limit

    out_dir = Path(out_dir_value)
    artifacts_dir = out_dir / "artifacts"
    snapshots_dir = out_dir / "snapshots" / "wayback"
    state_dir = out_dir / "state"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    enabled = get_nested(config, "sources", "wayback", "enabled", default=False)
    if not enabled:
        print("Wayback backfill disabled in config.")
        return 0

    state_path = state_dir / STATE_FILENAME
    state = load_state(state_path)

    if not isinstance(urls, list) or not urls:
        print("No Wayback URLs configured.")
        save_state(state_path, state)
        return 0

    total_captures = 0
    total_downloaded = 0
    total_skipped = 0
    total_changes = 0

    remaining = limit_per_run
    throttle = RateLimiter(rate_limit)

    for base_url in urls:
        if remaining <= 0:
            break
        if not isinstance(base_url, str) or not base_url.strip():
            continue

        match_type = "prefix" if include_subpaths else None
        query_url = base_url

        url_state = get_url_state(state, base_url)
        previous_hash = url_state.get("last_hash")
        previous_ts = url_state.get("last_processed")
        start = args.start
        if args.resume and not args.start:
            start = url_state.get("last_processed")
        captures = cdx_query(query_url, start, args.end, remaining, match_type, "desc", throttle)
        if not captures:
            continue

        change_checked = False
        for capture in captures:
            timestamp = capture.get("timestamp", "")
            original_url = capture.get("original", base_url)
            cdx_mimetype = capture.get("mimetype")
            if not timestamp or not original_url:
                continue
            total_captures += 1
            artifact_id = stable_id(original_url, timestamp)
            if artifact_id in url_state["seen_ids"]:
                total_skipped += 1
                continue

            snapshot_url = archived_url(original_url, timestamp)
            snapshot_ext = ".html"
            snapshot_path = snapshots_dir / f"{artifact_id}{snapshot_ext}"
            try:
                content_type, payload = download_snapshot(snapshot_url, snapshot_path, throttle)
                resolved_content_type = content_type or cdx_mimetype or "text/html"
                snapshot_ext = determine_extension(resolved_content_type, original_url)
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
                "content_type": resolved_content_type.split(";")[0]
                if resolved_content_type
                else "text/html",
                "tags": derive_tags(original_url, keywords, include_change=False),
            }
            artifact_path = artifacts_dir / f"{artifact_id}.json"
            artifact_path.write_text(
                json.dumps(artifact, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

            content_hash = hash_bytes(payload)
            if not change_checked:
                if previous_hash and previous_hash != content_hash and previous_ts:
                    change_artifact_id = change_id(original_url, timestamp)
                    if change_artifact_id not in url_state["seen_ids"]:
                        change_artifact = {
                            "id": change_artifact_id,
                            "source": {
                                "kind": "wayback",
                                "value": snapshot_url,
                                "retrieved_at": retrieved_at,
                            },
                            "title": f"Wayback change detected: {original_url}",
                            "body_text": (
                                f"{previous_ts} -> {timestamp}\n"
                                f"previous: {archived_url(original_url, previous_ts)}\n"
                                f"current: {snapshot_url}"
                            ),
                            "content_type": "text/plain",
                            "tags": derive_tags(original_url, keywords, include_change=True),
                        }
                        change_path = artifacts_dir / f"{change_artifact_id}.json"
                        change_path.write_text(
                            json.dumps(change_artifact, indent=2, sort_keys=True) + "\n",
                            encoding="utf-8",
                        )
                        url_state["seen_ids"].append(change_artifact_id)
                        total_changes += 1

            url_state["seen_ids"].append(artifact_id)
            if not change_checked:
                url_state["last_processed"] = timestamp
                url_state["last_hash"] = content_hash
                url_state["last_snapshot_url"] = snapshot_url
                change_checked = True
            total_downloaded += 1
            remaining -= 1
            if remaining <= 0:
                break

    save_state(state_path, state)

    if total_captures == 0:
        print("No Wayback captures found in this run.")

    print(
        f"Wayback summary: urls={len(urls)} captures={total_captures} "
        f"downloaded={total_downloaded} skipped={total_skipped} changes={total_changes}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except Exception as exc:
        print(f"Wayback backfill failed: {exc}")
        raise SystemExit(1)
