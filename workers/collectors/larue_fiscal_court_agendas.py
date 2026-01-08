import argparse
import hashlib
import json
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen
import tomllib

DEFAULT_OUT_DIR = "out"
DEFAULT_BASE_URL = "https://www.laruecounty.org/fiscal-court"
DEFAULT_TAGS = ["meeting", "fiscal_court", "larue", "ky"]


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for key, value in attrs:
            if key == "href" and value:
                self.links.append(value)


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


def fetch_html(url: str) -> str:
    with urlopen(url) as response:
        return response.read().decode("utf-8", errors="replace")


def discover_document_links(base_url: str) -> list[str]:
    html = fetch_html(base_url)
    parser = LinkExtractor()
    parser.feed(html)
    urls: list[str] = []
    for href in parser.links:
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if not absolute.startswith("http"):
            continue
        lower = absolute.lower()
        if any(keyword in lower for keyword in ["agenda", "minutes"]) and lower.endswith(
            (".pdf", ".html", ".htm")
        ):
            urls.append(absolute)
    return sorted(set(urls))


def stable_id(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return f"larue_fiscal_court:{digest}"


def snapshot_filename(url: str) -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix
    if not suffix:
        suffix = ".pdf"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return f"larue_fiscal_court_{digest}{suffix}"


def download_snapshot(url: str, destination: Path) -> None:
    with urlopen(url) as response:
        destination.write_bytes(response.read())


def write_artifact(
    artifacts_dir: Path,
    snapshots_dir: Path,
    url: str,
    tags: list[str],
) -> None:
    artifact_id = stable_id(url)
    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    snapshot_name = snapshot_filename(url)
    snapshot_path = snapshots_dir / snapshot_name
    artifact_filename = f"{snapshot_path.stem}.json"
    artifact_path = artifacts_dir / artifact_filename

    if artifact_path.exists() and snapshot_path.exists():
        return

    if not snapshot_path.exists():
        download_snapshot(url, snapshot_path)

    artifact = {
        "id": artifact_id,
        "source": {
            "kind": "url",
            "value": url,
            "retrieved_at": retrieved_at,
        },
        "title": None,
        "body_text": None,
        "content_type": snapshot_path.suffix.lstrip("."),
        "tags": tags,
    }
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect LaRue County Fiscal Court agendas/minutes."
    )
    parser.add_argument("--config", type=Path, help="Path to a config TOML file.")
    args = parser.parse_args()

    config = {}
    if args.config:
        config = read_config(args.config)

    enabled = get_nested(
        config, "sources", "larue_fiscal_court", "enabled", default=False
    )
    if not enabled:
        return

    out_dir_value = get_nested(config, "storage", "out_dir", default=DEFAULT_OUT_DIR)
    base_url = get_nested(
        config,
        "sources",
        "larue_fiscal_court",
        "base_url",
        default=DEFAULT_BASE_URL,
    )
    tags = DEFAULT_TAGS

    out_dir = Path(out_dir_value)
    artifacts_dir = out_dir / "artifacts"
    snapshots_dir = out_dir / "snapshots"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    try:
        urls = discover_document_links(base_url)
    except Exception as exc:
        print(f"Failed to discover fiscal court documents: {exc}")
        return

    for url in urls:
        try:
            write_artifact(artifacts_dir, snapshots_dir, url, tags)
        except Exception as exc:
            print(f"Failed to download {url}: {exc}")


if __name__ == "__main__":
    main()
