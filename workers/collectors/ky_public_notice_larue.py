#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.kentuckypublicnotice.com"
SEARCH_URL = f"{BASE_URL}/search/"
DEFAULT_QUERY = "LaRue County"
TAGS = ["public_notice", "larue", "ky"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def normalize_text(value: str | None) -> str | None:
    if not value:
        return None
    text = " ".join(value.split())
    return text if text else None


def fetch_search_html(query: str) -> str:
    headers = {"User-Agent": "larue-civic-intel/0.1 (+https://github.com/)"}
    params = {"q": query}
    response = requests.get(SEARCH_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def extract_rows(soup: BeautifulSoup) -> list:
    selectors = [
        "div.search-results article",
        "div.search-results .result",
        "li.search-result",
        "article.notice",
        "div.notice",
    ]
    for selector in selectors:
        rows = soup.select(selector)
        if rows:
            return rows
    return []


def extract_date_text(row) -> str | None:
    time_tag = row.find("time")
    if time_tag:
        return normalize_text(time_tag.get("datetime") or time_tag.get_text())
    for tag in row.find_all(["span", "div"]):
        classes = " ".join(tag.get("class", []))
        if "date" in classes or "publish" in classes:
            text = normalize_text(tag.get_text())
            if text:
                return text
    return None


def extract_notice_from_row(row) -> dict | None:
    link = row.find("a", href=True)
    if not link:
        return None
    url = urljoin(BASE_URL, link["href"])
    title = normalize_text(link.get_text()) or normalize_text(
        row.find(["h1", "h2", "h3"]).get_text() if row.find(["h1", "h2", "h3"]) else None
    )
    snippet = None
    paragraph = row.find("p")
    if paragraph:
        snippet = normalize_text(paragraph.get_text())
    return {
        "url": url,
        "title": title,
        "snippet": snippet,
        "publish_date": extract_date_text(row),
        "snapshot_html": str(row),
    }


def fallback_notice_links(soup: BeautifulSoup) -> Iterable[dict]:
    seen = set()
    for link in soup.select("a[href]"):
        href = link["href"]
        if "notice" not in href.lower():
            continue
        url = urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        yield {
            "url": url,
            "title": normalize_text(link.get_text()),
            "snippet": None,
            "publish_date": None,
            "snapshot_html": None,
        }


def write_outputs(notice: dict, artifacts_dir: Path, snapshots_dir: Path) -> None:
    url = notice["url"]
    if not url:
        return
    artifact_id = stable_id(url)
    artifact = {
        "id": artifact_id,
        "source": {
            "kind": "url",
            "value": url,
            "retrieved_at": utc_now_iso(),
        },
        "title": notice.get("title"),
        "body_text": notice.get("snippet"),
        "content_type": "text/html",
        "tags": TAGS,
    }

    artifact_path = artifacts_dir / f"{artifact_id}.json"
    with artifact_path.open("w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2, ensure_ascii=False)

    snapshot_html = notice.get("snapshot_html")
    if snapshot_html:
        snapshot_path = snapshots_dir / f"{artifact_id}.html"
        snapshot_path.write_text(snapshot_html, encoding="utf-8")


def main() -> None:
    out_root = Path("out")
    artifacts_dir = out_root / "artifacts"
    snapshots_dir = out_root / "snapshots"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    try:
        html = fetch_search_html(DEFAULT_QUERY)
    except Exception as exc:
        print(f"Failed to fetch search results: {exc}")
        return

    soup = BeautifulSoup(html, "html.parser")
    rows = extract_rows(soup)
    notices: list[dict] = []

    if rows:
        for row in rows:
            try:
                notice = extract_notice_from_row(row)
                if notice:
                    notices.append(notice)
            except Exception as exc:
                print(f"Skipping notice row due to error: {exc}")
    else:
        for notice in fallback_notice_links(soup):
            notices.append(notice)

    if not notices:
        print("No notices found in the search results.")
        return

    for notice in notices:
        try:
            write_outputs(notice, artifacts_dir, snapshots_dir)
            print(f"Wrote artifact for {notice.get('url')}")
        except Exception as exc:
            print(f"Failed to write notice {notice.get('url')}: {exc}")


if __name__ == "__main__":
    main()
