# LaRue Civic Intelligence

An open-source civic intelligence system for LaRue County, Kentucky.

This project collects publicly available civic information (meetings, notices,
elections, and filings), normalizes it into structured records, and produces
auditable summaries and reports for public awareness.

Key principles:
- Neutral and factual data collection
- Transparent scoring methodologies
- Human-auditable memory via Markdown (Obsidian-compatible)
- Designed for low-power hardware (Raspberry Pi)

This repository contains infrastructure only.  
Interpretive frameworks are versioned, documented, and reproducible.

## Local collector run

Install Python requirements:

```bash
pip install -r workers/requirements.txt
```

Run the Kentucky public notice collector for LaRue County:

```bash
python workers/collectors/ky_public_notice_larue.py
```

Artifacts will be written to `out/artifacts/` with HTML snapshots in `out/snapshots/`.

## Ingest artifacts from a directory

```bash
cargo run -p cli -- ingest-dir out/artifacts --db civic.db
```

This ingests all `*.json` files in the directory using the canonical Artifact schema.

## Build the Obsidian vault

```bash
cargo run -p cli -- build-vault --db civic.db --vault vault
```

## Weekly pipeline

For a Raspberry Pi or other always-on host, a weekly pipeline can be managed via
systemd (see `ops/systemd/`). Configure the service `WorkingDirectory` to your
repo checkout, then enable the timer:

```bash
sudo systemctl enable --now larue-civic-intel.timer
```

The timer runs the collector, batch ingestion, and vault build each Monday at
02:00 local time.
