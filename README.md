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
- Python 3.11+ for collectors (tomllib-based config loading)

This repository contains infrastructure only.  
Interpretive frameworks are versioned, documented, and reproducible.

## Configuration

1. Copy the template config:
   - `cp config.example.toml config.toml`
2. Edit `config.toml` for your environment.
3. Run commands with the config path, for example:
   - `larue build-vault --config ./config.toml`
   - `python workers/collectors/ky_public_notice_larue.py --config ./config.toml`

## Weekly pipeline

Run the full weekly pipeline (collector -> ingest-dir -> build-vault) with:

- `larue run-weekly --config ./config.toml`

Generate a weekly report note and JSON summary with:

- `larue report-weekly --config ./config.toml`

## Historical backfill (Wayback Machine)

To backfill archived snapshots, enable the Wayback source in `config.toml` and run the collector
in batches to respect rate limits:

- `python workers/collectors/wayback_backfill.py --config ./config.toml --limit 200`

State is stored in `out/state/wayback_state.json` and records the last processed timestamp per
configured URL plus a bounded set of seen capture IDs. Use `--resume` (default) to continue from
the last processed timestamp, or pass `--start`/`--end` to override the time window. Keep the
`rate_limit_seconds` setting in config to be a respectful client of the Internet Archive.
