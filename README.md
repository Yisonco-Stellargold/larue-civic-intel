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

Enable the Wayback source in `config.toml` to backfill archived snapshots and detect quiet edits.
Run in batches to respect the Internet Archive:

- `python workers/collectors/wayback_backfill.py --config ./config.toml --limit 200`

State is stored in `out/state/wayback_state.json` with per-URL `last_processed`, `last_hash`, and
bounded `seen_ids`. Use `--resume` (default) to continue from the last processed timestamp, or pass
`--start`/`--end` to override the time window. Keep `rate_limit_seconds` conservative.

When a new snapshot hash differs from the last run, the collector emits a deterministic change
artifact titled \"Wayback change detected: <url>\" so edits are preserved without diffing.

## Text Extraction & Normalization (Stage 1)

The Stage 1 text extraction worker populates `body_text` in Artifact JSONs using deterministic
HTML/plain-text normalization.

Run the parser directly with:

- `python workers/parsers/extract_text.py --config ./config.toml --artifacts <out_dir>/artifacts`

The parser reads `storage.out_dir` from the config to locate snapshots under
`<out_dir>/snapshots/**` and updates Artifact JSON files in-place.

## Stage 2: extract-text integration

The CLI wires the text extraction worker into the weekly pipeline. PDF extraction may be stubbed
depending on optional dependencies.

- `cargo run -p cli -- extract-text --config ./config.toml`
- `cargo run -p cli -- run-weekly --config ./config.toml`

## Roadmap

Planned interfaces (placeholders only; no live integrations yet):

- `larue digest-weekly` will generate an AI-assisted weekly digest once the AI integration is
  enabled.
- `larue publish` will publish artifacts to a chosen backend (static/Web3) once publishing is
  implemented.
- Future config stubs live under `[ai]` and `[publish]` in `config.toml`.
