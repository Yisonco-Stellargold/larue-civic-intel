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

## Meeting & Decision Extraction (Stage 4)

Meeting parsing is conservative and focused on LaRue County Fiscal Court artifacts with clear
structure. It relies on extracted text and issue tags, and may skip ambiguous documents to avoid
false positives.

- `python workers/parsers/parse_meeting_minutes.py --config ./config.toml --artifacts <out_dir>/artifacts`
- `cargo run -p cli -- ingest-decisions --config ./config.toml`
- `cargo run -p cli -- run-weekly --config ./config.toml`

## Rubric Scoring (Stage 5)

Weekly rubric scoring is deterministic and auditable. It uses the weights and bias controls under
`/rubric` and defaults to neutral scores with an “insufficient evidence” flag when rules do not
provide strong matches.

- Tune weights and thresholds in `rubric/weights.yaml`, `rubric/rubric_config.toml`, and
  `rubric/bias_controls.yaml`.
- Run scoring manually with:
  - `cargo run -p cli -- score-weekly --config ./config.toml`

## MVP Website Export (Stage 6)

The static site export is deterministic and based on rubric scoring and source records. Commentary
is opinion/satire derived from scores and includes explicit disclaimers; it is not a factual claim.

- Generate the site bundle:
  - `cargo run -p cli -- export-site --config ./config.toml`

TODO: Replace the template-based commentary generator with a future LLM provider via the existing
`[ai]` stubs.

## Free Weekly Automation (GitHub Actions + Pages)

This repository includes a weekly GitHub Actions workflow that runs the pipeline and publishes the
static site to GitHub Pages at `https://<org-or-user>.github.io/<repo>/`. Update the schedule in
`.github/workflows/weekly.yml` by editing the `cron` expression (UTC).

Local runs use `config.toml` (copied from `config.example.toml`). The workflow uses
`config/ci.toml`, which keeps output under `out/` and enables only safe collectors for CI.

Disclaimers:
- Scoring is rubric-based and conservative; insufficient evidence yields neutral scores.
- Commentary is opinion/satire tied to score changes and public records.
- Always link back to primary sources for auditability.

## Backfill + IPFS

This repo includes a manual GitHub Actions workflow that runs the Wayback backfill in chunks and pins
the resulting site to IPFS (Pinata) using `ipshipyard/ipfs-deploy-action`. To enable it, add
repository secrets:

- `PINATA_API_KEY`
- `PINATA_API_SECRET`

Run the workflow from the Actions tab using **Wayback Backfill + IPFS**, and adjust `limit` /
`rate_limit_seconds` to control chunk size and pacing. The first run will not restore prior state,
which is expected. The workflow prints the CID and writes `out/site/ipfs.json` with the CID metadata,
and you can access the pinned site via:
`https://ipfs.io/ipfs/<CID>/`.

## Roadmap

Planned interfaces (placeholders only; no live integrations yet):

- `larue digest-weekly` will generate an AI-assisted weekly digest once the AI integration is
  enabled.
- `larue publish` will publish artifacts to a chosen backend (static/Web3) once publishing is
  implemented.
- Future config stubs live under `[ai]` and `[publish]` in `config.toml`.
