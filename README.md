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

## Configuration

1. Copy the template config:
   - `cp config.example.toml config.toml`
2. Edit `config.toml` for your environment.
3. Run commands with the config path, for example:
   - `larue build-vault --config ./config.toml`
   - `python workers/collectors/ky_public_notice_larue.py --config ./config.toml`
