## zscrape

Compact suite to ingest eBay listing pages, parse structured data, compare Title/Specifics/Table/Metadata, and produce logs/reports. Optional tools include watchdog, specs fetcher, and training utilities.

### Requirements
- **Windows 10/11** (Win32 APIs and AutoHotkey v2 workflow)
- **Python 3.9+** with pip
- **AutoHotkey v2** (for the capture/automation workflow)
- **eBay seller account** (signed in to Seller Hub during capture)
- Python deps: `pip install -r requirements.txt`

Optional (only if you use related features):
- Tesseract OCR (for `pytesseract`-based flows)

### Setup
1) Clone this repo.
2) (Recommended) Create a virtualenv.
3) Install deps:
```
python -m pip install -r requirements.txt
```
4) Install AutoHotkey v2 (system‑wide) for end‑to‑end capture.
5) Sign in to your eBay seller account in the browser you use for capture.

### Quick start
- Parsing/Review (no capture):
  - Place listing dumps in `item_contents/` (e.g., `123456789012.html`, `_description_html.txt`, `.txt`).
  - Launch the UI: `python runit.py`.

- End‑to‑end with capture/monitor:
  - Start your AHK v2 capture workflow to populate `item_contents/`.
  - Run the monitor/processor: `python scan_monitor.py`.
  - The monitor can (re)launch `tools/security/watchdog.py` and process new items continuously.

### How it works (pipeline)
- Capture (external/AHK) or manual export writes raw item HTML/text into `item_contents/`.
- `process_description.py` parses sections (Title, Specifics, Table, Description), stops at disclaimers, and normalizes fields.
- `comparisons/` checks consistency between Title, Specifics, Table, and Metadata.
- `runit.py` provides a Tk UI to inspect items, run comparisons, and view helpers.
- `scan_monitor.py` watches for new items, orchestrates processing, and manages a security watchdog.
- Titles are appended to `tools/titles_extracted.txt` to grow a training corpus.

### Outputs
- Logs: `logs/processing/` (per‑item process logs, pull logs, main log)
- Reports: `reports/` (roll‑ups/weekly summaries)
- State and caches: `state/`

### Tools included (high level)
- `tools/security/watchdog.py`: Keeps capture/automation healthy (Windows).
- `tools/specs/`: Specs fetcher and small GUI; configure credentials via env (avoid committing secrets).
- `tools/training/`: LLM‑assisted validators, viewers, and utilities (optional; requires PyYAML, etc.).
- `tools/ai_agents/`: Developer utilities for automated edits/analysis (optional).

### Configuration notes
- Core parsing/extraction settings live in `configs/` and `description_extraction/`.
- Data/layout directories are created relative to the repo root: `item_contents/`, `logs/`, `reports/`, `state/`.


FOR SCREENSHOT CAPTURING SUPPORT, DROP GDIP_ALL.AHK INTO /LIB.

### If you use another relay or want the messages for issues to be sent to something other than Mattermost, change the following lines:
- Replace `testmattermostmsg.py` with your script path (and adjust arguments if your script expects different CLI parameters).
- Edit these locations:
  - `runit.py`: L268–L269
  - `runit.py`: L7163–L7165
  - `scan_monitor.py`: L2604
  - `zscrape_process_new_auto_shutdown_at_350pm_new.ahk`: L522
  - `zscrape_process_new_auto_shutdown_at_350pm_new.ahk`: L1147
  - `zscrape_process_new_auto_shutdown_at_350pm_new.ahk`: L1198
