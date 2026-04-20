# credit reports

Generates per-study CSVs tracking participant progress and SONA credits.

- `credit_report.py` — 10-day mindfulness intervention (two studies: mental & breath)
- `credit_report_gfactor.py` — single-session g-factor study (survey + 4 tasks)

## Setup

From the repo root:

```bash
pip install -r requirements.txt
cp .env.example .env     # then fill in your Sussex SSH credentials
```

## Usage

```bash
python credit_report/credit_report.py
python credit_report/credit_report_gfactor.py
```

Each script downloads fresh JSONs from the Sussex server and writes its CSV
next to the script.

### Flags (both scripts)

| Flag | Purpose |
|---|---|
| `--since YYYY-MM-DD` | Only download files modified on/after this date. |
| `--no-download` | Skip SSH, use existing local JSONs. |
| `--json-dir PATH` | Override the default download folder. |
| `--out-dir PATH` | Override where the CSV is written. |

Repeat runs automatically skip files that are already downloaded and current.

## Mindfulness report output

`credit_report_mental.csv` and `credit_report_breath.csv`, one row per participant,
sorted by start date (earliest first):

- `email`, `condition` (`mental` / `world` / `breath` / `control`)
- `current_participant` (bool — started, not finished, not dropped out)
- `finished` (bool — completed post-test)
- `dropout` (bool — 8+ days inactive, didn't finish)
- `credits_earned` (2 for pre, 1 per meditation day, 2 for post; max 14)
- `days_since_last_activity`
- `pre`, `day_1` … `day_10`, `post` — completion date or blank

Waitlist controls who complete post-test receive the full 14 credits.

## G-factor report output

`credit_report_gfactor.csv`, one row per participant, sorted by most recent activity:

- `id` — SONA participant ID (from the filename)
- `credits_earned` (1 per file received + 1 bonus for completing all 5; max 6)
- `complete` (bool — all 5 stages present)
- `stages_done` (0–5)
- `first_activity`, `last_activity`
- `missing_stages` — comma-separated list of stages not yet submitted

Activity times use the server file mtimes (the JSONs themselves don't contain timestamps).