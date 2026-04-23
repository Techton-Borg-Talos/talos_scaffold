# Standalone Dialpad History Tools

This folder is a standalone Dialpad history toolset that runs outside the
worker container from a normal Windows virtualenv.

It contains:

- `dialpad_historical_backfill.py`
- `dialpad_activity_catchup_email.py`
- `dialpad_history_common.py`

## Default Paths

- Archive root: `Z:\Historical\Dialpad`
- Contacts CSV: `Z:\Historical\Contacts\2026_0408_master_contacts_clean.csv`

Both defaults can be changed in `.env`.

## Setup

```powershell
cd D:\Talos\Data_Ingestion\Scripts\Dialpad_History
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

## Historical Backfill

This writes directly into `DIALPAD_ARCHIVE_ROOT` and archives each chunk as it
completes.

Example:

```powershell
python .\dialpad_historical_backfill.py --start-date 2026-04-01 --end-date 2026-04-22 --chunk-days 3 --progress-every 5 --types calls --target-type user --target-id 4703901367410688
```

## Daily Review Email

This reads archived Dialpad files from disk and sends a plain-text,
human-readable report. It is meant for reviewing the previous day or another
selected window, with full SMS and transcript text inline when available.

Dry run:

```powershell
python .\dialpad_activity_catchup_email.py --start 2026-04-09 --end 2026-04-23 --dry-run
```

Send:

```powershell
python .\dialpad_activity_catchup_email.py --start 2026-04-09 --end 2026-04-23
```

## Notes

- `--contacts-csv` overrides `DIALPAD_CONTACTS_CSV`.
- `--archive-root` overrides `DIALPAD_ARCHIVE_ROOT`.
- The email script prefers `DIALPAD_CONTACTS_CSV` before trying any CSV beside
  the script.
