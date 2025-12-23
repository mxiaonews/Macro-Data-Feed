# Macro Context Pack (Stage 1) — Auto Feed Starter Kit

Generate a daily “Macro Context Pack” you can paste into your Investing Project chats.

## What it does
- Pulls major macro/markets/credit series from **FRED**
- Optionally adds a **news/trend pulse** using **GDELT DOC API**
- Writes outputs to `./outputs/` as:
  - `macro_context_YYYY-MM-DD.md`
  - `macro_context_YYYY-MM-DD.json`

## Setup
1) Get a FRED API key
2) Set environment variable:
   - `FRED_API_KEY`

## Install
```bash
pip install -r requirements.txt
```

## Run once
```bash
python macro_daily.py
```

## Schedule daily (cron example)
Run at 7:30am local time:
```cron
30 7 * * * /usr/bin/env bash -lc 'cd /path/to/macro_feed_pack && /path/to/python macro_daily.py'
```
