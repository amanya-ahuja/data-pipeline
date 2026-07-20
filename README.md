# Automated Multi-Source Data Pipeline

An automated pipeline that integrates two independent public data sources —
daily weather (Open-Meteo) and daily USD/INR exchange rates (Frankfurter) —
into a single merged dataset, refreshed automatically on a schedule with no
manual intervention.

## What it does

1. **Extract** — pulls the last 30 days of weather data and USD/INR exchange
   rates from two separate free public APIs.
2. **Transform** — merges both sources on date into one unified table.
3. **Load** — writes the merged dataset to a local SQLite database and
   exports a CSV for downstream analysis / visualization.
4. **Automate** — a GitHub Actions workflow (`.github/workflows/refresh.yml`)
   runs the pipeline daily via cron, and commits the refreshed data back to
   the repo — so the dataset stays current with zero manual steps.

## Stack

- Python, Pandas, Requests
- SQLite (storage)
- GitHub Actions (scheduled automation / CI)
- Output CSV designed for direct import into Tableau Public

## Run locally

```bash
pip install -r requirements.txt
python pipeline.py
```

Outputs land in `data/pipeline.db` and `data/merged_data.csv`.

## Why this exists

Built to demonstrate an end-to-end data integration + automation workflow:
combining disparate data sources programmatically and keeping the dataset
fresh on a schedule without manual re-runs — the same pattern used in
production data engineering pipelines, just at a small scale.

## Next step

`data/merged_data.csv` is loaded into Tableau Public to visualize trends
across temperature, precipitation, and exchange rate over time.
