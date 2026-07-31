"""
Multi-source data integration pipeline.

Pulls:
  1. Daily weather data (Open-Meteo API, no key required)
  2. Daily USD/INR exchange rate (Frankfurter API, no key required)

Merges both on date into a single dataset, stores it in SQLite,
and exports a CSV for downstream use (e.g. Tableau).

Run manually:   python pipeline.py
Run scheduled:  via .github/workflows/refresh.yml (GitHub Actions cron)
"""

import sqlite3
import sys
from datetime import date, timedelta

import pandas as pd
import requests

import time

def fetch_with_retry(url, params=None, retries=3, backoff=5, timeout=20):
    """GET request with retries on transient network/server errors."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(backoff * attempt)

# ---- Config -----------------------------------------------------
LATITUDE = 29.92          # Sri Ganganagar, Rajasthan
LONGITUDE = 73.88
LOOKBACK_DAYS = 30         # how many past days of data to pull each run
DB_PATH = "data/pipeline.db"
CSV_PATH = "data/merged_data.csv"
TABLE_NAME = "daily_metrics"

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
FX_URL = "https://api.frankfurter.dev/v1"


def fetch_weather() -> pd.DataFrame:
    """Fetch daily max/min temperature and precipitation for the lookback window."""
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Kolkata",
        "past_days": LOOKBACK_DAYS,
        "forecast_days": 1,
    }
    resp = fetch_with_retry(WEATHER_URL, params=params)
    daily = resp.json()["daily"]

    df = pd.DataFrame({
        "date": daily["time"],
        "temp_max_c": daily["temperature_2m_max"],
        "temp_min_c": daily["temperature_2m_min"],
        "precipitation_mm": daily["precipitation_sum"],
    })
    return df


def fetch_exchange_rates() -> pd.DataFrame:
    """Fetch daily USD -> INR exchange rate for the same lookback window."""
    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    url = f"{FX_URL}/{start.isoformat()}..{end.isoformat()}
    resp = fetch_with_retry(url, params={"from": "USD", "to": "INR"})
    rates = resp.json()["rates"]

    records = [{"date": d, "usd_inr_rate": v["INR"]} for d, v in rates.items()]
    df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
    return df


def merge_sources(weather_df: pd.DataFrame, fx_df: pd.DataFrame) -> pd.DataFrame:
    """Integrate the two sources into a single dataset keyed on date."""
    merged = pd.merge(weather_df, fx_df, on="date", how="inner")
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


def load_to_sqlite(df: pd.DataFrame, db_path: str, table: str) -> None:
    """Upsert-style load: replace the table with the latest merged dataset."""
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table, conn, if_exists="replace", index=False)
    finally:
        conn.close()


def run() -> pd.DataFrame:
    print("Fetching weather data...")
    weather_df = fetch_weather()
    print(f"  -> {len(weather_df)} weather rows")

    print("Fetching exchange rate data...")
    fx_df = fetch_exchange_rates()
    print(f"  -> {len(fx_df)} exchange rate rows")

    print("Merging sources on date...")
    merged = merge_sources(weather_df, fx_df)
    print(f"  -> {len(merged)} merged rows")

    import os
    os.makedirs("data", exist_ok=True)

    load_to_sqlite(merged, DB_PATH, TABLE_NAME)
    merged.to_csv(CSV_PATH, index=False)
    print(f"Saved SQLite DB -> {DB_PATH}")
    print(f"Saved CSV       -> {CSV_PATH}")

    return merged


if __name__ == "__main__":
    try:
        run()
    except requests.exceptions.RequestException as e:
        print(f"Pipeline failed due to a network/API error: {e}", file=sys.stderr)
        sys.exit(1)
