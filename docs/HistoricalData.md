# Historical Data — ETL Guide

## Overview

The ETL service fetches historical stock data via the Twelve Data API and stores it in TimescaleDB. Historical coverage has been extended from 365 days to **14 years (2010-01-01 to present)**.

---

## How It Works

On startup (via `setup_etl_docker.bat`), the ETL service:

1. Calls `POST /stock/historical/store-top` — fetches 14 years of daily OHLCV data for all top symbols (AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM, JNJ, V) using date-range queries
2. Calls `POST /stock/historical/store-by-date?symbol=SPY&startDate=2010-01-01&...` — fetches 14 years of SPY data separately
3. Runs transformation and loads to data warehouse

A 1-second delay is applied between symbol fetches to avoid Twelve Data API rate limiting.

---

## API Endpoints

### Fetch by date range (recommended for large history)
```
POST /stock/historical/store-by-date
  ?symbol=SPY
  &startDate=2010-01-01
  &endDate=2024-12-31
  &interval=1day
```

### Fetch by output size
```
POST /stock/historical/store
  ?symbol=SPY
  &limit=5000
  &interval=1day
```

### Fetch all top symbols (14 years)
```
POST /stock/historical/store-top
```

---

## Supported Intervals

`1min`, `5min`, `15min`, `1h`, `4h`, `1day`

---

## Notes

- The Twelve Data free tier supports up to 800 API credits/day — fetching all 10 symbols at 14 years may require a paid plan or multiple days of incremental fetching
- Data is inserted with `ON CONFLICT DO NOTHING` so re-running is safe and idempotent
- For the ML strategy service, data is fetched directly from yfinance (not this ETL) to ensure full 2010–present coverage regardless of ETL state
