# Persistence ETL Service

A comprehensive ETL (Extract, Transform, Load) microservice for collecting, processing, and storing time-series data from multiple financial data sources.

## Features

### Data Collection
- **Historical Stock Data** - Real-time and historical stock prices from TwelveData API
- **Historical Forex Data** - Foreign exchange currency pairs from TwelveData API
- **Historical Crypto Data** - Cryptocurrency market data from CryptoCompare API
- **Historical News Data** - News articles from BBC via Hugging Face API

### Data Warehouse
A star schema data warehouse that consolidates time-series data for analytical queries:
- **Unified fact table** for OHLC data across all asset types
- **Dimension tables** for symbols, exchanges, time intervals, and asset types
- **Materialized views** for optimized analytical queries
- **TimescaleDB continuous aggregates** with automatic refresh policies
- **Incremental ETL** with scheduled updates
- **REST API** for warehouse management and statistics

## Technology Stack

- **Kotlin** - Primary development language
- **Spring Boot** - Application framework
- **PostgreSQL + TimescaleDB** - Time-series database
- **JdbcTemplate** - Database access
- **Scheduled Tasks** - Automated data refresh

## API Endpoints

### Data Warehouse

- `POST /api/v1/warehouse/load/full` - Trigger a full warehouse load
- `POST /api/v1/warehouse/load/incremental` - Load recent data (last 2 hours)
- `POST /api/v1/warehouse/refresh-views` - Manually refresh materialized views
- `GET /api/v1/warehouse/statistics` - Get warehouse statistics

## Documentation

- [ETL Overview](docs/index.md)
- [SQL Schema Reference](docs/data-warehouse-schema.sql)
- [Query Examples](docs/data-warehouse-queries.sql)