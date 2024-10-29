# TimescaleDB Setup and Initialization

This document provides instructions on how to run TimescaleDB using Docker Compose and initialize the necessary tables for the Quant AI project. This setup includes TimescaleDB service to support data storage for raw and transformed crypto data. ( many more in the future )

## Starting the Microservice

You can start the TimescaleDB microservice using one of the following methods:

### 1. Visual Studio GUI Method

1. Open Visual Studio and locate the `docker-compose.yml` file in your project’s root directory.
2. Right-click on `docker-compose.yml`.
3. Select **Compose Up - Selected Services**.
4. Choose **quant-ai-timescale-db** from the list to start only the TimescaleDB service.

### 2. Command-Line Method

Navigate to the directory containing `docker-compose.yml`, and run the following command to start TimescaleDB:

```bash
docker-compose up -d quant-ai-timescale-db
```
This will initialize and run only the TimescaleDB service in detached mode.

### Docker Compose Configuration

The TimescaleDB service is defined in the `docker-compose.yml` file alongside MongoDB and other dependent services. Below is an overview of the relevant sections:

```yaml
quant-ai-timescale-db:
  container_name: quant-ai-timescale-db
  image: timescale/timescaledb:latest-pg16
  environment:
    - POSTGRES_PASSWORD=zxcasdqwe123
    - POSTGRES_DB=quant-ai
  ports: [ 10003:5432 ]
  volumes: [ ../data/timescale-data:/var/lib/postgresql/data ]
  networks: [ quant-ai-timescale-db ]
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U postgres"]
    interval: 10s
    timeout: 5s
    retries: 5
```

## Initializing Tables in TimescaleDB

The tables in TimescaleDB are created using a Kotlin service named `TimescaleDBTableInitializer`. This script ensures that the required tables exist upon application startup.

### Tables

- **raw_crypto_data**: Stores raw crypto price data fetched from external APIs.
- **transformed_crypto_data**: Stores aggregated or transformed crypto data.

### Table Schema Definitions

#### `raw_crypto_data`
- `id`: Serial primary key
- `symbol`: Symbol of the cryptocurrency (e.g., BTC, ETH)
- `source`: Data source (e.g., CoinMarketCap)
- `price`: Price of the cryptocurrency at a specific timestamp
- `timestamp`: Date and time when the data was fetched

```sql
CREATE TABLE IF NOT EXISTS raw_crypto_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    source VARCHAR(50) NOT NULL,
    price DECIMAL NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);
```

#### `transformed_crypto_data` Table

- **id**: Serial primary key
- **symbol**: Symbol of the cryptocurrency
- **average_price**: Calculated average price
- **timestamp**: Date and time when the transformation was applied

```sql
CREATE TABLE IF NOT EXISTS transformed_crypto_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    average_price DECIMAL NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);
```

## Verifying Table Initialization

To verify the tables have been initialized:

1. **Access the TimescaleDB container:**
   ```bash
   docker exec -it quant-ai-timescale-db psql -U postgres -d quant-ai
   ```
2. **List tables** to confirm `raw_crypto_data` and `transformed_crypto_data` exist:
   ```sql
   \dt
   ```

## Healthcheck and Dependencies

The TimescaleDB service includes a health check, ensuring it is ready before other dependent services start. Both MongoDB and TimescaleDB must pass their health checks before data persistence tasks can proceed.

## Troubleshooting

If tables do not appear after running the initializer, ensure:

- The `quant-ai-timescale-db` service is healthy by checking logs:
  ```bash
  docker logs quant-ai-timescale-db
- No database connection issues are present in the `quant-ai-persistence-etl` logs.

## Conclusion

This completes the TimescaleDB setup and table initialization for the Quant AI project. TimescaleDB is now ready to store and manage both raw and transformed crypto data.
