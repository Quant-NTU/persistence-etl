# Crypto Price Data - ETL Pipeline

This documentation outlines the functionalities, workflows, and structure of the ETL pipeline for cryptocurrency price data. The pipeline includes raw data fetching, transformation, and management, with TimescaleDB used for storage and MongoDB planned for future expansions involving unstructured data like news.

The pipeline supports both manual and scheduled operations and ensures deduplication and efficient data processing.

## Fetching and Storing Historical Data
### Controller Class: `CryptoController`

The `CryptoController` handles API endpoints for fetching historical cryptocurrency data, initiating data transformation, and retrieving top cryptocurrency symbols.

#### Endpoints

1. **Fetch and Store Historical Data**:
   - **Endpoint**: `POST /crypto/historical/store?symbol={symbol}&currency={currency}&limit={limit}`
   - **Description**: Fetches historical data for a specific cryptocurrency symbol in the specified currency and stores it in the `raw_crypto_compare_crypto_data` table.
   - **Example**:
     ```bash
     curl -X POST "http://localhost:8080/crypto/historical/store?symbol=BTC&currency=USD&limit=10"
     ```

2. **Fetch and Store Historical Data for Top Symbols**:
   - **Endpoint**: `POST /crypto/historical/store-top`
   - **Description**: Automatically fetches and stores historical data for the top 10 cryptocurrencies by volume.
   - **Example**:
     ```bash
     curl -X POST "http://localhost:8080/crypto/historical/store-top"
     ```

3. **Trigger Data Transformation**:
   - **Endpoint**: `POST /crypto/transform`
   - **Description**: Initiates the transformation of raw data into an aggregated format.
   - **Example**:
     ```bash
     curl -X POST "http://localhost:8080/crypto/transform"
     ```

4. **Retrieve Top Cryptocurrency Symbols**:
   - **Endpoint**: `GET /crypto/top-symbols`
   - **Description**: Fetches the top 10 cryptocurrency symbols by trading volume.
   - **Example**:
     ```bash
     curl -X GET "http://localhost:8080/crypto/top-symbols"
     ```

---

## Data Storage and Transformation

### TimescaleDB Tables

1. **`raw_crypto_compare_crypto_data`**:
   - Stores raw cryptocurrency data fetched from the CryptoCompare API.
   - Columns: `symbol`, `currency`, `open`, `high`, `low`, `close`, `volume_from`, `volume_to`, `timestamp`.

2. **`transformed_crypto_data`**:
   - Stores transformed data with aggregated metrics like average price and price change.
   - Additional Columns: `avg_price`, `price_change`.

### Table Initialization
The `TimescaleDBTableInitializer` ensures the tables are created if they don't already exist. This process is automated at application startup.

---

## Services

1. **`CryptoService`**:
   - Fetches cryptocurrency data from the CryptoCompare API.
   - Checks for duplicate data based on `symbol` and `timestamp`.
   - Inserts data into the `raw_crypto_compare_crypto_data` table.

2. **`CryptoTransformationService`**:
   - Transforms raw data and saves aggregated results into the `transformed_crypto_data` table.
   - Aggregations include metrics like daily average price and price change percentage.
   - **Duplicate Check**: Ensures that raw data is not transformed multiple times by checking if a record for the same symbol, currency, and timestamp (truncated to the day) already exists in the `transformed_crypto_data` table. Only new records are processed.

---

## Schedulers

1. **`CryptoRawDataScheduler`**:
   - Fetches and stores data for the top 10 cryptocurrencies daily at 12:05 AM UTC.
   - Cron Expression: `0 5 0 * * ?`.

2. **`CryptoTransformationScheduler`**:
   - Transforms raw data daily at 12:30 AM UTC.
   - Cron Expression: `0 30 0 * * ?`.

---

## Example Workflow

1. **Daily Scheduled Workflow**:
   - At 12:05 AM, `CryptoRawDataScheduler` fetches raw data for the top 10 cryptocurrencies.
   - At 12:30 AM, `CryptoTransformationScheduler` transforms the fetched data.

2. **Manual Workflow**:
   - A user can manually trigger data fetching or transformation using the respective endpoints.

---

## Exception Handling

Errors during data fetching, insertion, or transformation are logged and handled gracefully to ensure pipeline stability. Key exception scenarios include:
- API errors.
- Database connection issues.
- Duplicate data handling.

---

This documentation provides a comprehensive overview of the ETL pipeline for cryptocurrency price data, ensuring transparency and ease of use for future development and maintenance.
