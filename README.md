# Quant AI ETL Service

## Overview
This ETL (Extract, Transform, Load) service is designed to handle NASDAQ stock data processing, cryptocurrency data, and news articles. It downloads data from various sources, transforms it, and stores it in TimescaleDB and MongoDB for further analysis.

## Prerequisites
- Java 21
- PostgreSQL 16 (TimescaleDB)
- MongoDB
- Gradle
- OneDrive access (for NASDAQ data)

## Project Structure
```
persistence-etl/
├── src/
│   ├── main/
│   │   ├── kotlin/
│   │   │   └── sg/com/quantai/etl/
│   │   │       ├── controllers/    # REST endpoints
│   │   │       ├── services/       # Business logic
│   │   │       └── repositories/   # Data access
│   │   └── resources/
│   │       └── application.properties  # Configuration files
├── data/
│   ├── downloads/   # For downloaded ZIP files
│   └── extracted/   # For extracted CSV files
```

## Setup Instructions

### 1. Database Setup
#### PostgreSQL/TimescaleDB:
```bash
# Create database
psql -U postgres
CREATE DATABASE "quant-ai";
```

#### MongoDB:
- Ensure MongoDB is running on port 10000 (local) or 27017 (production)
- Database will be created automatically on first run

### 2. Configuration
1. Copy and configure application properties:
```bash
cp src/main/resources/application.properties src/main/resources/application-local.properties
```

2. Update the following in `application-local.properties`:
```properties
# PostgreSQL
spring.datasource.url=jdbc:postgresql://localhost:5432/quant-ai
spring.datasource.username=postgres
spring.datasource.password=your_password

# MongoDB
spring.data.mongodb.host=localhost
spring.data.mongodb.port=10000

# API Keys
quantai.external.api.cryptocompare.key=your_api_key
```

### 3. Build Project
```bash
./gradlew clean build
```

## Running the Application

### Using Gradle
```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

### Using dev-setup.sh Script
The project includes a dev-setup.sh script with various options:
```bash
# Show help
./dev-setup.sh -h

# Run in local mode
./dev-setup.sh -l

# Run with continuous build
./dev-setup.sh -c

# Run tests
./dev-setup.sh -t
```

## Available Endpoints

### NASDAQ Data
- `GET /nasdaq/data` - Retrieve transformed NASDAQ data
  - Parameters:
    - `symbol` (optional): Stock symbol
    - `startDate` (optional): Start date (YYYY-MM-DD)
    - `endDate` (optional): End date (YYYY-MM-DD)

### OneDrive Integration
- `POST /onedrive/fetch` - Manually trigger data fetch from OneDrive
  - Parameter: `oneDriveUrl` - URL to the data file

## Scheduled Tasks
The application includes several automated tasks:
- NASDAQ data fetch: Daily at 1:00 AM UTC
- Data transformation: Daily at 1:30 AM UTC

## Logging
Logs are available in the console and can be configured in `application.properties`:
```properties
logging.level.sg.com.quantai=INFO
```

## Troubleshooting

### Common Issues
1. Database Connection:
   - Verify PostgreSQL is running on the configured port
   - Check database credentials in application-local.properties
   - Ensure the database "quant-ai" exists

2. MongoDB Connection:
   - Verify MongoDB is running on the configured port
   - Check MongoDB connection string

3. OneDrive Access:
   - Verify OneDrive URL is accessible
   - Check file permissions

### Error Resolution
If you encounter errors:
1. Check application logs
2. Verify all prerequisites are installed
3. Ensure all required services (PostgreSQL, MongoDB) are running
4. Validate configuration in application-local.properties

## Contributing
1. Create a feature branch
2. Make your changes
3. Submit a pull request

## License
Proprietary - All rights reserved