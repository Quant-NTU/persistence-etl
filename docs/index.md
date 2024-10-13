# ETL Introduction
## Background
This ETL (Extract, Transform, Load) micro-service was introduced, to aid in collection of various forms of data such as historical price and historical news data, processing and storing them in our platform.

This will improve data availability for researchers to perform experiments on, bringing great value to their analysis.

## Roadmap
For this iteration, our roadmap will focus on collection of data in the following 3 key areas:

### Historical News Data
- We will be using historical news data from BBC News, which has been made available via the Hugging Face API
- Link: https://huggingface.co/datasets/RealTimeData/bbc_news_alltime

### Historical Price Data for Stocks [TODO]
- We will be using historical stock price data from Nasdaq and Yahoo Finance API.

### Historical Price Data for Crypto [TODO]
- We will be using historical price data from CryptoCompare.

The ETL service will be developed, bearing in mind maintainability and integration of new data sources for future iterations of the project.

## Architecture
We have defined the following architecture, denoting the flow of data, processes and interactions that will take place in this micro-service. 


The `persistence-middleware` microservice interacts with this `persistence-etl` microservice, to gain access to the transformed data such as historical news and historical price,
and provides it to the front-facing user such as researchers who only have access to the `persistence-middleware` application.
This serves as the only route for these users to gain access to this data as they do not have access to the ETL service.

![Overview](./images/DataCollectionOverview.png)
