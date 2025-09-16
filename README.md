📊 Smart Retail Insights with Weather Integration

This project implements a data engineering pipeline that integrates retail sales with live weather data, validates data quality, and loads into PostgreSQL for analytics.

🚀 Features

Data ingestion: Load daily retail sales CSVs.

Data cleaning: Handle missing, duplicate, inconsistent values.

Dimensional schema: Star schema with fact_sales + dim_store, dim_customer, dim_product, dim_weather.

Weather enrichment: Fetch live weather for each store using OpenWeatherMap API
.

ETL pipeline: Load enriched sales and weather into PostgreSQL.

Audit logging: Track ingestion counts and rejected rows in etl_audit.

Analytics-ready: Run queries and visualize insights (e.g., sales vs temperature).

By Anshul Bhilare
