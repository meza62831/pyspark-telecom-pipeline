# LATAM Telecom ETL Pipeline

PySpark ETL pipeline that extracts, transforms, and loads
telecom churn data into Google BigQuery for executive analytics.

## Stack
- Apache Spark / PySpark
- Google BigQuery
- Google Cloud SDK
- Python 3.11

## Pipeline
Extract (CSV) → Transform (PySpark) → Quality Checks → Load (BigQuery)

## Results
- Processed X,XXX records
- X data quality rules enforced
- Output: BigQuery table ready for Looker/dashboarding