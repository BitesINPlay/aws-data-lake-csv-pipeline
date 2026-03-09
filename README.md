# AWS Data Lake CSV Pipeline

## Architecture

S3 → Lambda → S3 → AWS Glue → S3 (Parquet) → Athena → QuickSight

## Services Used

- Amazon S3
- AWS Lambda
- AWS Glue (Crawler + ETL Job)
- AWS Athena
- Amazon QuickSight

## Pipeline Steps

1. CSV files uploaded to S3 raw bucket
2. Lambda preprocesses and cleans the data
3. Processed files stored in S3 processed bucket
4. Glue crawler detects schema
5. Glue ETL job transforms data to Parquet
6. Athena queries the final dataset
7. QuickSight visualizes the data
