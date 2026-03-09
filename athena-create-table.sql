CREATE EXTERNAL TABLE final_sales (
    id bigint,
    name string,
    amount double,
    event_date date
)
STORED AS PARQUET
LOCATION 's3://csv-final-data--bucket/final/';
