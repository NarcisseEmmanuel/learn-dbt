-- SELECT * FROM  'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'LIMIT 10;

-- SELECT COUNT(*) FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';

-- SELECT VendorID, COUNT(*) AS trip_count
-- FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
-- GROUP BY VendorID;

-- SELECT store_and_fwd_flag, COUNT(*) AS trip_count
-- FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
-- GROUP BY store_and_fwd_flag;

-- SELECT payment_type, COUNT(*) AS trip_count
-- FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
-- GROUP BY payment_type;

-- SELECT PULocationID, COUNT(*) AS trip_count
-- FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
-- GROUP BY PULocationID
-- ORDER BY trip_count DESC;

-- SELECT DOLocationID, COUNT(*) AS trip_count
-- FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
-- GROUP BY DOLocationID
-- ORDER BY trip_count DESC;


SELECT COUNT(*) AS trip_count
FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
WHERE tpep_pickup_datetime > tpep_dropoff_datetime;

SELECT *
FROM "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
WHERE tpep_pickup_datetime > tpep_dropoff_datetime
LIMIT 10;
      