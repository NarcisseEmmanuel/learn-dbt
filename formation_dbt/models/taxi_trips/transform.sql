
/* base de données : tlc_taxi_trips.raw_yellow_tripdata test
   modèle dbt : formation_dbt.taxi_trips.transform

   Ce modèle sélectionne un sous-ensemble de colonnes pertinentes
   à partir des données brutes des trajets en taxi pour une analyse ultérieure.

SELECT tpep_pickup_datetime,
       tpep_dropoff_datetime,
       passenger_count,
       trip_distance,
       payment_type,
       fare_amount,
       extra,
       mta_tax,
       tip_amount,
       tolls_amount,
       improvement_surcharge,
       total_amount
FROM {{ source('tlc_taxi_trips', 'raw_yellow_tripdata') }}
LIMIT 100   

*/
WITH sources_data AS (
    SELECT * EXCLUDE(VendorID, RatecodeID)
    FROM {{ source('tlc_taxi_trips', 'raw_yellow_tripdata') }}
),

filtred_data AS (
    SELECT *
    FROM sources_data
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND passenger_count > 0
      AND trip_distance > 0
      AND store_and_fwd_flag = 'N'
      AND tpep_pickup_datetime < tpep_dropoff_datetime
      AND tip_amount >= 0
      AND payment_type IN (1, 2)
),

transformed_data AS (
    SELECT 
        CAST(passenger_count AS BIGINT) AS passenger_counts,
        CASE 
            WHEN payment_type = 1 THEN 'Credit Card'
            WHEN payment_type = 2 THEN 'Cash'
            ELSE 'Other'
        END AS payment_type,
        DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
        * EXCLUDE(passenger_count, payment_type)  
    FROM filtred_data
)

SELECT *
FROM transformed_data
WHERE trip_duration_minutes > 0