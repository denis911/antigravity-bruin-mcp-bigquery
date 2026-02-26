/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "The date when the trip started."
    primary_key: true
  - name: total_revenue
    type: double
    description: "The total amount charged to passengers for all trips on this date."
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: double
    description: "The total distance traveled for all trips on this date."
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: "The total fare amount for all trips on this date."
    checks:
      - name: non_negative
  - name: trip_count
    type: bigint
    description: "The total number of trips on this date."
    checks:
      - name: positive

@bruin */

SELECT 
    CAST(tpep_pickup_datetime AS DATE) as pickup_date,
    SUM(total_amount) as total_revenue,
    SUM(trip_distance) as total_trip_distance,
    SUM(fare_amount) as total_fare_amount,
    COUNT(*) as trip_count
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY 1
