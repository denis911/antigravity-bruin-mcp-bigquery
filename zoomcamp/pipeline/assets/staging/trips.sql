/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: integer
    description: "A code indicating the TPEP vendor that provided the record."
  - name: tpep_pickup_datetime
    type: timestamp
    description: "The date and time when the meter was engaged."
    primary_key: true
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "The date and time when the meter was disengaged."
  - name: passenger_count
    type: float
    description: "The number of passengers in the vehicle."
  - name: trip_distance
    type: float
    description: "The elapsed trip distance in miles reported by the taximeter."
  - name: payment_type
    type: integer
    description: "A numeric code signifying how the passenger paid for the trip."
  - name: payment_type_name
    type: string
    description: "Descriptive name of the payment type (credit card, cash, etc.)."
  - name: fare_amount
    type: float
    description: "The time-and-distance fare calculated by the meter."
  - name: extra
    type: float
    description: "Miscellaneous extras and surcharges."
  - name: mta_tax
    type: float
    description: "0.50 MTA tax that is automatically triggered based on the metered rate in use."
  - name: tip_amount
    type: float
    description: "Tip amount – this field is automatically populated for credit card tips. Cash tips are not included."
  - name: tolls_amount
    type: float
    description: "Total amount of all tolls paid in trip. "
  - name: improvement_surcharge
    type: float
    description: "0.30 improvement surcharge assessed trips at the time of sections, but not for credit card tips."
  - name: total_amount
    type: float
    description: "The total amount charged to passengers. Does not include cash tips."
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge for the trip."
  - name: airport_fee_legacy
    type: float
    description: "Airport fee (renamed from Airport_fee to avoid collisions)."
  - name: extraction_timestamp
    type: timestamp
    description: "Timestamp when the data was extracted from the source."

@bruin */

SELECT 
    t.vendor_id,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    p.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    -- Handle the duplicate column naming collision issue noted in trips.py
    t.airport_fee, 
    -- as airport_fee_legacy,
    t.extraction_timestamp
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p ON t.payment_type = p.payment_type_id
WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
  AND t.tpep_pickup_datetime < '{{ end_datetime }}'
