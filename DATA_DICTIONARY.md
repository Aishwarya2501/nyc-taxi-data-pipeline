# nyc-taxi-data-pipeline
Data pipeline for ingesting, cleaning, and validating NYC taxi trip records into a Delta Lake architecture.

# Data Dictionary  
`workspace.default.taxi_trips`

* **Volume:** 6,869,502 rows
* **Scope:** December 2024 – January 2025, Yellow + Green cabs combined
* **Storage:** managed Delta table (re-runs overwrite rather than append, so counts stay stable)
* **Quarantine:** rows that failed the 5 data quality checks live in `workspace.default.taxi_quarantine`, tagged with a `rejection_reason`

| Column | Type | Plain-English meaning |
| --- | --- | --- |
| DOLocationID | int | TLC zone ID where the trip ended. |
| PULocationID | int | TLC zone ID where the trip started. |
| VendorID | int | Code for the cab's meter provider (1 or 2). |
| pickup_datetime | timestamp_ntz | When the passenger got in. |
| dropoff_datetime | timestamp_ntz | When the passenger got out. |
| passenger_count | bigint | Passengers as entered by the driver. |
| trip_distance | double | Trip length in miles per the meter. |
| RatecodeID | bigint | Fare-rate code (1 = standard metered, 2 = JFK flat rate,...) |
| store_and_fwd_flag | string | Y = meter had no signal, so the record was stored and sent later. |
| payment_type | bigint | How it was paid: 1 = card, 2 = cash, 3 = no charge, 4 = dispute. |
| fare_amount | double | The meter fare alone — before tips, tolls, and surcharges. |
| extra | double | Rush-hour and overnight surcharges added on top of the fare. |
| mta_tax | double | Flat $0.50 MTA tax per ride. |
| tip_amount | double | Tip (card payments only). |
| tolls_amount | double | Tolls paid during the trip. |
| improvement_surcharge | double | Flat $1.00 ride surcharge. |
| total_amount | double | Full amount charged to the passenger. |
| congestion_surcharge | double | Manhattan congestion charge added at the end of the ride. |
| Airport_fee | double | Airport pickup fee (JFK/LaGuardia). |
| cab_colour | string | Yellow or Green cab. |
| ehail_fee | double | E-hail booking fee (Green only). |
| trip_type | bigint | 1 = street hail, 2 = dispatch (Green only). |
| pickup_borough | string | Borough of the pickup zone. |
| pickup_zone | string | Pickup zone name, e.g. JFK Airport. |
| dropoff_borough | string | Borough where the trip ended. |
| dropoff_zone | string | Dropoff zone name. |

## Heads Up for Analysts

* **total_amount is not fare_amount**: total_amount is what the passenger actually paid (use it for revenue); fare_amount is only the meter portion.
* **ehail_fee and trip_type are NULL for all Yellow rows** : NULL there means the column does not exist for that cab type, not missing data.
* **tip_amount only exists for card payments** : average tip underestimates cash riders.
* **passenger_count of 0 or NULL does not mean a fake ride**: drivers often skip it, and our checks deliberately do not reject these.
