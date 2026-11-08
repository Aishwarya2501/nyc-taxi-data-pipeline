# NYC Taxi Data Pipeline

PySpark pipeline on Databricks, built on the medallion pattern. Once I'd mapped the work into ingest, transform and validation, Bronze → Silver → Gold was the natural shape: raw parquet in (Bronze), schemas aligned and bad trips quarantined (Silver), zone-enriched tables for analytics (Gold).

It loads Yellow + Green taxi parquet (Dec 2024 - Jan 2025) and writes two Unity Catalog tables:

- `workspace.default.taxi_trips` — the clean trips
- `workspace.default.taxi_quarantine` — rejected trips, with the reason attached

## How to run it

The raw files live in `data/raw_data/`. Open `src/orchestrator.py` and click Run All (or run it as a Job task, or `python src/orchestrator.py`). It finds the data relative to the project, no hardcoded paths. To point it somewhere else, set the `TAXI_DATA_DIR` environment variable to your project root.

Once it's done, the analytics are in the `src/queries` notebook, and the three query results are also saved as CSVs in `outputs/`.

Re-running on the same input overwrites both tables, so you never get duplicate rows.

## Data quality issues and how I handled each

Five checks. A failing row goes to quarantine tagged with every rule it broke. Nothing gets deleted.

| Issue | Rule | Trips caught |
| --- | --- | --- |
| Negative fare | fare_amount < 0 | 222,877 |
| Bad distance | trip_distance <= 0 or > 1000 miles | 172,249 |
| Dropoff before pickup | dropoff <= pickup | 3,382 |
| Outside date range | pickup outside Dec 2024 - Jan 2025 | 77 |
| Missing pickup zone | PULocationID null or unknown | 0 |

7,245,917 raw trips in, 376,415 quarantined, 6,869,502 clean. Most of the date-range rejects are records with nonsense years in them.

## How I dealt with Yellow and Green having different columns

- Yellow names its timestamps `tpep_...`, Green uses `lpep_...`. Renamed both to `pickup_datetime` / `dropoff_datetime`.
- Yellow has `Airport_fee`, Green has `ehail_fee` and `trip_type`. `unionByName(allowMissingColumns=True)` fills the missing side with NULL.
- Added a `cab_colour` column (Yellow / Green) before the union so queries can split them apart.

So `ehail_fee` and `trip_type` are NULL on every Yellow row in the final table. Expected, not lost data.

## Assumptions I made

- The data is Dec 2024 and Jan 2025 only.
- A $0 fare passes, only negative fares get rejected.
- `passenger_count` of 0 or NULL passes, drivers often skip it.
- Zero-duration trips count as invalid, since I used `<=`.

## What I logged, and how I'd know it broke

Each run prints the passed count, quarantined count, and a breakdown per rejection reason. Every quarantined row keeps its reasons, so rejects can be audited.

It's broken if passed + quarantined stops adding up to 7,245,917, or a quarantine category balloons or vanishes, or a renamed input column fails the run at load time. It fails loudly, not silently.

## What I'd improve with more time

1. Check `total_amount` too, today a trip can pass with a positive fare but a zero or negative total.
2. Drop exact duplicate trips, clean up better and enrich further.
3. Run it on a schedule as a Workflow Job in Databricks and alert if the counts drift.
