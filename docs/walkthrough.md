# 3-minute walkthrough: PySpark Lakehouse Pipeline

A timed script for a Loom recording of about 3 minutes (screen plus camera bubble, 1080p). Say it in your own words; the lines are a guide, not a teleprompter.

**Open these tabs before you press record:** README, the medallion diagram, `lakehouse/generate.py`, `lakehouse/transforms.py`, `lakehouse/pipeline.py`, `tests/test_transforms.py`, GitHub Actions.

| Time | On screen | What to say |
|---|---|---|
| 0:00–0:20 | Portfolio card, then the README title | Order and shipment feeds arrive with duplicates, blanks and unknown products, and delivery KPIs built on them are wrong. This PySpark pipeline cleans them through bronze, silver and gold. |
| 0:20–0:40 | README intro and `generate.py` | The data is generated with those problems planted on purpose. It shows the lakehouse and Databricks skills on my CV; my Smart-Meter project is the larger Spark example. |
| 0:40–1:05 | Medallion diagram | Bronze is the raw feeds. Silver has explicit schemas, typed dates and deduplication. Gold is a product dimension, a shipment fact partitioned by warehouse, and monthly carrier KPIs. |
| 1:05–1:35 | `transforms.py`: `clean_orders` | Bad rows are quarantined with a reason, never silently dropped. That makes data quality measurable, and the data owner knows exactly what to fix at source. |
| 1:35–2:05 | `transforms.py`: `quality_checks` | Quality gates fail the job: an empty fact, duplicate order ids, null order values, or more than 5% of input quarantined. |
| 2:05–2:30 | `transforms.py`: the fact and KPIs | Each shipment gets an on-time flag, days late and order value, including undelivered orders. The monthly mart gives on-time %, average days late and delivered value by warehouse and carrier. |
| 2:30–2:45 | `tests/test_transforms.py` and GitHub Actions | The transforms are pure DataFrame functions, tested with a local SparkSession in CI, and portable to Databricks by switching Parquet to Delta. |
| 2:45–3:00 | Back to the README top | For an operations team, that's delivery KPIs they can trust, and a quarantine table that tells them what to fix. Links are under the video. |

## Recording checklist
- Close email, chat and notifications. Zoom the browser to 125% so code is readable.
- Spark needs Java 17 or 21. If it isn't set up, show the CI run instead of running the pipeline live.
- Keep to one take per section; Loom lets you trim. Aim for 2:45 to 3:10.
- Show real numbers on screen as you say them; never estimate on camera.
- Say "synthetic" out loud when you introduce the data. This is a skills demonstration, not a rebuild of client work.

## After recording
1. Copy the Loom share link.
2. In `README.md`, replace the walkthrough line with `- [x] [3-minute Loom walkthrough](<your Loom link>)` and add the link under the title.
3. Add the link to this project's card on saigwe.github.io.
