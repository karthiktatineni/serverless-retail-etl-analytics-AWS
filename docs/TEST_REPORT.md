# Test Report — Automated Unit & Live AWS Integration Evidence

> **Simulated Data Notice:** All tested rows, partitions, quality reports, and Gemini summaries are generated from synthetic demonstration orders.

## 1. Automated Unit Test Suite (`29/29 Passed`)

Executed via `.\.venv\Scripts\python.exe -m unittest discover -s tests -v` (`0.204s`, zero failures):

- **`tests/test_lambda_ingest.py` (13 tests):**
  - Valid CSV parsing, `int64` casting, `order_date` partition derivation.
  - Missing/fractional/non-numeric `customer_id`, missing/invalid `order_ts`, zero/negative/fractional `quantity`, zero/negative/`inf` `unit_price`.
  - Per-file duplicate `order_id` detection (`keep="first"`) without falsely flagging multiple missing `order_id` rows as duplicates.
  - Missing required columns, 0-byte CSV (`EmptyDataError`), header-only CSV (`df.empty`), and malformed CSV (`ParserError`).
  - Multi-record S3 event filtering, URL-decoded keys, `overwrite_partitions` call verification, SNS alert suppression below threshold, SNS publish above threshold, and SNS error logging + re-raise.
- **`tests/test_lambda_insights.py` (6 tests):**
  - Explicit `date`/`report_date` resolution and default EventBridge `{}` yesterday-in-`Asia/Kolkata` date resolution.
  - Athena `FAILED` status error raising and Athena timeout (`stop_query_execution` cleanup + exception).
  - Gemini structured JSON success, malformed JSON failure, HTTP `429` bounded retry recovery, and HTTP `400` non-retryable failure.
  - Missing Glue partition clean skip (`skipped_missing_partition`, no S3 write, no SNS publish).
  - Full mocked handler flow verifying KPI deltas, S3 `reports/insights/orders_insights_YYYY-MM-DD.json` upload, SNS publish, and idempotent re-invocation.
- **`tests/test_dashboard.py` (10 tests):**
  - Date validation (`YYYY-MM-DD`, invalid range rejection) and partition-filter SQL guard.
  - Athena paginated result assembly (header row skipped on page 0 only) and `FAILED` query exception handling.
  - Quality report filename filtering (`orders_YYYY-MM-DD.json` only) and missing/malformed report handling.
  - AI insights missing report, malformed JSON, stale report detection, and regression test `test_fetch_latest_insights_nov11_not_stale_when_nov12_partition_exists` confirming `2026-11-11` is not flagged stale when a `2026-11-12` partition exists on `2026-11-12` IST.

## 2. Controlled Live AWS Integration Tests

### A. Ingestion, Partition Overwrite & Quality Alerting (`orders-etl-ingest`)
- **Nov 10 (`2026-11-10`):** `5,050` input rows -> `4,800` valid, `250` rejected (`4.95%`), verified against local Parquet (`4,800` orders, `3,589` customers, `12,084` units, `1,839,616.39` revenue, `383.25` AOV).
- **Nov 11 (`2026-11-11`):** `5,050` input rows -> `4,800` valid, `250` rejected (`4.95%`), verified against `docs/curated_orders_2026-11-11.parquet` and Athena (`4,800` orders, `3,606` customers, `11,993` units, `1,868,497.74` revenue, `389.27` AOV).
- **Isolated Same-Date Reprocessing (`2026-11-01`, `202` rows):** Upload 1 wrote `849e12f24d3a4ea3929ca242f565edbe.snappy.parquet` (`192` valid rows); upload 2 replaced it with `b42f49f6a1544233a1f32bae10b55086.snappy.parquet` (`Parquet file count: 1`, `Total rows: 192`, `Unique orders: 192`).
- **High-Rejection SNS Alert (`2026-11-12`, `1,040` rows, `--defect-rate 0.04`):** `840` valid, `200` rejected (`19.2308%` > `10%`). CloudWatch RequestId `66975a0d-3d46-4121-8563-1adaa7399346` succeeded; CloudWatch `AWS/SNS` `NumberOfMessagesPublished = 1.0`; email delivery to `karthiktatineni34@gmail.com` confirmed by owner.
- **30-Day Historical Backfill (`2026-10-10` to `2026-11-09` excl. `2026-11-01`):** 30 CSVs uploaded to `raw/orders/`, all 30 processed by `orders-etl-ingest`, registered via Athena DDL (`eac1feb3-ae37-4daf-8d6f-d6eaf80bf3ff`). Combined 33-day (`2026-10-10` to `2026-11-11`) Athena KPIs (`qid=d8952504-b9c4-4563-96b4-382a705c2e03`, `144,175` orders, `55,004,231.69` revenue, `381.51` AOV) match S3 quality reports (`151,590` input, `144,175` valid, `7,415` rejected, `4.8915%`).

### B. AI Insights Lambda (`orders-etl-insights`) & Scheduled Config
- **Live `{}` EventBridge-format invocation (`RequestId: 5ec2b676-fefb-4501-a9b9-2b2ef958bea5`):**
  - Resolved `report_date=2026-11-11` (`Asia/Kolkata`).
  - Executed 3 partition-filtered Athena queries (`0cf3ab18-efc8-47e2-bac1-7de317155687`, `e334201b-90e5-45f1-aeed-73363e65224c`, `154c3a1d-60ab-4cb1-8603-77c40034f735`).
  - Called Gemini `gemini-3-flash-preview` via `generativelanguage.googleapis.com` using SSM `SecureString` `/orders-etl/gemini-api-key`.
  - Wrote `s3://orders-etl-pipeline-2026-11-7f3a9c/reports/insights/orders_insights_2026-11-11.json`.
  - Published SNS `messageId=69718dcc-a099-5008-8610-eea277b12085` to confirmed subscription `karthiktatineni34@gmail.com`; email delivery confirmed by owner.
- **Missing partition live check (`{"date": "2026-11-19"}`, `RequestId: 5ed7b6ed-f0b4-4c1f-aec3-5c6d5e16aa9c`):** Returned `status: "skipped_missing_partition"`, logged warning, wrote no S3 file, sent no SNS email.
- **EventBridge Scheduler:** `orders-etl-daily-insights-schedule` verified via `aws scheduler get-schedule` as `State: ENABLED`, `cron(0 12 * * ? *)`, `Asia/Kolkata`, targeting `orders-etl-insights` with `{}` and `RetryPolicy` (`MaximumRetryAttempts=2`, `MaximumEventAgeInSeconds=3600`). *(Note: Manual `{}` invocation succeeded; an automated 12:00 PM IST time-triggered invocation has not been observed yet.)*

### C. Streamlit Dashboard & Browser Verification
- **Live Playwright Chromium (`http://localhost:8501`):** `0` console errors, `0` warnings; verified KPIs (`144,175` orders, `55,004,231.69` revenue, `381.51` AOV, `4.89%` rejection), all 6 Plotly charts, Quality Report Coverage, and live S3 `2026-11-11` **AI-Generated Business Insights** with no false stale alert. Screenshots saved to `docs/screenshots/dashboard_full.png` and `docs/screenshots/dashboard_insights.png`.
- **Streamlit `AppTest` UI state tests:** Verified missing, malformed, and stale warning rendering plus single-day (`2026-11-10`) filter updates.

## 3. Security & Secret Scan Verification
- `aws iam list-users` returned `{"Users": []}` (no static IAM access keys created).
- S3 Public Access Block `4/4` enabled (`true`), `AES256` encryption active.
- Repository secret regex scan found `0` matches outside local `.env`.
