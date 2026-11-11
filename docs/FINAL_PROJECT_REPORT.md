# Final Project Report — Serverless AWS ETL Pipeline

**Project:** Serverless AWS ETL Pipeline with Automated Data Quality and AI Insights  
**Account / Region:** `744792530652` / `ap-southeast-2` (Sydney)  
**Report Date:** `2026-11-12`  
**Data Disclosure:** All datasets, customer IDs, quality metrics, and AI insights are **synthetic demonstration data**.

---

## A. Executive Summary

The end-to-end serverless AWS ETL, data-quality monitoring, Gemini AI insights, EventBridge scheduling, and local Streamlit presentation layers are **implemented and live-verified**:
1. Daily CSV uploads to S3 (`raw/orders/`) automatically invoke `orders-etl-ingest`, validate records, partition clean rows into Snappy Parquet (`curated/orders/order_date=YYYY-MM-DD/`), quarantine rejected rows (`quarantine/orders/`), emit per-file JSON audits (`reports/orders_YYYY-MM-DD.json`), and publish SNS email alerts (`orders-etl-alerts`) when rejection rates exceed `10%`.
2. AWS Glue Data Catalog (`orders_analytics_db.orders`) and Amazon Athena (`orders-etl-workgroup`) expose `33` contiguous verified partitions (`2026-10-10` to `2026-11-11`, plus isolated `2026-11-12` high-defect test).
3. EventBridge Scheduler (`orders-etl-daily-insights-schedule`) triggers `orders-etl-insights` daily at **12:00 PM `Asia/Kolkata`**. The function verifies Glue partitions, computes authoritative KPIs and day-over-day comparisons in Athena, retrieves `GEMINI_API_KEY` from SSM `SecureString`, generates structured JSON via `gemini-3-flash-preview`, writes `reports/insights/orders_insights_YYYY-MM-DD.json`, and emails the daily summary via `orders-etl-daily-insights`.
4. The local Streamlit dashboard (`dashboard/app.py`) renders live Athena business KPIs, Plotly sales/quality charts, and the S3 AI insights section at `http://localhost:8501`.

---

## B. Implementation Status

| Component | Status | Verification Evidence |
| --- | --- | --- |
| S3 ingestion | Complete and verified | `OrdersCsvIngestTrigger` invoked `orders-etl-ingest` automatically across `2026-10-10`–`2026-11-12` (`324` total S3 objects, `14,612,529` bytes). |
| Lambda 1 (`orders-etl-ingest`) | Complete and verified | Deployed `CodeSha256: nI5tBifcL07EJ4p0wrJnipB34i3ugERvtainjCTGcN8=`; same-date overwrite verified on `2026-11-01` (`192` rows, `1` Parquet file). |
| Data validation | Complete and verified | `13/13` unit tests in `tests/test_lambda_ingest.py` pass; empty, malformed, missing-column, duplicate, and invalid numeric edge cases verified. |
| Quarantine | Complete and verified | Rejected rows archived to `quarantine/orders/orders_YYYY-MM-DD.csv` with `source_row_number` and `reject_reason`. |
| SNS alerts | Complete and verified | `orders-etl-alerts` confirmed subscription (`karthiktatineni34@gmail.com`); `2026-11-12` (`19.23%` reject) triggered CloudWatch `NumberOfMessagesPublished = 1.0` and confirmed email delivery. |
| Glue Catalog | Complete and verified | `orders_analytics_db.orders` holds `33` contiguous partitions (`2026-10-10` to `2026-11-11`) registered via Athena DDL `eac1feb3-ae37-4daf-8d6f-d6eaf80bf3ff`. |
| Athena | Complete and verified | `orders-etl-workgroup` (`100 MiB` cutoff, `SSE_S3`); Nov 10/11 KPIs matched local Parquet; 33-day KPIs (`d8952504-b9c4-4563-96b4-382a705c2e03`) returned `144,175` orders. |
| Lambda 2 (`orders-etl-insights`) | Complete and verified | `6/6` unit tests pass; live `{}` invocation `5ec2b676-fefb-4501-a9b9-2b2ef958bea5` succeeded (`15,561.99 ms`); `2026-11-19` skipped missing partition cleanly. |
| Gemini API | Complete and verified | `gemini-3-flash-preview` via `generativelanguage.googleapis.com` returned valid structured JSON grounded in Athena KPIs; key secured in SSM `/orders-etl/gemini-api-key`. |
| Insights report & email | Complete and verified | Saved `s3://orders-etl-pipeline-2026-11-7f3a9c/reports/insights/orders_insights_2026-11-11.json`; SNS `messageId=69718dcc-a099-5008-8610-eea277b12085` delivered to confirmed inbox. |
| EventBridge Scheduler | Implemented and verified (config); automated firing pending | `orders-etl-daily-insights-schedule` verified `State: ENABLED`, `cron(0 12 * * ? *)`, `Asia/Kolkata`, target `orders-etl-insights`, `{}` payload, `MaximumRetryAttempts=2`, `MaximumEventAgeInSeconds=3600`. Manual `{}` payload succeeded; first real 12:00 PM IST time-triggered invocation has not yet occurred. |
| SQS/DLQ | Optional and deferred | Evaluated and deferred per owner approval; direct S3-to-Lambda trigger preserved. |
| Streamlit dashboard | Complete and verified | `10/10` unit tests pass, `AppTest` passes, live Chromium (Playwright) verified `0` console errors and `0` warnings at `http://localhost:8501` with screenshots captured. |
| Documentation | Complete and verified | `README.md`, `ARCHITECTURE.md`, `PROJECT_GUIDE.md`, `DEPLOYMENT_GUIDE.md`, `DASHBOARD_BUILD_REPORT.md`, `TEST_REPORT.md`, `COST_ESTIMATE.md`, and `FINAL_PROJECT_REPORT.md`. |

---

## C. AWS Resource Inventory

| Resource | Region | ARN / Identifier | Purpose & Configuration |
| --- | --- | --- | --- |
| S3 Bucket | `ap-southeast-2` | `arn:aws:s3:::orders-etl-pipeline-2026-11-7f3a9c` | Raw, curated, quarantine, reports, insights, Athena results; `AES256`, Public Access Block `4/4` `true`. |
| Ingest Lambda | `ap-southeast-2` | `arn:aws:lambda:ap-southeast-2:744792530652:function:orders-etl-ingest` | Python 3.14, `1024 MB`, `120s`, `AWSSDKPandas-Python314:12`, `REJECT_RATE_THRESHOLD=0.10`. |
| Ingest IAM Role | `ap-southeast-2` | `arn:aws:iam::744792530652:role/orders-etl-lambda-ingest-role` | `AWSLambdaBasicExecutionRole`, `orders-etl-ingest-s3-policy`, inline `orders-etl-ingest-sns-policy`. |
| Insights Lambda | `ap-southeast-2` | `arn:aws:lambda:ap-southeast-2:744792530652:function:orders-etl-insights` | Python 3.14, `512 MB`, `120s`, `GEMINI_MODEL=gemini-3-flash-preview`. |
| Insights IAM Role | `ap-southeast-2` | `arn:aws:iam::744792530652:role/orders-etl-insights-role` | Least-privilege Athena, Glue read, S3 curated/reports read, `reports/insights/*` write, SSM+KMS decrypt, daily SNS publish. |
| Scheduler IAM Role | `ap-southeast-2` | `arn:aws:iam::744792530652:role/orders-etl-scheduler-role` | Scoped solely to `lambda:InvokeFunction` on `orders-etl-insights` with `aws:SourceAccount` trust condition. |
| EventBridge Schedule | `ap-southeast-2` | `arn:aws:scheduler:ap-southeast-2:744792530652:schedule/default/orders-etl-daily-insights-schedule` | `State: ENABLED`, `cron(0 12 * * ? *)`, `Asia/Kolkata`, payload `{}`, `MaximumRetryAttempts=2`, `MaximumEventAgeInSeconds=3600`. |
| Glue Catalog | `ap-southeast-2` | `orders_analytics_db.orders` | External Parquet table (`33` partitions: `2026-10-10`–`2026-11-11`). |
| Athena Workgroup | `ap-southeast-2` | `orders-etl-workgroup` | `SSE_S3`, output `athena-results/`, `100 MiB` scan cutoff. |
| SNS Alerts Topic | `ap-southeast-2` | `arn:aws:sns:ap-southeast-2:744792530652:orders-etl-alerts` | Confirmed email sub `...:e3401a5b-1d40-41db-ac6d-bf5fe5328f1d` (`karthiktatineni34@gmail.com`). |
| SNS Daily Topic | `ap-southeast-2` | `arn:aws:sns:ap-southeast-2:744792530652:orders-etl-daily-insights` | Confirmed email sub `...:f3e4dd1c-797f-4d27-b721-cccc60190d5e` (`karthiktatineni34@gmail.com`). |
| SSM SecureString | `ap-southeast-2` | `arn:aws:ssm:ap-southeast-2:744792530652:parameter/orders-etl/gemini-api-key` | Encrypted Gemini API key (`alias/aws/ssm`, Key ID `0a9b994b-b7c7-4458-b39e-a4d5dae7d3b0`). |

---

## D. Data-Quality Results (Synthetic Data)

- **Nov 10 baseline:** `5,050` rows | `4,800` valid | `250` rejected | `4.9505%` reject rate (`50` each across 5 rules).
- **Nov 11 baseline:** `5,050` rows | `4,800` valid | `250` rejected | `4.9505%` reject rate (`50` each across 5 rules).
- **Nov 1 idempotency fixture:** `202` rows | `192` valid | `10` rejected | `4.9505%` reject rate.
- **Nov 12 high-rejection alert test:** `1,040` rows | `840` valid | `200` rejected | `19.2308%` reject rate (alert delivered).
- **33-day dashboard span (`2026-10-10` to `2026-11-11`):** `151,590` input rows | `144,175` valid rows | `7,415` rejected rows | `4.8915%` overall rejection rate.

---

## E. Athena KPI Results (Synthetic Data)

| Scope | Query Execution ID | Orders | Revenue | AOV |
| --- | --- | ---: | ---: | ---: |
| `2026-11-10` | `36c46379-d229-4173-a6fa-4bcdd9bc95e1` | `4,800` | `1,839,616.39` | `383.25` |
| `2026-11-11` | `666c865a-ce16-4d60-898b-932876b212d2` | `4,800` | `1,868,497.74` | `389.27` |
| `2026-10-10` to `2026-11-11` (33 days) | `d8952504-b9c4-4563-96b4-382a705c2e03` | `144,175` | `55,004,231.69` | `381.51` |

---

## F. AI Insights Results

- **Model Used:** `gemini-3-flash-preview` (structured JSON via `responseSchema`).
- **S3 Output Location:** `s3://orders-etl-pipeline-2026-11-7f3a9c/reports/insights/orders_insights_2026-11-11.json`
- **Sample Grounded Executive Summary:** *"This report provides an analysis of simulated retail performance for 2026-11-11. Total revenue reached 1868497.74, a 1.57% increase over the previous day, despite stagnant order volume. The dataset consists of synthetic retail data and synthetic customer IDs."*
- **Email Delivery:** Confirmed by owner at `karthiktatineni34@gmail.com` (`sns_message_id: 69718dcc-a099-5008-8610-eea277b12085`).

---

## G. Test & Security Evidence

- **Automated Unit Tests:** `29/29 passed` (`.\.venv\Scripts\python.exe -m unittest discover -s tests -v`, `0.204s`).
- **Live Browser Check:** Playwright Chromium on `http://localhost:8501` returned `0` console errors and `0` warnings; screenshots captured in [`docs/screenshots/dashboard_full.png`](screenshots/dashboard_full.png) and [`docs/screenshots/dashboard_insights.png`](screenshots/dashboard_insights.png).
- **Security Checks:** `aws iam list-users` returned `[]`; S3 Public Access Block `4/4` enabled; `AES256` default encryption active; secret scan found `0` hits outside local `.env`.

---

## H. Cost & Security Review

- **Monthly Budget:** `aws-etl-monthly-cost-watch` (`$5.00 USD/month`, `HealthStatus: HEALTHY`).
- **Estimated Run Rate:** **~$0.02 – $0.10 USD/month** across S3 (`13.94 MiB`), Lambda, Glue, Athena, SNS, EventBridge Scheduler, SSM `SecureString`, CloudWatch Logs, and Gemini API (see [`docs/COST_ESTIMATE.md`](COST_ESTIMATE.md)).
- **Render Hosting Assessment:** Hosting the dashboard publicly on Render is **not recommended** for this setup without OIDC/IAM authentication refactoring because Render lacks local `~/.aws` login credentials and public refresh triggers create unnecessary Athena scan exposure.

---

## I. Remaining Limitations

- Per-file duplicate `order_id` detection (no cross-file global deduplication).
- Zero-valid-row reprocessed CSVs skip `overwrite_partitions` rather than purging older partition objects.
- `orders-etl-ingest` does not auto-register brand-new `order_date` partitions in Glue (`MSCK REPAIR TABLE` or `ALTER TABLE ADD PARTITION` is needed after uploading new dates).
- EventBridge Scheduler is configured for `12:00 PM IST` (`cron(0 12 * * ? *)`) and `ENABLED`; an actual automated 12:00 PM IST scheduled invocation has not been observed yet (only manual `{}` payload execution has been tested). Note also that because the pipeline processes synthetic test files uploaded manually (no live automated daily upstream producer runs yet), future scheduled runs on dates without uploaded partitions will cleanly exit with `skipped_missing_partition`.
- SQS/DLQ hardening remains deferred.

---

## J. What You Need From the User

No further user action is required for the verified core pipeline.

*(Optional future actions only when you choose to proceed: capture any personal Gmail/AWS console screenshots for your GitHub portfolio, initialize a Git repository and authorize pushing to GitHub when ready, or request future cleanup of the preserved `2026-11-01` and `2026-11-12` test artifacts.)*
