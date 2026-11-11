# Serverless AWS ETL Pipeline with Automated Data Quality & AI Insights

> **Simulated Data Disclosure:** All order CSVs, customer IDs, quality metrics, and AI insights in this repository are generated from **synthetic retail data** via [`scripts/generate_orders.py`](scripts/generate_orders.py) for engineering demonstration. Do not interpret figures as real customer transactions.

## Project Overview & Business Problem

Daily retail ingestion pipelines frequently fail silently when upstream exports contain missing keys, malformed timestamps, negative quantities, or duplicate records—leading to skewed executive dashboards and manual fire drills. This project builds an automated, serverless AWS ETL and analytics platform that:

1. Ingests daily CSV exports via Amazon S3 event notifications into AWS Lambda (`orders-etl-ingest`).
2. Validates every record, partitions valid orders into Snappy-compressed Parquet in S3, quarantines bad rows with granular rejection reasons, and writes JSON data-quality audits.
3. Fires immediate SNS email alerts whenever a batch's rejection rate exceeds `10%`.
4. Catalogs curated data in AWS Glue Data Catalog and exposes partition-filtered SQL analytics through Amazon Athena (`orders-etl-workgroup`).
5. Runs a scheduled daily Lambda (`orders-etl-insights`) via EventBridge Scheduler (`12:00 PM IST`) that computes verified KPIs in Athena and invokes Google's Gemini API (`gemini-3-flash-preview`)—with the API key encrypted in AWS SSM Parameter Store—to publish structured JSON insights and a daily executive email.
6. Surfaces interactive business analytics, data-quality coverage, and AI insights in a local Streamlit dashboard (`dashboard/app.py`).

## Architecture

```mermaid
flowchart TD
    G[Synthetic CSV Generator<br/>scripts/generate_orders.py] -->|Upload CSV| RAW[Amazon S3<br/>raw/orders/]
    RAW -->|s3:ObjectCreated:* .csv| L1[Lambda: orders-etl-ingest<br/>Python 3.14 + AWSSDKPandas]
    L1 -->|Valid rows<br/>overwrite_partitions| CUR[Amazon S3<br/>curated/orders/order_date=YYYY-MM-DD/ Parquet]
    L1 -->|Rejected rows| QUAR[Amazon S3<br/>quarantine/orders/]
    L1 -->|Quality metrics| QREP[Amazon S3<br/>reports/orders_YYYY-MM-DD.json]
    L1 -->|reject_rate > 0.10| SNS1[SNS: orders-etl-alerts]
    SNS1 -->|Alert email| USER[Subscriber Inbox]

    CUR --> GLUE[Glue Data Catalog<br/>orders_analytics_db.orders]
    GLUE --> ATH[Athena Workgroup<br/>orders-etl-workgroup]

    EB[EventBridge Scheduler<br/>12:00 PM Asia/Kolkata] -->|Payload {}| L2[Lambda: orders-etl-insights<br/>Python 3.14]
    L2 --> GLUE
    L2 --> ATH
    L2 -->|WithDecryption=True| SSM[SSM Parameter Store<br/>SecureString]
    L2 -->|Aggregated KPIs| GEM[Google Gemini API<br/>gemini-3-flash-preview]
    L2 -->|Structured JSON| INS[S3: reports/insights/orders_insights_YYYY-MM-DD.json]
    L2 -->|Daily briefing| SNS2[SNS: orders-etl-daily-insights]
    SNS2 -->|Daily email| USER

    ATH --> DASH[Local Streamlit Dashboard<br/>dashboard/app.py]
    QREP --> DASH
    INS --> DASH
```

*Detailed architecture notes: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)*

## AWS Services Used

| Service | Resource / Identifier | Configuration |
| --- | --- | --- |
| **Amazon S3** | `orders-etl-pipeline-2026-11-7f3a9c` | `ap-southeast-2`, SSE-S3 (`AES256`), Public Access Block `4/4` enabled |
| **AWS Lambda (Ingest)** | `orders-etl-ingest` | Python 3.14, `1024 MB`, `120s`, `AWSSDKPandas-Python314:12`, `REJECT_RATE_THRESHOLD=0.10` |
| **AWS Lambda (Insights)** | `orders-etl-insights` | Python 3.14, `512 MB`, `120s`, `GEMINI_MODEL=gemini-3-flash-preview` |
| **AWS Glue Catalog** | `orders_analytics_db.orders` | External Snappy Parquet table partitioned by `order_date` (`33` partitions verified) |
| **Amazon Athena** | `orders-etl-workgroup` | `SSE_S3`, output `athena-results/`, `100 MiB` per-query bytes scan cutoff |
| **Amazon SNS** | `orders-etl-alerts`, `orders-etl-daily-insights` | Standard topics with confirmed email subscriptions |
| **EventBridge Scheduler** | `orders-etl-daily-insights-schedule` | `cron(0 12 * * ? *)`, `Asia/Kolkata`, `State: ENABLED`, dedicated `orders-etl-scheduler-role` |
| **SSM Parameter Store** | `/orders-etl/gemini-api-key` | `SecureString` encrypted with AWS-managed `alias/aws/ssm` KMS key |
| **AWS Budgets** | `aws-etl-monthly-cost-watch` | `$5.00 USD` monthly budget alert |

## Data Flow, Validation & Partitioning

- **Input schema (`raw/orders/`):** `order_id`, `customer_id`, `category`, `quantity`, `unit_price`, `order_ts`.
- **Validation rules:** Rejects empty `order_id` or `category`, missing/non-integer `customer_id`, non-positive/fractional `quantity`, non-positive/non-finite `unit_price`, unparseable `order_ts`, and per-file duplicate `order_id`s (`keep="first"`). Missing/empty `order_id`s do not falsely increment `duplicate_order_id`.
- **Quarantine & Quality Reports:** Rejected rows write to `quarantine/orders/orders_YYYY-MM-DD.csv` with `source_row_number` and semicolon-joined `reject_reason`. Per-file quality JSON writes to `reports/orders_YYYY-MM-DD.json`.
- **Parquet Partitioning:** Valid rows write via `awswrangler.s3.to_parquet(..., mode="overwrite_partitions", partition_cols=["order_date"], compression="snappy")`. Re-uploading the same date replaces the partition file idempotently (`192` rows verified twice on `2026-11-01`).
- **SQL & Athena Analytics:** Partition-filtered queries in [`sql/`](sql/) (`01_validate_orders.sql`, `02_daily_kpis.sql`, `03_multi_day_kpis.sql`). Across the verified 33-day dataset (`2026-10-10` to `2026-11-11`), Athena aggregates `144,175` valid orders and `55,004,231.69` revenue (`381.51` AOV).

## SNS Alerts, Gemini AI Insights & EventBridge Scheduling

- **Quality Alerts (`orders-etl-alerts`):** Tested with synthetic high-defect batch `orders_2026-11-12.csv` (`1,040` rows, `200` rejected, `19.23%` rejection rate); SNS email delivery confirmed end-to-end.
- **Gemini AI Insights (`orders-etl-insights`):** Computes authoritative KPIs, day-over-day deltas, category share, and top-10 customer concentration in Athena/Python before passing aggregated-only JSON to `gemini-3-flash-preview` with `responseSchema` structured JSON output. Saves `reports/insights/orders_insights_YYYY-MM-DD.json` and emails via `orders-etl-daily-insights`.
- **EventBridge Scheduler (`orders-etl-daily-insights-schedule`):** Triggers `orders-etl-insights` daily at `12:00 PM` `Asia/Kolkata` (`cron(0 12 * * ? *)`, payload `{}`, `MaximumRetryAttempts=2`). The Lambda resolves yesterday's IST date and skips gracefully (`skipped_missing_partition`) if data is unavailable rather than fabricating insights.

## Interactive Analytics Dashboard

The dashboard uses simulated order data and runs locally. It is not publicly hosted.

- **Why Streamlit:** Rapid Python-native UI with Plotly interactivity, sidebar date controls, and `@st.cache_data(ttl=3600)` caching (1 hour default) without maintaining frontend build pipelines.
- **Athena & S3 Integration:** [`dashboard/athena.py`](dashboard/athena.py) authenticates via local `AWS_PROFILE=etl-project` or Render environment variables (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) and enforces `order_date` filters on every query. [`dashboard/quality.py`](dashboard/quality.py) aggregates S3 quality JSON (`SUM(rejected_rows) / SUM(total_rows) * 100`). [`dashboard/insights.py`](dashboard/insights.py) displays the latest S3 Gemini briefing with IST completed-day freshness checks and missing/malformed warnings.
- **Render Hosting & Budget Guardrails:** Includes [`render.yaml`](render.yaml), read-only policy [`docs/render_readonly_policy.json`](docs/render_readonly_policy.json), 1-hour query caching (`DASHBOARD_CACHE_TTL_SECONDS=3600`), 60s refresh cooldown, and optional `DASHBOARD_PASSCODE` access protection so public traffic cannot spam Athena.
- **KPIs & Charts:** KPI cards (`Total Orders`, `Total Revenue`, `Average Order Value`, `Overall Rejection Rate`), daily orders/revenue line charts, category revenue bar chart, top-10 simulated customers horizontal bar chart, daily rejection trend, validation-failure occurrences bar chart, quality coverage metrics, and the **AI-Generated Business Insights** briefing.

![Orders Analytics Dashboard Overview](docs/screenshots/dashboard_full.png)
![AI-Generated Business Insights Panel](docs/screenshots/dashboard_insights.png)

## Local Execution, Testing & Deployment

### Run the Dashboard Locally
```powershell
Copy-Item .env.example .env
.\.venv\Scripts\Activate.ps1
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

### Run Automated Tests (`29/29 Passing`)
```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

### Generate & Upload Synthetic Data
```powershell
.\.venv\Scripts\python.exe .\scripts\generate_orders.py 2026-11-13 --rows 5000 --defect-rate 0.01
aws --no-cli-pager s3 cp .\data\orders_2026-11-13.csv s3://orders-etl-pipeline-2026-11-7f3a9c/raw/orders/ --profile etl-project --region ap-southeast-2
```

*Full deployment & operations guide: [`docs/DEPLOYMENT_GUIDE.md`](docs/DEPLOYMENT_GUIDE.md) | Developer guide: [`docs/PROJECT_GUIDE.md`](docs/PROJECT_GUIDE.md)*

## IAM, Credential Security & Cost Considerations

- **Zero static keys:** Authentication uses AWS CLI v2 browser login (`etl-project`), `aws iam list-users` is empty (`[]`), and `.env` is excluded by `.gitignore`.
- **SSM SecureString:** `GEMINI_API_KEY` is stored encrypted in SSM (`/orders-etl/gemini-api-key`) and fetched with `WithDecryption=True` without logging.
- **Least-privilege IAM:** Separate roles for ingestion (`orders-etl-lambda-ingest-role`), insights (`orders-etl-insights-role`), and scheduler (`orders-etl-scheduler-role`).
- **Costs:** Steady-state monthly cost is estimated around **~$0.02 – $0.10 USD/month** within AWS Free Tier and the `$5.00 USD` monthly budget ([`docs/COST_ESTIMATE.md`](docs/COST_ESTIMATE.md)).

## Known Limitations

- Duplicate `order_id` checks are per-file (`keep="first"`), not global across files.
- If a reprocessed CSV has zero valid rows, `overwrite_partitions` is skipped so an older partition may remain.
- `orders-etl-ingest` writes Parquet without calling Glue `CreatePartition`; new dates require `MSCK REPAIR TABLE` or `ALTER TABLE ADD PARTITION` before Athena queries see them.
- SQS/DLQ buffering is intentionally deferred; direct S3-to-Lambda relies on Lambda's async retry behavior (at-least-once delivery).

## Project Reports & Evidence

- [`docs/FINAL_PROJECT_REPORT.md`](docs/FINAL_PROJECT_REPORT.md) — End-to-end verification & handover summary
- [`docs/TEST_REPORT.md`](docs/TEST_REPORT.md) — Unit & live AWS integration test results
- [`docs/DASHBOARD_BUILD_REPORT.md`](docs/DASHBOARD_BUILD_REPORT.md) — Dashboard architecture, verification & Render hosting guidance
- [`docs/COST_ESTIMATE.md`](docs/COST_ESTIMATE.md) — AWS & Gemini resource/cost audit
