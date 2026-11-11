# Project Guide — Serverless AWS ETL Pipeline

> **Simulated Data Notice:** Every dataset in this repository is synthetic and generated locally for demonstration and testing purposes. Customer IDs do not map to real individuals.

## Repository Layout

```text
ETL/
├── .env.example                 # Non-secret environment template
├── .gitignore                   # Excludes .venv, .env, ZIPs, CSVs, Parquet
├── .streamlit/config.toml       # Light theme configuration
├── README.md                    # Portfolio overview and quickstart
├── requirements.txt             # Root dev/test dependencies
├── dashboard/
│   ├── app.py                   # Streamlit UI (KPIs, Plotly charts, Quality, AI Insights)
│   ├── athena.py                # Glue discovery + partition-filtered Athena client
│   ├── insights.py              # S3 AI insights loader + IST completed-day freshness checks
│   ├── quality.py               # S3 quality report aggregator + missing/malformed checks
│   └── requirements.txt         # Local dashboard dependencies (includes boto3[crt])
├── docs/
│   ├── ARCHITECTURE.md          # Mermaid diagram & design boundaries
│   ├── COST_ESTIMATE.md         # Resource inventory & $5 budget audit
│   ├── DASHBOARD_BUILD_REPORT.md
│   ├── DEPLOYMENT_GUIDE.md      # Step-by-step deployment & operations
│   ├── FINAL_PROJECT_REPORT.md  # Full verification & handover report
│   ├── PROJECT_GUIDE.md         # This guide
│   ├── TEST_REPORT.md           # Unit + live AWS integration test evidence
│   ├── screenshots/             # Real captured Streamlit UI screenshots
│   └── *.json                   # Saved IAM policies, Glue/Athena configs, test payloads
├── scripts/
│   └── generate_orders.py       # Synthetic daily order CSV generator
├── sql/
│   ├── 01_validate_orders.sql
│   ├── 02_daily_kpis.sql
│   └── 03_multi_day_kpis.sql
├── src/
│   ├── lambda_ingest/lambda_function.py
│   └── lambda_insights/lambda_function.py
└── tests/
    ├── test_dashboard.py
    ├── test_lambda_ingest.py
    └── test_lambda_insights.py
```

## Data Schema & Validation Rules

### Input CSV Schema (`raw/orders/orders_YYYY-MM-DD.csv`)
| Column | Required Type / Rule | Rejection Reason Key |
| --- | --- | --- |
| `order_id` | Non-empty string; unique per file (`keep="first"`) | `missing_order_id`, `duplicate_order_id` |
| `customer_id` | Finite integer value (`mod 1 == 0`) | `missing_customer_id` |
| `category` | Non-empty string | `missing_category` |
| `quantity` | Positive finite integer (`> 0`, `mod 1 == 0`) | `invalid_quantity` |
| `unit_price` | Positive finite numeric (`> 0`, not `inf`/`-inf`) | `invalid_unit_price` |
| `order_ts` | Parseable UTC timestamp | `missing_order_ts` |

- **Header/File failures:** Missing required columns, 0-byte CSVs, header-only CSVs, and malformed quotes write `"status": "failed"` JSON under `reports/<stem>.json`.
- **Per-row failures:** A row may violate multiple rules; `reject_reason` joins them with `;`, and `reason_counts` counts occurrences per rule independently (not mutually exclusive row counts).
- **Outputs:** Valid rows write to `curated/orders/order_date=YYYY-MM-DD/` (Snappy Parquet), rejected rows write to `quarantine/orders/<stem>.csv`, and summary metrics write to `reports/<stem>.json`.

## Operating the Pipeline

### 1. Generate and Upload Synthetic Data
```powershell
.\.venv\Scripts\Activate.ps1
python .\scripts\generate_orders.py 2026-11-13 --rows 5000 --defect-rate 0.01
aws --no-cli-pager s3 cp .\data\orders_2026-11-13.csv s3://orders-etl-pipeline-2026-11-7f3a9c/raw/orders/ --profile etl-project --region ap-southeast-2
```

### 2. Register New Partitions in Glue / Athena
If a new `order_date` partition is added under `curated/orders/`, register it before querying via Athena or `orders-etl-insights`:
```sql
MSCK REPAIR TABLE orders_analytics_db.orders;
```
Or register specific dates with `ALTER TABLE orders_analytics_db.orders ADD IF NOT EXISTS PARTITION (order_date='2026-11-13') LOCATION 's3://orders-etl-pipeline-2026-11-7f3a9c/curated/orders/order_date=2026-11-13/';`.

### 3. Manually Trigger AI Insights for a Specific Date
```powershell
'{"date":"2026-11-11"}' | Out-File -Encoding ascii $env:TEMP\payload.json
aws --no-cli-pager lambda invoke --function-name orders-etl-insights --cli-binary-format raw-in-base64-out --payload file://$env:TEMP\payload.json $env:TEMP\out.json --profile etl-project --region ap-southeast-2
Get-Content $env:TEMP\out.json
```

### 4. Launch the Local Dashboard
```powershell
.\.venv\Scripts\Activate.ps1
streamlit run dashboard/app.py
```
Open `http://localhost:8501`.
