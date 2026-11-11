# Dashboard Build Report — Orders Analytics Dashboard

> **Simulated Data Disclosure:** All order records, customer identifiers, quality reports, AI summaries, and revenue totals shown by this dashboard are generated from synthetic test datasets created for an ETL pipeline demonstration.

## 1. What Was Built

Built a locally running interactive **Streamlit** analytics, data-quality, and AI-insights presentation dashboard (`Orders Analytics Dashboard`) over the existing AWS ETL pipeline (`orders-etl-pipeline-2026-11-7f3a9c`, `orders_analytics_db.orders`, and `orders-etl-workgroup`):

- Top-level simulated-data disclosure banner.
- Clean sidebar (`Controls & Filters`) with **Refresh Data** (`@st.cache_data` clear + rerun) and Glue `order_date` partition-bounded date-range selector (`33` partitions spanning `2026-10-10` to `2026-11-11`). Technical debug metadata was intentionally removed from the sidebar per user request.
- **Panel 1 — KPI Cards:** Total Orders, Total Revenue, Average Order Value, and Overall Rejection Rate (`SUM(rejected_rows) / SUM(total_rows) * 100`).
- **Panel 2 — Daily Sales Performance:** Interactive Plotly line charts for daily total orders and daily total revenue.
- **Panel 3 — Revenue by Category:** Interactive Plotly bar chart.
- **Panel 4 — Top 10 Customers by Revenue:** Interactive Plotly horizontal bar chart labeled with synthetic customer IDs (`Sim ID ...`) and no invented product attributes.
- **Panel 5 — Daily Data Quality Trend:** Daily rejection percentage from S3 `reports/orders_YYYY-MM-DD.json`.
- **Panel 6 — Validation-Failure Occurrences by Reason:** Aggregated `reason_counts` bar chart labeled as independent rule failures rather than mutually exclusive rejected rows.
- **Panel 7 — Quality Report Coverage:** Selected dates, reports found, missing reports, total input rows, total valid rows, and total rejected rows.
- **Panel 8 — AI-Generated Business Insights:** Read-only panel reading the latest valid `reports/insights/orders_insights_YYYY-MM-DD.json` from S3, displaying report date, Gemini model, formatted UTC timestamp, executive summary, observations, actionable recommendations, opportunities, and caveats, with missing/malformed/stale report detection aligned to completed `Asia/Kolkata` days.

## 2. Files Created or Modified

- [`dashboard/athena.py`](../dashboard/athena.py): `boto3` session (`etl-project`, `ap-southeast-2`), STS check, Glue `get_partitions`, partition-filtered Athena execution with pagination and 300s cache.
- [`dashboard/quality.py`](../dashboard/quality.py): Paginated S3 `reports/orders_YYYY-MM-DD.json` reader, JSON validation, coverage metrics, and 300s cache.
- [`dashboard/insights.py`](../dashboard/insights.py): Paginated S3 `reports/insights/orders_insights_YYYY-MM-DD.json` loader with IST completed-day freshness checks (`min(latest_partition_date, today_ist - 1 day)` and 36h age check) and 300s cache.
- [`dashboard/app.py`](../dashboard/app.py): Streamlit UI covering all 8 panels, responsive metric typography, clean error handling, and cache refresh.
- [`dashboard/requirements.txt`](../dashboard/requirements.txt): `streamlit==1.67.0`, `boto3[crt]==1.44.0`, `pandas==3.1.1`, `plotly==7.1.0`, `python-dotenv==1.2.4`.
- [`.streamlit/config.toml`](../.streamlit/config.toml): Light theme styling.
- [`.env.example`](../.env.example), [`tests/test_dashboard.py`](../tests/test_dashboard.py), [`README.md`](../README.md), [`docs/screenshots/dashboard_full.png`](screenshots/dashboard_full.png), and [`docs/screenshots/dashboard_insights.png`](screenshots/dashboard_insights.png).

## 3. Actual AWS Services Accessed

- **AWS STS:** `get_caller_identity` (`744792530652`, `AccountFullAccessRole`).
- **AWS Glue:** `get_partitions` on `orders_analytics_db.orders` (`33` partitions: `2026-10-10` to `2026-11-11`).
- **Amazon Athena:** `orders-etl-workgroup` with `WHERE order_date BETWEEN ... AND ...` predicates on every `SELECT`.
- **Amazon S3:** Read-only `list_objects_v2` and `get_object` on `reports/orders_*.json` and `reports/insights/orders_insights_*.json`.

## 4. Actual Athena Queries & Verified KPI Results

- **2-Day Baseline (`2026-11-10` to `2026-11-11`):**
  - `2026-11-10` (`qid=36c46379-d229-4173-a6fa-4bcdd9bc95e1`): `4,800` orders | `1,839,616.39` revenue | `383.25` AOV
  - `2026-11-11` (`qid=666c865a-ce16-4d60-898b-932876b212d2`): `4,800` orders | `1,868,497.74` revenue | `389.27` AOV
  - Combined (`qid=ee905aa1-2cf1-4433-b69b-98ed449aaecd`): `9,600` orders | `3,708,114.13` revenue | `386.26` AOV
- **Full 33-Day Dataset (`2026-10-10` to `2026-11-11`):**
  - KPIs (`qid=d8952504-b9c4-4563-96b4-382a705c2e03`): `144,175` orders | `55,004,231.69` revenue | `381.51` AOV
  - Categories (`qid=b69ce8ef-99df-4d0f-b56a-19d472c5dc02`): `Electronics` (`9,195,314.36`), `Fashion` (`9,194,750.45`), `Beauty` (`9,189,721.47`), `Home` (`9,161,769.49`), `Books` (`9,137,437.45`), `Sports` (`9,125,238.47`)

## 5. Quality Report Coverage & AI Insights Display

- **33-Day Coverage (`2026-10-10` to `2026-11-11`):** `33` selected dates, `33` reports found, `0` missing, `151,590` input rows, `144,175` valid rows, `7,415` rejected rows, `4.8915%` (`4.89%` displayed) overall rejection rate.
- **AI Insights Panel:** Displays real S3 object `reports/insights/orders_insights_2026-11-11.json` (`gemini-3-flash-preview`, `2026-11-11 20:49:44 UTC`) with zero stale warning even when a `2026-11-12` test partition exists on `2026-11-12` IST.

## 6. Tests & Errors Encountered

1. **`MissingDependencyException` for AWS CLI `login` provider:** Installed `boto3[crt]==1.44.0` (`awscrt==0.38.0`) and pinned it in `dashboard/requirements.txt`.
2. **Streamlit `use_container_width` deprecation warnings:** Updated all calls to `width="stretch"`.
3. **Hot-reload `DashboardConfig` cache mismatch:** Added resilient `getattr` fallback in `dashboard/insights.py` and restarted the local Streamlit process.
4. **Metric truncation at `1280px` width:** Added responsive `clamp(...)` font sizing in `dashboard/app.py` and verified cleanly at `1440x900` in Playwright Chromium (`0` console errors).
5. **Freshness edge case with `2026-11-12` test partition:** Fixed `dashboard/insights.py` to compare against `min(latest_partition_date, today_ist - 1 day)` and added a dedicated regression test.

## 7. Startup Status, Athena Bytes & Security

- **Local Server:** Running at `http://localhost:8501` (`/_stcore/health` returned `200 ok`).
- **Athena Bytes Scanned:** `483,374` bytes (`472.04 KiB`) for the initial 2-day view; `1,870,705` bytes (`KPIs`) + `966,173` bytes (`Category`) for the 33-day view.
- **Security & Hosting:** Local only (`http://localhost:8501`), uses temporary AWS CLI profile `etl-project` (no static IAM keys), `.env` excluded by `.gitignore`. Public Render hosting is **not recommended** without OIDC/IAM refactoring and authentication because Render lacks `~/.aws` CLI login sessions and public refresh clicks could incur Athena charges against the $5 budget.
