# Cost & Resource Audit — Serverless AWS ETL Pipeline

**Account:** `744792530652` | **Region:** `ap-southeast-2` (Sydney) | **Budget:** `aws-etl-monthly-cost-watch` (`$5.00 USD/month`, `HealthStatus: HEALTHY`)

> **Important Note on Billing:** `aws budgets describe-budgets` reported `ActualSpend: 0.0 USD` as of `2026-11-11T22:58:57+05:30` (before the overnight ingestion and Athena test runs; AWS billing metrics update with standard processing delay). Below are verified resource sizes and official AWS / Google pricing unit rates rather than fabricated invoice totals.

## 1. Audited AWS Resource Inventory

| Service | Resource / Identifier | Verified Configuration / Size |
| --- | --- | --- |
| **Amazon S3** | `orders-etl-pipeline-2026-11-7f3a9c` | `324` objects, `14,612,529` bytes (~`13.94 MiB`); SSE-S3 (`AES256`); Public Access Block `4/4` enabled |
| **AWS Lambda** | `orders-etl-ingest` | Python 3.14, `1024 MB`, `120s` timeout, layer `AWSSDKPandas-Python314:12` (~744 ms warm execution, ~283 MB max memory) |
| **AWS Lambda** | `orders-etl-insights` | Python 3.14, `512 MB`, `120s` timeout (~15.5s execution, ~114 MB max memory) |
| **AWS Glue** | `orders_analytics_db.orders` | 1 database, 1 External Parquet table, `33` registered `order_date` partitions (`2026-10-10`–`2026-11-11`) |
| **Amazon Athena** | `orders-etl-workgroup` | `100 MiB` (`104,857,600` bytes) per-query scan cutoff; `SSE_S3` enforced; `33-day` dashboard scan ~`4.7 MiB` total across 4 queries |
| **Amazon SNS** | `orders-etl-alerts`, `orders-etl-daily-insights` | 2 Standard topics, 1 confirmed email subscription each (`karthiktatineni34@gmail.com`) |
| **EventBridge Scheduler** | `orders-etl-daily-insights-schedule` | `cron(0 12 * * ? *)`, `Asia/Kolkata`, `State: ENABLED`, `30–31` invocations/month |
| **SSM Parameter Store** | `/orders-etl/gemini-api-key` | 1 Standard-tier `SecureString` parameter (`alias/aws/ssm` AWS-managed KMS key) |
| **CloudWatch Logs** | `/aws/lambda/orders-etl-ingest`, `/aws/lambda/orders-etl-insights` | 2 Standard log groups (currently no log retention policy configured) |
| **SQS / DLQ** | *(Deferred)* | Not created; $0 recurring or request cost |

## 2. Monthly Recurring Cost Estimate (Steady-State Daily Pipeline)

Assuming 1 daily CSV (~`300 KB` -> ~`160 KB` Parquet), 1 daily `orders-etl-insights` run, and ~5–10 local dashboard refreshes/month:

| Service | Official Pricing Model | Expected Monthly Usage | Estimated Monthly Cost |
| --- | --- | --- | --- |
| **AWS Lambda** | 1M requests + 400,000 GB-s/mo free forever; then `$0.20 / 1M req` | ~60 invocations, <250 GB-s | **$0.00** (within permanent free tier) |
| **Amazon S3** | ~`$0.023 / GB-month` (`ap-southeast-2` Standard), `$0.005 / 1k PUT` | ~`15–25 MiB` total stored; <200 API calls/mo | **< $0.01** |
| **Amazon Athena** | `$5.00 / TB` scanned (10 MB minimum per billed SELECT query); DDL free | ~30 daily insights runs + ~10 dashboard views (10 MB min/query × ~130 queries ≈ ~`1.3 GB`) | **~$0.01 – $0.05** |
| **AWS Glue Catalog** | First 1M objects & 1M requests/month free | 1 table, ~33–65 partitions, <5k requests | **$0.00** (within free tier) |
| **Amazon SNS** | First 1M publishes free; email deliveries `$2.00 / 100k` | ~30–35 emails/month | **< $0.01** |
| **EventBridge Scheduler** | 14M invocations/month free tier; then `$1.00 / 1M` | ~30 invocations/month | **$0.00** (within free tier) |
| **SSM Parameter Store** | Standard parameters free; AWS-managed `alias/aws/ssm` KMS calls free within standard quotas | 1 Standard `SecureString`, ~30 `GetParameter` calls/mo | **$0.00** |
| **CloudWatch Logs** | 5 GB ingestion/archive free tier/month | <1 MB/month | **$0.00** (within free tier) |
| **Google Gemini API** | `gemini-3-flash-preview`: Free tier available in Google AI Studio (Paid tier: `$0.50 / 1M` input tokens, `$3.00 / 1M` output tokens) | 1 prompt/day (~1k input, ~600 output tokens ≈ ~50k tokens/mo) | **$0.00** (Free tier) or **~$0.07** if Paid tier |

**Estimated Total Steady-State Monthly Run Rate:** **~$0.02 – $0.10 USD/month** (well within the `$5.00 USD` monthly budget).

## 3. Cost Risk & Hygiene Recommendations

1. **No always-on infrastructure deployed:** Zero EC2 instances, NAT Gateways, RDS instances, load balancers, or public cloud app hosting.
2. **Athena query scan protection:** All Athena queries require a partition predicate on `order_date`, the workgroup enforces a `100 MiB` cutoff (`104,857,600` bytes), and Streamlit caches results for `300s`.
3. **CloudWatch log retention (optional hygiene):** Both Lambda log groups currently have unlimited retention (`Never expire`). Setting a 30-day retention policy (`aws logs put-retention-policy --retention-in-days 30`) prevents long-term log accumulation.
4. **S3 lifecycle cleanup (optional):** `athena-results/` accumulates small query output CSV/metadata files over time; an optional 30-day expiration lifecycle rule on `athena-results/` keeps S3 clean.
5. **Gemini API Free Tier privacy caveat:** Under Google AI Studio's free tier, inputs/outputs may be used by Google per free-tier terms. Because this pipeline passes only **aggregated synthetic metrics** (never real customer PII), free-tier usage is appropriate for this portfolio demonstration.
