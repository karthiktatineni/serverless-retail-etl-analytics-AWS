# Deployment Guide — Serverless AWS ETL Pipeline

## Prerequisites

- AWS Account `744792530652`, Region `ap-southeast-2` (Sydney), AWS CLI v2 configured with profile `etl-project` (browser-based `aws login`, no permanent IAM user access keys).
- Python `3.14` with `.venv` created at project root.
- A Google AI Studio API key stored locally (never committed) for SSM `SecureString` upload.

## 1. Core Storage, Catalog & Workgroup

- **S3 Bucket:** `orders-etl-pipeline-2026-11-7f3a9c` with Block Public Access enabled (`4/4` flags `true`) and default `AES256` (SSE-S3) encryption.
- **Athena Workgroup:** `orders-etl-workgroup` enforcing output location `s3://orders-etl-pipeline-2026-11-7f3a9c/athena-results/`, `SSE_S3`, and `104,857,600` bytes (`100 MiB`) scan cutoff.
- **Glue Catalog:** Database `orders_analytics_db`, External Parquet table `orders` partitioned by `order_date`.

## 2. Deploy Ingestion Lambda (`orders-etl-ingest`)

1. Create `orders-etl-alerts` Standard SNS topic and subscribe your alert email; confirm subscription via email.
2. Create IAM role `orders-etl-lambda-ingest-role` using [`docs/lambda_ingest_trust_policy.json`](lambda_ingest_trust_policy.json), attach `AWSLambdaBasicExecutionRole`, managed policy `orders-etl-ingest-s3-policy` ([`docs/lambda_ingest_s3_policy.json`](lambda_ingest_s3_policy.json)), and inline policy `orders-etl-ingest-sns-policy` ([`docs/lambda_ingest_sns_policy.json`](lambda_ingest_sns_policy.json)).
3. Package and update code:
   ```powershell
   Compress-Archive -Path .\src\lambda_ingest\lambda_function.py -DestinationPath .\lambda_ingest.zip -Force
   aws --no-cli-pager lambda update-function-code --function-name orders-etl-ingest --zip-file fileb://lambda_ingest.zip --profile etl-project --region ap-southeast-2
   ```
4. Configure runtime `python3.14`, handler `lambda_function.handler`, memory `1024 MB`, timeout `120s`, layer `AWSSDKPandas-Python314:12`, and env vars `REJECT_RATE_THRESHOLD=0.10`, `SNS_TOPIC_ARN=arn:aws:sns:ap-southeast-2:744792530652:orders-etl-alerts`.
5. Keep S3 notification `OrdersCsvIngestTrigger` ([`docs/s3_notification.json`](s3_notification.json)) scoped to prefix `raw/orders/` and suffix `.csv`.

## 3. Store Gemini API Key in SSM Parameter Store

Store the key via Python `boto3` (`SecureString` encrypted with `alias/aws/ssm`) so the key never enters shell history or logs:
```powershell
.\.venv\Scripts\python.exe -c "import getpass, boto3; key = getpass.getpass('Gemini API Key: ').strip(); boto3.Session(profile_name='etl-project', region_name='ap-southeast-2').client('ssm').put_parameter(Name='/orders-etl/gemini-api-key', Value=key, Type='SecureString', Overwrite=True)"
```

## 4. Deploy AI Insights Lambda (`orders-etl-insights`)

1. Create `orders-etl-daily-insights` SNS topic and subscribe your daily report email; confirm subscription via email.
2. Create IAM role `orders-etl-insights-role` using [`docs/lambda_insights_trust_policy.json`](lambda_insights_trust_policy.json), attach `AWSLambdaBasicExecutionRole`, and attach inline policy `orders-etl-insights-policy` ([`docs/lambda_insights_policy.json`](lambda_insights_policy.json)).
3. Package and deploy:
   ```powershell
   Compress-Archive -Path .\src\lambda_insights\lambda_function.py -DestinationPath .\lambda_insights.zip -Force
   aws --no-cli-pager lambda update-function-code --function-name orders-etl-insights --zip-file fileb://lambda_insights.zip --profile etl-project --region ap-southeast-2
   ```
4. Configure runtime `python3.14`, handler `lambda_function.handler`, memory `512 MB`, timeout `120s`, and env vars:
   - `S3_BUCKET=orders-etl-pipeline-2026-11-7f3a9c`
   - `ATHENA_DATABASE=orders_analytics_db`
   - `ATHENA_TABLE=orders`
   - `ATHENA_WORKGROUP=orders-etl-workgroup`
   - `INSIGHTS_PREFIX=reports/insights/`
   - `QUALITY_REPORT_PREFIX=reports/`
   - `GEMINI_MODEL=gemini-3-flash-preview`
   - `SSM_GEMINI_PARAM_NAME=/orders-etl/gemini-api-key`
   - `SNS_TOPIC_ARN=arn:aws:sns:ap-southeast-2:744792530652:orders-etl-daily-insights`

## 5. EventBridge Daily Scheduler

1. Create `orders-etl-scheduler-role` ([`docs/scheduler_trust_policy.json`](scheduler_trust_policy.json) + [`docs/scheduler_invoke_policy.json`](scheduler_invoke_policy.json)).
2. Create `orders-etl-daily-insights-schedule`:
   - Expression: `cron(0 12 * * ? *)`
   - Timezone: `Asia/Kolkata`
   - Target: `orders-etl-insights`, Payload `{}`
   - RetryPolicy: `MaximumRetryAttempts=2`, `MaximumEventAgeInSeconds=3600`
   - Enable only after manual Lambda & SNS verification succeed.

## 6. Local Dashboard Setup

```powershell
Copy-Item .env.example .env
.\.venv\Scripts\Activate.ps1
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

## 7. Deploying Safely on Render (Optional Web Hosting)

The repository includes [`render.yaml`](../render.yaml) and a strictly read-only IAM policy template [`docs/render_readonly_policy.json`](render_readonly_policy.json) so the dashboard can run on Render while keeping AWS usage inside your `$5` budget:

1. **Read-Only Credentials:** Create a dedicated IAM user/role with **only** [`docs/render_readonly_policy.json`](render_readonly_policy.json) (no `PutObject`/`DeleteObject` on `raw/` or `curated/`, no Lambda/SNS/SSM/IAM permissions) and keep the secret key only in Render's Environment Variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
2. **Budget Guardrails Built Into the App:**
   - `DASHBOARD_CACHE_TTL_SECONDS=3600`: Caches Glue partitions, Athena queries, quality JSON, and AI insights for **1 hour** per instance so repeat views do not re-query Athena.
   - `DASHBOARD_PASSCODE`: When set in Render's secret Environment Variables, no AWS/Athena requests execute until the visitor enters the passcode, preventing bots or strangers from scanning Athena.
   - `Refresh Data` enforces a 60-second cooldown, and `orders-etl-workgroup` enforces the `100 MiB` per-query scan limit.
3. **Render Service Settings (via [`render.yaml`](../render.yaml)):**
   - **Build Command:** `pip install -r dashboard/requirements.txt`
   - **Start Command:** `streamlit run dashboard/app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true`
   - **Health Check Path:** `/_stcore/health`
   - **Environment Variables:** `PYTHON_VERSION=3.12.8`, `AWS_PROFILE=""`, `AWS_REGION=ap-southeast-2`, `DASHBOARD_CACHE_TTL_SECONDS=3600`, plus secrets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `DASHBOARD_PASSCODE`.
