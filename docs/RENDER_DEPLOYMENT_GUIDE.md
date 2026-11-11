# Render Deployment Guide — Orders Analytics Dashboard

This guide walks through deploying the local Streamlit dashboard (`dashboard/app.py`) safely to **Render** while keeping AWS costs well within your **$5/month budget** and avoiding Administrator credentials in the cloud.

---

## Before You Start: Security & Cost Architecture

| Safeguard | How It Protects Your AWS Account |
| --- | --- |
| **Read-Only IAM Policy** | Uses [`docs/render_readonly_policy.json`](render_readonly_policy.json) — grants **no** write/delete access to `raw/`, `curated/`, Lambda, SNS, or SSM. |
| **Passcode Gate (`DASHBOARD_PASSCODE`)** | Zero STS, Glue, Athena, or S3 calls run until the visitor enters your passcode. |
| **1-Hour Query Cache (`ttl=3600`)** | `DASHBOARD_CACHE_TTL_SECONDS=3600` caches partitions, Athena queries, quality reports, and AI insights for 60 minutes per running instance. |
| **60-Second Refresh Cooldown** | Prevents rapid repeated clicks on **Refresh Data**. |
| **Athena 100 MiB Scan Cutoff** | Workgroup `orders-etl-workgroup` enforces partition filtering and a `100 MiB` per-query limit. |

---

## Step 1: Create a Strictly Read-Only IAM Credential in AWS

> Never reuse your personal admin login or put keys in Git, code, or `.env`. Create a dedicated read-only user just for Render.

1. Open the **AWS Console** → search for **IAM**.
2. **Create the policy:**
   - Left menu → **Policies** → **Create policy**.
   - Switch to the **JSON** tab.
   - Paste the exact contents of [`docs/render_readonly_policy.json`](render_readonly_policy.json).
   - Click **Next**, name it `orders-etl-render-readonly-policy`, and click **Create policy**.
3. **Create the user:**
   - Left menu → **Users** → **Create user**.
   - User name: `orders-etl-render-readonly`.
   - Leave **"Provide user access to the AWS Management Console"** **unchecked** → **Next**.
   - Choose **Attach policies directly** → search and check **only** `orders-etl-render-readonly-policy` → **Next** → **Create user**.
4. **Generate the access key:**
   - Click on user `orders-etl-render-readonly` → **Security credentials** tab.
   - Under **Access keys**, click **Create access key**.
   - Select **Application running outside AWS** → **Next** → **Create access key**.
   - Keep this tab open (or copy **Access key** and **Secret access key** directly into Render in Step 2).

---

## Step 2: Deploy on Render

### Option A: One-Click Blueprint Deployment (Recommended)

This repository includes [`render.yaml`](../render.yaml), which configures everything automatically.

1. Sign in to [Render](https://dashboard.render.com/).
2. Click **New +** → **Blueprint**.
3. Connect and select your GitHub repository: `karthiktatineni/serverless-retail-etl-analytics-AWS` (branch `main`).
4. Render detects `render.yaml` (`orders-analytics-dashboard`) and prompts you for the 3 `sync: false` secrets:
   - **`AWS_ACCESS_KEY_ID`**: paste the Access key from Step 1.
   - **`AWS_SECRET_ACCESS_KEY`**: paste the Secret access key from Step 1.
   - **`DASHBOARD_PASSCODE`**: enter any strong password/passcode you want visitors to type before viewing the dashboard.
5. Click **Apply** and wait ~2–3 minutes for dependencies (`dashboard/requirements.txt`) to install and Streamlit to start.

---

### Option B: Manual Web Service Setup (If Not Using Blueprint)

1. In Render, click **New +** → **Web Service** → select `karthiktatineni/serverless-retail-etl-analytics-AWS`.
2. Fill in the service settings:
   - **Name:** `orders-analytics-dashboard`
   - **Region:** Singapore or Oregon (either works; AWS calls still route to `ap-southeast-2`)
   - **Branch:** `main`
   - **Language / Runtime:** `Python 3`
   - **Build Command:**
     ```bash
     pip install -r dashboard/requirements.txt
     ```
   - **Start Command:**
     ```bash
     streamlit run dashboard/app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true
     ```
   - **Instance Type:** `Free`
   - *(Optional under Advanced)* **Health Check Path:** `/_stcore/health`
3. Under **Environment Variables**, add every key below:

| Key | Value |
| --- | --- |
| `PYTHON_VERSION` | `3.12.8` |
| `AWS_REGION` | `ap-southeast-2` |
| `AWS_PROFILE` | *(leave completely empty — do NOT put `etl-project`)* |
| `S3_BUCKET` | `orders-etl-pipeline-2026-11-7f3a9c` |
| `ATHENA_DATABASE` | `orders_analytics_db` |
| `ATHENA_TABLE` | `orders` |
| `ATHENA_WORKGROUP` | `orders-etl-workgroup` |
| `ATHENA_OUTPUT_LOCATION` | `s3://orders-etl-pipeline-2026-11-7f3a9c/athena-results/` |
| `QUALITY_REPORT_PREFIX` | `reports/` |
| `INSIGHTS_PREFIX` | `reports/insights/` |
| `DASHBOARD_CACHE_TTL_SECONDS` | `3600` |
| `AWS_ACCESS_KEY_ID` | *(from Step 1)* |
| `AWS_SECRET_ACCESS_KEY` | *(from Step 1)* |
| `DASHBOARD_PASSCODE` | *(your chosen dashboard password)* |

4. Click **Deploy Web Service**.

---

## Step 3: Verify Deployment

1. Once Render shows **Live**, open `https://<your-service-name>.onrender.com/_stcore/health` — it should return `ok`.
2. Open `https://<your-service-name>.onrender.com/`.
3. Confirm the **Access Verification** screen appears before loading any AWS data.
4. Enter your `DASHBOARD_PASSCODE` and click **Unlock Dashboard**.
5. Verify:
   - Sidebar shows `Query cache TTL: 60 min` and `Glue partitions found: 33 (2026-10-10 to 2026-11-11)`.
   - KPI cards display `144,175` orders, `55,004,231.69` revenue, `381.51` AOV, and `4.89%` rejection rate.
   - **AI-Generated Business Insights** displays the `2026-11-11` report from S3.

---

## Troubleshooting

| Symptom on Render | Cause | Fix |
| --- | --- | --- |
| `ProfileNotFound` for `etl-project` | `AWS_PROFILE` was set to `etl-project` instead of blank | In Render → **Environment**, set `AWS_PROFILE` to empty (`""`) or delete it. |
| `InvalidClientTokenId` / `SignatureDoesNotMatch` | Extra whitespace or typo in IAM keys | Re-paste `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` with no leading/trailing spaces and trigger **Manual Deploy**. |
| `AccessDenied` on Athena or S3 | Policy missing a prefix or action | Verify the IAM user uses the exact JSON from [`docs/render_readonly_policy.json`](render_readonly_policy.json). |
| App takes 30–50 seconds to wake up | Render Free Tier sleeps after 15 minutes of inactivity | Normal Free Tier behavior; wait for container spin-up. |

---

## Decommissioning / Key Rotation

When you no longer need the public Render demo:
1. Suspend or delete the service in **Render Dashboard**.
2. In **AWS IAM → Users → `orders-etl-render-readonly` → Security credentials**, deactivate and delete the access key (or delete the IAM user) so zero permanent keys remain in your AWS account.
