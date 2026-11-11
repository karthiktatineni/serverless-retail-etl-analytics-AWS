import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

IST = ZoneInfo("Asia/Kolkata")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_WORKGROUP_RE = re.compile(r"^[A-Za-z0-9_-]+$")

_SSM_CACHE: dict[str, str] = {}

INSIGHTS_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "executive_summary": {"type": "STRING"},
        "numerical_observations": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "revenue_and_order_trends": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "category_performance": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "customer_concentration": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "opportunities": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "recommendations": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "limitations_and_caveats": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
    },
    "required": [
        "executive_summary",
        "numerical_observations",
        "revenue_and_order_trends",
        "category_performance",
        "customer_concentration",
        "opportunities",
        "recommendations",
        "limitations_and_caveats",
    ],
}


class AthenaExecutionError(RuntimeError):
    """Raised when an Athena query fails, is cancelled, or times out."""


class GeminiIntegrationError(RuntimeError):
    """Raised when the Gemini API call fails or returns invalid output."""


def _validate_identifier(
    value: str,
    label: str,
    pattern: re.Pattern[str] = _IDENTIFIER_RE,
) -> str:
    if not value or not pattern.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def _validate_date_str(date_str: str) -> str:
    if not isinstance(date_str, str) or not _DATE_RE.match(date_str.strip()):
        raise ValueError(f"Date must use YYYY-MM-DD format: got {date_str!r}")
    cleaned = date_str.strip()
    datetime.strptime(cleaned, "%Y-%m-%d")
    return cleaned


def resolve_report_date(
    event: dict[str, Any] | None,
    now_ist: datetime | None = None,
) -> str:
    if event:
        explicit = event.get("report_date") or event.get("date")
        if explicit:
            return _validate_date_str(str(explicit))

    current_ist = now_ist or datetime.now(IST)
    yesterday_ist = (current_ist.astimezone(IST).date() - timedelta(days=1)).isoformat()
    return yesterday_ist


def discover_partitions(
    glue_client: Any,
    database: str,
    table: str,
) -> list[str]:
    paginator = glue_client.get_paginator("get_partitions")
    dates: set[str] = set()
    for page in paginator.paginate(DatabaseName=database, TableName=table):
        for partition in page.get("Partitions", []):
            values = partition.get("Values") or []
            if not values:
                continue
            raw = str(values[0]).strip()
            if _DATE_RE.match(raw):
                try:
                    datetime.strptime(raw, "%Y-%m-%d")
                    dates.add(raw)
                except ValueError:
                    continue
    return sorted(dates)


def execute_athena_query(
    athena_client: Any,
    sql: str,
    database: str,
    workgroup: str,
    timeout_seconds: int = 60,
    poll_interval: float = 1.0,
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    if "where" not in sql.lower() or "order_date" not in sql.lower():
        raise ValueError("All Athena queries must filter on order_date.")

    start_resp = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_id = start_resp["QueryExecutionId"]

    deadline = time.monotonic() + timeout_seconds
    execution: dict[str, Any] = {}

    while True:
        resp = athena_client.get_query_execution(QueryExecutionId=query_id)
        execution = resp["QueryExecution"]
        status = execution["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No failure reason provided.")
            raise AthenaExecutionError(
                f"Athena query {query_id} {state.lower()}: {reason}"
            )
        if time.monotonic() >= deadline:
            try:
                athena_client.stop_query_execution(QueryExecutionId=query_id)
            except Exception:
                pass
            raise AthenaExecutionError(
                f"Athena query {query_id} timed out after {timeout_seconds}s."
            )
        time.sleep(poll_interval)

    paginator = athena_client.get_paginator("get_query_results")
    columns: list[str] = []
    rows: list[dict[str, Any]] = []

    for page_index, page in enumerate(paginator.paginate(QueryExecutionId=query_id)):
        result_set = page.get("ResultSet", {})
        if page_index == 0:
            col_info = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
            columns = [c.get("Label") or c.get("Name") for c in col_info]

        raw_rows = result_set.get("Rows", [])
        data_rows = raw_rows[1:] if page_index == 0 else raw_rows

        for raw_row in data_rows:
            cells = raw_row.get("Data", [])
            values = [
                cell.get("VarCharValue") if isinstance(cell, dict) else None
                for cell in cells
            ]
            if len(values) < len(columns):
                values.extend([None] * (len(columns) - len(values)))
            rows.append(dict(zip(columns, values[: len(columns)])))

    stats = execution.get("Statistics", {})
    return rows, query_id, stats


def _to_int(val: Any) -> int:
    if val is None:
        return 0
    return int(float(val))


def _to_float(val: Any) -> float:
    if val is None:
        return 0.0
    return round(float(val), 2)


def _pct_change(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return round(((current - previous) / previous) * 100.0, 2)


def _read_quality_report(
    s3_client: Any,
    bucket: str,
    prefix: str,
    report_date: str,
) -> dict[str, Any]:
    normalized_prefix = prefix if not prefix or prefix.endswith("/") else f"{prefix}/"
    key = f"{normalized_prefix}orders_{report_date}.json"
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        payload = json.loads(resp["Body"].read().decode("utf-8"))
        return {
            "available": True,
            "s3_key": key,
            "status": payload.get("status"),
            "total_rows": payload.get("total_rows"),
            "valid_rows": payload.get("valid_rows"),
            "rejected_rows": payload.get("rejected_rows"),
            "reject_rate": payload.get("reject_rate"),
            "reason_counts": payload.get("reason_counts"),
        }
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        return {"available": False, "s3_key": key, "reason": code}
    except (ValueError, UnicodeDecodeError) as exc:
        return {"available": False, "s3_key": key, "reason": f"Invalid JSON: {exc}"}


def fetch_analytics_bundle(
    athena_client: Any,
    s3_client: Any,
    database: str,
    table: str,
    workgroup: str,
    bucket: str,
    quality_prefix: str,
    report_date: str,
    available_partitions: list[str],
) -> dict[str, Any]:
    previous_dates = [d for d in available_partitions if d < report_date]
    previous_date = previous_dates[-1] if previous_dates else None

    target_dates = [previous_date, report_date] if previous_date else [report_date]
    dates_sql = ", ".join(f"'{d}'" for d in target_dates)

    kpis_sql = (
        "SELECT\n"
        "    order_date,\n"
        "    COUNT(DISTINCT order_id) AS total_orders,\n"
        "    COUNT(DISTINCT customer_id) AS unique_customers,\n"
        "    SUM(quantity) AS units_sold,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,\n"
        "    ROUND(\n"
        "        SUM(quantity * unit_price) / NULLIF(COUNT(DISTINCT order_id), 0),\n"
        "        2\n"
        "    ) AS average_order_value\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date IN ({dates_sql})\n"
        "GROUP BY order_date\n"
        "ORDER BY order_date;"
    )

    category_sql = (
        "SELECT\n"
        "    category,\n"
        "    COUNT(DISTINCT order_id) AS total_orders,\n"
        "    SUM(quantity) AS units_sold,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS revenue\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date = '{report_date}'\n"
        "GROUP BY category\n"
        "ORDER BY revenue DESC;"
    )

    customers_sql = (
        "SELECT\n"
        "    customer_id,\n"
        "    COUNT(DISTINCT order_id) AS total_orders,\n"
        "    SUM(quantity) AS units_sold,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS revenue\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date = '{report_date}'\n"
        "GROUP BY customer_id\n"
        "ORDER BY revenue DESC\n"
        "LIMIT 10;"
    )

    kpi_rows, kpi_qid, _ = execute_athena_query(athena_client, kpis_sql, database, workgroup)
    cat_rows, cat_qid, _ = execute_athena_query(athena_client, category_sql, database, workgroup)
    cust_rows, cust_qid, _ = execute_athena_query(athena_client, customers_sql, database, workgroup)

    by_date = {row["order_date"]: row for row in kpi_rows if row.get("order_date")}
    current_row = by_date.get(report_date)
    if not current_row or _to_int(current_row.get("total_orders")) == 0:
        return {
            "has_data": False,
            "query_ids": {
                "kpis": kpi_qid,
                "categories": cat_qid,
                "top_customers": cust_qid,
            },
        }

    kpis = {
        "total_orders": _to_int(current_row.get("total_orders")),
        "unique_customers": _to_int(current_row.get("unique_customers")),
        "units_sold": _to_int(current_row.get("units_sold")),
        "total_revenue": _to_float(current_row.get("total_revenue")),
        "average_order_value": _to_float(current_row.get("average_order_value")),
    }

    previous_comparison = None
    prev_row = by_date.get(previous_date) if previous_date else None
    if prev_row and _to_int(prev_row.get("total_orders")) > 0:
        prev_kpis = {
            "total_orders": _to_int(prev_row.get("total_orders")),
            "unique_customers": _to_int(prev_row.get("unique_customers")),
            "units_sold": _to_int(prev_row.get("units_sold")),
            "total_revenue": _to_float(prev_row.get("total_revenue")),
            "average_order_value": _to_float(prev_row.get("average_order_value")),
        }
        previous_comparison = {
            "previous_date": previous_date,
            "previous_kpis": prev_kpis,
            "deltas": {
                "total_orders_diff": kpis["total_orders"] - prev_kpis["total_orders"],
                "total_orders_pct_change": _pct_change(
                    kpis["total_orders"], prev_kpis["total_orders"]
                ),
                "unique_customers_diff": kpis["unique_customers"] - prev_kpis["unique_customers"],
                "unique_customers_pct_change": _pct_change(
                    kpis["unique_customers"], prev_kpis["unique_customers"]
                ),
                "units_sold_diff": kpis["units_sold"] - prev_kpis["units_sold"],
                "units_sold_pct_change": _pct_change(
                    kpis["units_sold"], prev_kpis["units_sold"]
                ),
                "total_revenue_diff": round(
                    kpis["total_revenue"] - prev_kpis["total_revenue"], 2
                ),
                "total_revenue_pct_change": _pct_change(
                    kpis["total_revenue"], prev_kpis["total_revenue"]
                ),
                "average_order_value_diff": round(
                    kpis["average_order_value"] - prev_kpis["average_order_value"], 2
                ),
                "average_order_value_pct_change": _pct_change(
                    kpis["average_order_value"], prev_kpis["average_order_value"]
                ),
            },
        }

    total_rev = kpis["total_revenue"]
    categories = []
    for row in cat_rows:
        rev = _to_float(row.get("revenue"))
        share = round((rev / total_rev) * 100.0, 2) if total_rev > 0 else 0.0
        categories.append(
            {
                "category": row.get("category"),
                "total_orders": _to_int(row.get("total_orders")),
                "units_sold": _to_int(row.get("units_sold")),
                "revenue": rev,
                "revenue_share_pct": share,
            }
        )

    top_10_rev_sum = round(sum(_to_float(r.get("revenue")) for r in cust_rows), 2)
    top_10_orders_sum = sum(_to_int(r.get("total_orders")) for r in cust_rows)
    top_1_rev = _to_float(cust_rows[0].get("revenue")) if cust_rows else 0.0
    customer_concentration = {
        "top_10_customer_count": len(cust_rows),
        "top_1_simulated_customer_id": cust_rows[0].get("customer_id") if cust_rows else None,
        "top_1_revenue": top_1_rev,
        "top_1_revenue_share_pct": round((top_1_rev / total_rev) * 100.0, 2)
        if total_rev > 0
        else 0.0,
        "top_10_revenue_sum": top_10_rev_sum,
        "top_10_revenue_share_pct": round((top_10_rev_sum / total_rev) * 100.0, 2)
        if total_rev > 0
        else 0.0,
        "top_10_orders_sum": top_10_orders_sum,
        "top_10_orders_share_pct": round(
            (top_10_orders_sum / kpis["total_orders"]) * 100.0, 2
        )
        if kpis["total_orders"] > 0
        else 0.0,
    }

    quality_summary = _read_quality_report(s3_client, bucket, quality_prefix, report_date)

    return {
        "has_data": True,
        "query_ids": {
            "kpis": kpi_qid,
            "categories": cat_qid,
            "top_customers": cust_qid,
        },
        "kpis": kpis,
        "previous_day_comparison": previous_comparison,
        "category_breakdown": categories,
        "customer_concentration": customer_concentration,
        "quality_summary": quality_summary,
    }


def get_gemini_api_key(ssm_client: Any, param_name: str) -> str:
    if param_name in _SSM_CACHE:
        return _SSM_CACHE[param_name]

    resp = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
    value = resp.get("Parameter", {}).get("Value", "").strip()
    if not value:
        raise GeminiIntegrationError(
            f"SSM parameter {param_name!r} returned an empty value."
        )
    _SSM_CACHE[param_name] = value
    return value


def generate_gemini_insights(
    api_key: str,
    model: str,
    metrics_payload: dict[str, Any],
    timeout_seconds: int = 35,
    max_attempts: int = 3,
) -> dict[str, Any]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    prompt = (
        "You are a retail analytics analyst writing a daily executive intelligence briefing.\n"
        "STRICT RULES:\n"
        "1. Use ONLY the verified aggregated metrics supplied in JSON below.\n"
        "2. Do NOT invent, estimate, or extrapolate any numbers not explicitly provided.\n"
        "3. State clearly that the dataset is simulated/synthetic retail data and customer IDs are synthetic.\n"
        "4. Distinguish observed facts from interpretations; do not claim causation without evidence.\n"
        "5. Only reference day-over-day changes if 'previous_day_comparison' is non-null; otherwise note comparison data is unavailable.\n"
        "6. Base every recommendation directly on the observed numbers and note limitations/caveats (such as lack of product/margin/marketing columns and per-file deduplication).\n"
        "7. Do not invent currency symbols (such as $) since unit_price is unitless, and clearly distinguish order count ('orders') from unit quantity ('units').\n\n"
        f"VERIFIED AGGREGATED METRICS JSON:\n{json.dumps(metrics_payload, indent=2)}"
    )

    request_body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
            "responseSchema": INSIGHTS_SCHEMA,
        },
    }
    encoded_body = json.dumps(request_body).encode("utf-8")

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        req = urllib.request.Request(
            url,
            data=encoded_body,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                raw_resp = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="ignore")[:500]
            if exc.code == 429 or exc.code >= 500:
                last_error = GeminiIntegrationError(
                    f"Gemini API HTTP {exc.code} (attempt {attempt}/{max_attempts}): {err_body}"
                )
                if attempt < max_attempts:
                    time.sleep(float(attempt))
                    continue
                raise last_error from exc
            raise GeminiIntegrationError(
                f"Gemini API rejected request with HTTP {exc.code}: {err_body}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = GeminiIntegrationError(
                f"Gemini API transport/decode error (attempt {attempt}/{max_attempts}): {exc}"
            )
            if attempt < max_attempts:
                time.sleep(float(attempt))
                continue
            raise last_error from exc

        candidates = raw_resp.get("candidates") or []
        if not candidates:
            raise GeminiIntegrationError("Gemini API returned no candidates.")

        parts = candidates[0].get("content", {}).get("parts") or []
        text = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
        if not text:
            raise GeminiIntegrationError("Gemini API returned an empty text part.")

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise GeminiIntegrationError(
                f"Gemini API returned malformed JSON: {exc}"
            ) from exc

        required_keys = set(INSIGHTS_SCHEMA["required"])
        if not isinstance(parsed, dict) or not required_keys.issubset(parsed.keys()):
            raise GeminiIntegrationError(
                f"Gemini JSON output missing required fields: {sorted(required_keys - set(parsed.keys() if isinstance(parsed, dict) else []))}"
            )

        return parsed

    raise GeminiIntegrationError(f"Gemini API failed after retries: {last_error}")


def _format_email_message(report: dict[str, Any]) -> str:
    kpis = report["kpis"]
    insights = report["insights"]
    lines = [
        f"Daily AI Business Insights Report — {report['report_date']}",
        "DISCLOSURE: Generated from simulated retail order data.",
        "",
        "Key Performance Indicators (Athena-verified):",
        f"- Total Orders: {kpis['total_orders']:,}",
        f"- Unique Customers (simulated): {kpis['unique_customers']:,}",
        f"- Units Sold: {kpis['units_sold']:,}",
        f"- Total Revenue: {kpis['total_revenue']:,.2f}",
        f"- Average Order Value: {kpis['average_order_value']:,.2f}",
    ]

    comp = report.get("previous_day_comparison")
    if comp:
        d = comp["deltas"]
        lines.extend(
            [
                "",
                f"Comparison vs {comp['previous_date']}:",
                f"- Revenue Change: {d['total_revenue_diff']:+,.2f} ({d['total_revenue_pct_change']:+.2f}%)"
                if d.get("total_revenue_pct_change") is not None
                else f"- Revenue Change: {d['total_revenue_diff']:+,.2f}",
                f"- Orders Change: {d['total_orders_diff']:+,d} ({d['total_orders_pct_change']:+.2f}%)"
                if d.get("total_orders_pct_change") is not None
                else f"- Orders Change: {d['total_orders_diff']:+,d}",
                f"- AOV Change: {d['average_order_value_diff']:+,.2f} ({d['average_order_value_pct_change']:+.2f}%)"
                if d.get("average_order_value_pct_change") is not None
                else f"- AOV Change: {d['average_order_value_diff']:+,.2f}",
            ]
        )

    lines.extend(
        [
            "",
            "Executive Summary:",
            insights["executive_summary"],
            "",
            "Top Observations:",
            *[f"- {item}" for item in insights["numerical_observations"][:5]],
            "",
            "Actionable Recommendations:",
            *[f"- {item}" for item in insights["recommendations"][:5]],
            "",
            f"Gemini Model: {report['gemini_model']}",
            f"S3 Report Location: {report['s3_report_uri']}",
            f"Generated At: {report['generated_at']}",
        ]
    )
    return "\n".join(lines)


def handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    bucket = os.environ.get("S3_BUCKET", "orders-etl-pipeline-2026-11-7f3a9c")
    database = _validate_identifier(
        os.environ.get("ATHENA_DATABASE", "orders_analytics_db"), "database"
    )
    table = _validate_identifier(os.environ.get("ATHENA_TABLE", "orders"), "table")
    workgroup = _validate_identifier(
        os.environ.get("ATHENA_WORKGROUP", "orders-etl-workgroup"),
        "workgroup",
        pattern=_WORKGROUP_RE,
    )
    insights_prefix = os.environ.get("INSIGHTS_PREFIX", "reports/insights/")
    if not insights_prefix.endswith("/"):
        insights_prefix = f"{insights_prefix}/"
    quality_prefix = os.environ.get("QUALITY_REPORT_PREFIX", "reports/")
    gemini_model = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
    ssm_param_name = os.environ.get(
        "SSM_GEMINI_PARAM_NAME", "/orders-etl/gemini-api-key"
    )
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "").strip()

    report_date = resolve_report_date(event)
    logger.info("Starting insights generation for report_date=%s", report_date)

    glue = boto3.client("glue")
    athena = boto3.client("athena")
    s3 = boto3.client("s3")
    ssm = boto3.client("ssm")
    sns = boto3.client("sns")

    partitions = discover_partitions(glue, database, table)
    if report_date not in partitions:
        logger.warning(
            "Skipping insights generation: partition order_date=%s not found in %s.%s",
            report_date,
            database,
            table,
        )
        return {
            "status": "skipped_missing_partition",
            "report_date": report_date,
            "available_partitions": partitions,
            "reason": f"Partition order_date={report_date} is not registered in Glue catalog.",
        }

    analytics = fetch_analytics_bundle(
        athena_client=athena,
        s3_client=s3,
        database=database,
        table=table,
        workgroup=workgroup,
        bucket=bucket,
        quality_prefix=quality_prefix,
        report_date=report_date,
        available_partitions=partitions,
    )

    if not analytics["has_data"]:
        logger.warning(
            "Skipping insights generation: zero valid rows returned for order_date=%s",
            report_date,
        )
        return {
            "status": "skipped_no_data",
            "report_date": report_date,
            "athena_query_execution_ids": analytics["query_ids"],
            "reason": f"No valid curated orders found in Athena for order_date={report_date}.",
        }

    metrics_for_gemini = {
        "report_date": report_date,
        "is_synthetic_dataset": True,
        "kpis": analytics["kpis"],
        "previous_day_comparison": analytics["previous_day_comparison"],
        "category_breakdown": analytics["category_breakdown"],
        "customer_concentration": analytics["customer_concentration"],
        "quality_summary": analytics["quality_summary"],
    }

    api_key = get_gemini_api_key(ssm, ssm_param_name)
    gemini_output = generate_gemini_insights(
        api_key=api_key,
        model=gemini_model,
        metrics_payload=metrics_for_gemini,
    )

    generated_at = datetime.now(timezone.utc).isoformat()
    s3_key = f"{insights_prefix}orders_insights_{report_date}.json"
    s3_uri = f"s3://{bucket}/{s3_key}"

    observations = (
        gemini_output["numerical_observations"]
        + gemini_output["revenue_and_order_trends"]
        + gemini_output["category_performance"]
        + gemini_output["customer_concentration"]
    )

    report = {
        "status": "generated",
        "report_date": report_date,
        "generated_at": generated_at,
        "athena_query_execution_ids": analytics["query_ids"],
        "kpis": analytics["kpis"],
        "previous_day_comparison": analytics["previous_day_comparison"],
        "category_breakdown": analytics["category_breakdown"],
        "customer_concentration": analytics["customer_concentration"],
        "gemini_model": gemini_model,
        "summary": gemini_output["executive_summary"],
        "insights": gemini_output,
        "observations": observations,
        "opportunities": gemini_output["opportunities"],
        "recommendations": gemini_output["recommendations"],
        "data_coverage_and_limitations": {
            "is_synthetic_dataset": True,
            "quality_report": analytics["quality_summary"],
            "caveats": gemini_output["limitations_and_caveats"],
        },
        "s3_report_uri": s3_uri,
    }

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Saved insights report to %s", s3_uri)

    sns_message_id = None
    if sns_topic_arn:
        try:
            pub_resp = sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Daily Orders AI Insights: {report_date}"[:100],
                Message=_format_email_message(report),
            )
            sns_message_id = pub_resp.get("MessageId")
            logger.info("Published daily insights SNS messageId=%s", sns_message_id)
        except (ClientError, BotoCoreError, Exception):
            logger.exception("Failed to publish daily insights SNS report for %s", report_date)
            raise
    else:
        logger.warning("SNS_TOPIC_ARN is not configured; skipping daily email publish.")

    return {
        "status": "generated",
        "report_date": report_date,
        "s3_report_uri": s3_uri,
        "gemini_model": gemini_model,
        "athena_query_execution_ids": analytics["query_ids"],
        "sns_message_id": sns_message_id,
    }

