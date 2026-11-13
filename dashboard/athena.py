from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    MissingDependencyException,
    NoCredentialsError,
    ProfileNotFound,
)
from dotenv import load_dotenv

load_dotenv()

CACHE_TTL_SECONDS = int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "3600"))

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_WORKGROUP_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class AwsAccessError(RuntimeError):
    """Raised when AWS authentication, Glue discovery, or Athena execution fails."""


@dataclass(frozen=True)
class DashboardConfig:
    aws_profile: str
    aws_region: str
    s3_bucket: str
    athena_database: str
    athena_table: str
    athena_workgroup: str
    quality_report_prefix: str
    insights_prefix: str


@dataclass(frozen=True)
class AthenaQueryResult:
    df: pd.DataFrame
    query_execution_id: str
    data_scanned_bytes: int
    execution_time_ms: int


def get_config() -> DashboardConfig:
    load_dotenv()
    return DashboardConfig(
        aws_profile=os.getenv("AWS_PROFILE", "etl-project"),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-2"),
        s3_bucket=os.getenv("S3_BUCKET", "orders-etl-pipeline-2026-11-7f3a9c"),
        athena_database=os.getenv("ATHENA_DATABASE", "orders_analytics_db"),
        athena_table=os.getenv("ATHENA_TABLE", "orders"),
        athena_workgroup=os.getenv("ATHENA_WORKGROUP", "orders-etl-workgroup"),
        quality_report_prefix=os.getenv("QUALITY_REPORT_PREFIX", "reports/"),
        insights_prefix=os.getenv("INSIGHTS_PREFIX", "reports/insights/"),
    )


def create_aws_session(
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> boto3.Session:
    cfg = get_config()
    region = aws_region or cfg.aws_region

    has_env_creds = bool(
        os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    raw_env_profile = os.getenv("AWS_PROFILE")
    profile = aws_profile if aws_profile is not None else raw_env_profile
    if profile is None and not has_env_creds:
        profile = cfg.aws_profile

    try:
        if profile and profile.strip():
            return boto3.Session(profile_name=profile.strip(), region_name=region)
        os.environ.pop("AWS_PROFILE", None)
        os.environ.pop("AWS_DEFAULT_PROFILE", None)
        return boto3.Session(region_name=region)
    except ProfileNotFound as exc:
        if has_env_creds:
            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_DEFAULT_PROFILE", None)
            try:
                return boto3.Session(region_name=region)
            except (
                ProfileNotFound,
                MissingDependencyException,
                NoCredentialsError,
                BotoCoreError,
            ) as fallback_exc:
                raise AwsAccessError(
                    f"Failed to initialize AWS session from environment credentials: {fallback_exc}"
                ) from fallback_exc
        raise AwsAccessError(
            f"AWS profile '{profile}' was not found. Configure it locally or supply AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables."
        ) from exc
    except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
        raise AwsAccessError(f"Failed to initialize AWS session: {exc}") from exc


def verify_aws_identity(
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> dict[str, Any]:
    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    try:
        sts = session.client("sts")
        return sts.get_caller_identity()
    except (
        ClientError,
        MissingDependencyException,
        NoCredentialsError,
        BotoCoreError,
    ) as exc:
        raise AwsAccessError(
            f"AWS authentication failed for profile '{session.profile_name}': {exc}"
        ) from exc


def _validate_identifier(value: str, label: str, pattern: re.Pattern[str] = _IDENTIFIER_RE) -> str:
    if not pattern.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def validate_date_range(start_date: date | str, end_date: date | str) -> tuple[str, str]:
    start_str = start_date.isoformat() if isinstance(start_date, date) else str(start_date)
    end_str = end_date.isoformat() if isinstance(end_date, date) else str(end_date)

    if not _DATE_RE.match(start_str) or not _DATE_RE.match(end_str):
        raise ValueError(f"Dates must use YYYY-MM-DD format: got {start_str!r}, {end_str!r}")

    start_parsed = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_parsed = datetime.strptime(end_str, "%Y-%m-%d").date()
    if start_parsed > end_parsed:
        raise ValueError(f"Start date ({start_str}) cannot be after end date ({end_str}).")

    return start_str, end_str


def discover_partition_dates(
    database: str | None = None,
    table: str | None = None,
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> list[date]:
    cfg = get_config()
    db_name = _validate_identifier(database or cfg.athena_database, "database")
    table_name = _validate_identifier(table or cfg.athena_table, "table")

    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    glue = session.client("glue")
    paginator = glue.get_paginator("get_partitions")

    dates: set[date] = set()
    try:
        for page in paginator.paginate(DatabaseName=db_name, TableName=table_name):
            for partition in page.get("Partitions", []):
                values = partition.get("Values") or []
                if not values:
                    continue
                raw_date = str(values[0]).strip()
                if _DATE_RE.match(raw_date):
                    try:
                        dates.add(datetime.strptime(raw_date, "%Y-%m-%d").date())
                    except ValueError:
                        continue
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        raise AwsAccessError(
            f"Glue get_partitions failed ({code}) for {db_name}.{table_name}: {exc}"
        ) from exc
    except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
        raise AwsAccessError(f"Failed to query Glue partitions: {exc}") from exc

    return sorted(dates)


def _cast_dataframe_types(df: pd.DataFrame, col_info: list[dict[str, Any]]) -> pd.DataFrame:
    int_types = {"bigint", "integer", "int", "smallint", "tinyint"}
    float_types = {"double", "float", "real", "decimal"}

    for meta in col_info:
        name = meta.get("Label") or meta.get("Name")
        col_type = str(meta.get("Type", "")).lower()
        if not name or name not in df.columns:
            continue
        if col_type in int_types:
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif col_type in float_types:
            df[name] = pd.to_numeric(df[name], errors="coerce")
    return df


def execute_athena_query(
    sql: str,
    database: str | None = None,
    workgroup: str | None = None,
    timeout_seconds: int = 60,
    poll_interval_seconds: float = 1.0,
    aws_profile: str | None = None,
    aws_region: str | None = None,
    allow_ddl: bool = False,
) -> AthenaQueryResult:
    is_ddl = allow_ddl or sql.strip().upper().startswith(("MSCK", "ALTER"))
    if not is_ddl and ("where" not in sql.lower() or "order_date" not in sql.lower()):
        raise ValueError("All Athena queries must filter on order_date.")

    cfg = get_config()
    db_name = _validate_identifier(database or cfg.athena_database, "database")
    wg_name = _validate_identifier(
        workgroup or cfg.athena_workgroup,
        "workgroup",
        pattern=_WORKGROUP_RE,
    )

    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    athena = session.client("athena")

    try:
        start_resp = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": db_name},
            WorkGroup=wg_name,
        )
        query_id = start_resp["QueryExecutionId"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        raise AwsAccessError(f"Athena start_query_execution failed ({code}): {exc}") from exc
    except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
        raise AwsAccessError(f"Failed to submit Athena query: {exc}") from exc

    deadline = time.monotonic() + timeout_seconds
    execution: dict[str, Any] = {}
    state = "QUEUED"

    while True:
        try:
            resp = athena.get_query_execution(QueryExecutionId=query_id)
            execution = resp["QueryExecution"]
            status = execution["Status"]
            state = status["State"]
        except (ClientError, BotoCoreError) as exc:
            raise AwsAccessError(
                f"Failed to poll Athena query {query_id}: {exc}"
            ) from exc

        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No failure reason provided.")
            raise AwsAccessError(
                f"Athena query {query_id} {state.lower()}: {reason}"
            )
        if time.monotonic() >= deadline:
            try:
                athena.stop_query_execution(QueryExecutionId=query_id)
            except (ClientError, BotoCoreError):
                pass
            raise AwsAccessError(
                f"Athena query {query_id} timed out after {timeout_seconds} seconds."
            )
        time.sleep(poll_interval_seconds)

    paginator = athena.get_paginator("get_query_results")
    col_info: list[dict[str, Any]] = []
    columns: list[str] = []
    records: list[list[Any]] = []

    try:
        for page_index, page in enumerate(paginator.paginate(QueryExecutionId=query_id)):
            result_set = page.get("ResultSet", {})
            if page_index == 0:
                col_info = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
                columns = [c.get("Label") or c.get("Name") for c in col_info]

            rows = result_set.get("Rows", [])
            data_rows = rows[1:] if page_index == 0 else rows

            for row in data_rows:
                data = row.get("Data", [])
                values = [
                    cell.get("VarCharValue") if isinstance(cell, dict) else None
                    for cell in data
                ]
                if len(values) < len(columns):
                    values.extend([None] * (len(columns) - len(values)))
                records.append(values[: len(columns)])
    except (ClientError, BotoCoreError) as exc:
        raise AwsAccessError(
            f"Failed retrieving Athena results for {query_id}: {exc}"
        ) from exc

    df = pd.DataFrame(records, columns=columns)
    if not df.empty:
        df = _cast_dataframe_types(df, col_info)

    stats = execution.get("Statistics", {})
    return AthenaQueryResult(
        df=df,
        query_execution_id=query_id,
        data_scanned_bytes=int(stats.get("DataScannedInBytes", 0) or 0),
        execution_time_ms=int(stats.get("EngineExecutionTimeInMillis", 0) or 0),
    )


def build_kpis_sql(database: str, table: str, start_str: str, end_str: str) -> str:
    return (
        "SELECT\n"
        "    COUNT(DISTINCT order_id) AS total_orders,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,\n"
        "    ROUND(\n"
        "        SUM(quantity * unit_price) / NULLIF(COUNT(DISTINCT order_id), 0),\n"
        "        2\n"
        "    ) AS average_order_value\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date BETWEEN '{start_str}' AND '{end_str}';"
    )


def build_daily_performance_sql(
    database: str, table: str, start_str: str, end_str: str
) -> str:
    return (
        "SELECT\n"
        "    order_date,\n"
        "    COUNT(DISTINCT order_id) AS total_orders,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS total_revenue\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date BETWEEN '{start_str}' AND '{end_str}'\n"
        "GROUP BY order_date\n"
        "ORDER BY order_date;"
    )


def build_category_revenue_sql(
    database: str, table: str, start_str: str, end_str: str
) -> str:
    return (
        "SELECT\n"
        "    category,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS revenue\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date BETWEEN '{start_str}' AND '{end_str}'\n"
        "GROUP BY category\n"
        "ORDER BY revenue DESC;"
    )


def build_top_customers_sql(
    database: str, table: str, start_str: str, end_str: str
) -> str:
    return (
        "SELECT\n"
        "    customer_id,\n"
        "    COUNT(DISTINCT order_id) AS total_orders,\n"
        "    ROUND(SUM(quantity * unit_price), 2) AS revenue\n"
        f"FROM {database}.{table}\n"
        f"WHERE order_date BETWEEN '{start_str}' AND '{end_str}'\n"
        "GROUP BY customer_id\n"
        "ORDER BY revenue DESC\n"
        "LIMIT 10;"
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def cached_discover_partition_dates() -> list[date]:
    return discover_partition_dates()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def cached_fetch_dashboard_athena_data(
    start_str: str,
    end_str: str,
) -> dict[str, Any]:
    start_str, end_str = validate_date_range(start_str, end_str)
    cfg = get_config()
    db_name = _validate_identifier(cfg.athena_database, "database")
    table_name = _validate_identifier(cfg.athena_table, "table")

    queries = {
        "kpis": build_kpis_sql(db_name, table_name, start_str, end_str),
        "daily": build_daily_performance_sql(db_name, table_name, start_str, end_str),
        "category": build_category_revenue_sql(db_name, table_name, start_str, end_str),
        "customers": build_top_customers_sql(db_name, table_name, start_str, end_str),
    }

    results: dict[str, AthenaQueryResult] = {}
    for key, sql in queries.items():
        results[key] = execute_athena_query(sql)

    return {
        "kpis_df": results["kpis"].df,
        "daily_df": results["daily"].df,
        "category_df": results["category"].df,
        "customers_df": results["customers"].df,
        "total_bytes_scanned": sum(r.data_scanned_bytes for r in results.values()),
        "query_ids": {k: r.query_execution_id for k, r in results.items()},
        "bytes_by_query": {k: r.data_scanned_bytes for k, r in results.items()},
        "queries": queries,
    }

