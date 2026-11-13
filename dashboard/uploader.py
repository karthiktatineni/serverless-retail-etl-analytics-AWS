from __future__ import annotations

import io
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError

try:
    from dashboard.athena import (
        AwsAccessError,
        DashboardConfig,
        create_aws_session,
        execute_athena_query,
        get_config,
    )
except ImportError:
    from athena import (
        AwsAccessError,
        DashboardConfig,
        create_aws_session,
        execute_athena_query,
        get_config,
    )

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "category",
    "quantity",
    "unit_price",
    "order_ts",
]

MAX_UPLOAD_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB
DEFAULT_UPLOAD_PASSCODE = os.environ.get("PIPELINE_UPLOAD_PASSCODE", "orders2026")
_FILENAME_SAFE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class UploadValidationError(ValueError):
    """Raised when an uploaded CSV fails structural validation."""


def sanitize_filename(raw_name: str) -> str:
    """Sanitizes an uploaded filename to prevent directory traversal or unsafe keys."""
    base = os.path.basename(raw_name).strip()
    if not base.lower().endswith(".csv"):
        base = f"{base}.csv"
    # Replace any character not in [A-Za-z0-9_.-] with '_'
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
    if not cleaned.lower().startswith("orders_"):
        cleaned = f"orders_{cleaned}"
    return cleaned


def validate_uploaded_csv(file_bytes: bytes, filename: str) -> dict[str, Any]:
    """Performs structural preflight validation on an uploaded CSV buffer.

    Validates:
    - File size <= 5 MB
    - Valid CSV format
    - Presence of all 6 required columns
    - Extraction of distinct order dates from order_ts
    """
    if len(file_bytes) == 0:
        raise UploadValidationError("The uploaded CSV is empty (0 bytes).")
    if len(file_bytes) > MAX_UPLOAD_SIZE_BYTES:
        max_mb = MAX_UPLOAD_SIZE_BYTES / (1024 * 1024)
        raise UploadValidationError(
            f"File size ({len(file_bytes) / (1024 * 1024):.2f} MB) exceeds maximum allowed limit ({max_mb:.0f} MB)."
        )

    try:
        sample_df = pd.read_csv(io.BytesIO(file_bytes), nrows=10)
    except Exception as exc:
        raise UploadValidationError(f"Could not parse CSV: {exc}") from exc

    missing = [c for c in REQUIRED_COLUMNS if c not in sample_df.columns]
    if missing:
        raise UploadValidationError(
            f"CSV missing required columns: {missing}. Required columns: {REQUIRED_COLUMNS}"
        )

    # Read complete dataframe to count rows and detect dates
    try:
        full_df = pd.read_csv(io.BytesIO(file_bytes))
    except Exception as exc:
        raise UploadValidationError(f"Error reading CSV content: {exc}") from exc

    if full_df.empty:
        raise UploadValidationError("CSV contains header only, no data rows.")

    dates_detected: list[str] = []
    if "order_ts" in full_df.columns:
        parsed_dates = pd.to_datetime(full_df["order_ts"], errors="coerce").dt.strftime("%Y-%m-%d").dropna().unique()
        dates_detected = sorted(d for d in parsed_dates if _DATE_RE.match(d))

    return {
        "valid": True,
        "row_count": len(full_df),
        "columns": list(full_df.columns),
        "dates_detected": dates_detected,
        "sample_preview": full_df.head(5),
    }


def upload_csv_to_s3(
    file_bytes: bytes,
    filename: str,
    bucket: str,
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> dict[str, Any]:
    """Uploads validated CSV bytes to s3://{bucket}/raw/orders/{filename}."""
    sanitized = sanitize_filename(filename)
    s3_key = f"raw/orders/{sanitized}"

    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    s3 = session.client("s3")

    try:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_bytes,
            ContentType="text/csv",
            ServerSideEncryption="AES256",
        )
        head = s3.head_object(Bucket=bucket, Key=s3_key)
    except (ClientError, BotoCoreError) as exc:
        raise AwsAccessError(f"Failed to upload {s3_key} to S3 bucket {bucket}: {exc}") from exc

    return {
        "bucket": bucket,
        "key": s3_key,
        "filename": sanitized,
        "file_stem": sanitized.rsplit(".", 1)[0],
        "size_bytes": head.get("ContentLength", len(file_bytes)),
        "etag": head.get("ETag"),
        "last_modified": head.get("LastModified"),
        "server_side_encryption": head.get("ServerSideEncryption"),
    }


def poll_for_quality_report(
    bucket: str,
    file_stem: str,
    max_wait_seconds: int = 40,
    poll_interval: float = 1.5,
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> dict[str, Any] | None:
    """Polls S3 for the ingestion Lambda's output quality report (reports/{file_stem}.json)."""
    report_key = f"reports/{file_stem}.json"
    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    s3 = session.client("s3")

    deadline = time.monotonic() + max_wait_seconds
    while time.monotonic() < deadline:
        try:
            resp = s3.get_object(Bucket=bucket, Key=report_key)
            payload = json.loads(resp["Body"].read().decode("utf-8"))
            return payload
        except s3.exceptions.NoSuchKey:
            time.sleep(poll_interval)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                time.sleep(poll_interval)
            else:
                raise AwsAccessError(f"Error checking report {report_key}: {exc}") from exc

    return None


def register_partition_via_athena(
    database: str,
    table: str,
    order_date: str,
    bucket: str,
    workgroup: str,
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> str:
    """Registers the new partition in AWS Glue Catalog via an Athena ALTER TABLE statement."""
    if not _DATE_RE.match(order_date):
        raise ValueError(f"Invalid partition date: {order_date!r}")

    location = f"s3://{bucket}/curated/orders/order_date={order_date}/"
    sql = (
        f"ALTER TABLE {database}.{table} "
        f"ADD IF NOT EXISTS PARTITION (order_date = '{order_date}') "
        f"LOCATION '{location}';"
    )
    result = execute_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        aws_profile=aws_profile,
        aws_region=aws_region,
        allow_ddl=True,
    )
    return result.query_execution_id


def render_upload_ui(cfg: DashboardConfig) -> None:
    """Renders the secure CSV Upload and Pipeline Ingestion interface in Streamlit."""
    with st.expander("📤 Upload Orders CSV & Run Pipeline (Event-Driven)", expanded=False):
        st.markdown(
            """
            Upload a daily retail orders CSV directly to Amazon S3 (`raw/orders/`).
            The upload triggers the **`orders-etl-ingest`** Lambda via an S3 `ObjectCreated` event,
            converting valid orders to Snappy Parquet, quarantining invalid records, and updating the Glue Catalog.
            """
        )

        col_left, col_right = st.columns([2, 1])

        with col_left:
            uploaded_file = st.file_uploader(
                "Choose a CSV file",
                type=["csv"],
                help="Requires columns: order_id, customer_id, category, quantity, unit_price, order_ts. Max size 5 MB.",
            )

        with col_right:
            st.markdown("**Security Verification**")
            expected_passcode = os.environ.get("PIPELINE_UPLOAD_PASSCODE", DEFAULT_UPLOAD_PASSCODE)
            user_passcode = st.text_input(
                "Upload Passcode",
                type="password",
                placeholder="Enter pipeline upload key",
                help="Protects your AWS infrastructure from unauthorized public uploads.",
            )

        if uploaded_file is not None:
            file_bytes = uploaded_file.getvalue()
            filename = uploaded_file.name

            try:
                validation = validate_uploaded_csv(file_bytes, filename)
            except UploadValidationError as exc:
                st.error(f"❌ Validation failed: {exc}")
                return

            st.success(
                f"✅ **CSV Validated**: {validation['row_count']:,} rows | "
                f"Dates detected: `{', '.join(validation['dates_detected']) if validation['dates_detected'] else 'None'}`"
            )

            with st.expander("Preview CSV Rows (First 5)", expanded=False):
                st.dataframe(validation["sample_preview"], width="stretch")

            # Execution button
            run_btn = st.button(
                "🚀 Upload to S3 & Trigger Ingestion",
                type="primary",
                disabled=st.session_state.get("is_uploading", False),
            )

            if run_btn:
                if not user_passcode or user_passcode.strip() != expected_passcode:
                    st.error("🔒 Invalid upload passcode. Unauthorized pipeline submission prevented.")
                    return

                st.session_state["is_uploading"] = True
                progress_placeholder = st.empty()
                status_box = st.empty()

                try:
                    with progress_placeholder.container():
                        st.info("Step 1/3: Uploading CSV to S3 `raw/orders/` with AES256 encryption...")

                    upload_meta = upload_csv_to_s3(
                        file_bytes=file_bytes,
                        filename=filename,
                        bucket=cfg.s3_bucket,
                    )
                    file_stem = upload_meta["file_stem"]

                    with progress_placeholder.container():
                        st.info(
                            f"Step 2/3: S3 `ObjectCreated` event fired! "
                            f"Waiting for `orders-etl-ingest` Lambda to process `{upload_meta['key']}`..."
                        )

                    # Poll for ingestion Lambda report
                    report = poll_for_quality_report(
                        bucket=cfg.s3_bucket,
                        file_stem=file_stem,
                        max_wait_seconds=40,
                    )

                    if not report:
                        st.warning(
                            f"⚠️ Uploaded successfully to `{upload_meta['key']}`, "
                            f"but quality report `reports/{file_stem}.json` took longer than 40s to arrive. "
                            f"Check CloudWatch logs for `orders-etl-ingest`."
                        )
                        return

                    # Step 3: Partition Registration
                    with progress_placeholder.container():
                        st.info("Step 3/3: Registering new partition(s) in AWS Glue Catalog via Athena...")

                    for d in validation["dates_detected"]:
                        try:
                            register_partition_via_athena(
                                database=cfg.athena_database,
                                table=cfg.athena_table,
                                order_date=d,
                                bucket=cfg.s3_bucket,
                                workgroup=cfg.athena_workgroup,
                            )
                        except Exception as reg_exc:
                            logger.warning("Partition register warning for %s: %s", d, reg_exc)

                    progress_placeholder.empty()

                    # Display Completion Metrics
                    st.success("🎉 **Pipeline Ingestion Succeeded!**")
                    metric_c1, metric_c2, metric_c3, metric_c4 = st.columns(4)
                    metric_c1.metric("Input Rows", f"{report.get('total_rows', 0):,}")
                    metric_c2.metric("Valid Rows (Curated)", f"{report.get('valid_rows', 0):,}")
                    metric_c3.metric("Quarantined Rows", f"{report.get('rejected_rows', 0):,}")
                    reject_pct = report.get("reject_rate", 0.0) * 100.0
                    metric_c4.metric("Rejection Rate", f"{reject_pct:.2f}%")

                    st.info(
                        f"📁 **Curated Parquet**: `s3://{cfg.s3_bucket}/curated/orders/` | "
                        f"📋 **Quality Report**: `s3://{cfg.s3_bucket}/reports/{file_stem}.json`"
                    )

                    # Clear cache so the new date immediately appears in filters & charts
                    st.cache_data.clear()
                    st.button("🔄 Refresh Dashboard to view new date", on_click=lambda: st.rerun())

                except AwsAccessError as exc:
                    st.error(f"AWS Error: {exc}")
                except Exception as exc:
                    st.error(f"Unexpected error: {exc}")
                finally:
                    st.session_state["is_uploading"] = False
