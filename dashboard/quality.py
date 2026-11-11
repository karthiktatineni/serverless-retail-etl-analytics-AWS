from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    MissingDependencyException,
    NoCredentialsError,
)

try:
    from dashboard.athena import (
        CACHE_TTL_SECONDS,
        AwsAccessError,
        create_aws_session,
        get_config,
        validate_date_range,
    )
except ImportError:
    from athena import (
        CACHE_TTL_SECONDS,
        AwsAccessError,
        create_aws_session,
        get_config,
        validate_date_range,
    )

_REPORT_FILENAME_RE = re.compile(r"(?:^|/)orders_(\d{4}-\d{2}-\d{2})\.json$")

DEFAULT_REASON_ORDER = [
    "missing_order_id",
    "missing_customer_id",
    "invalid_quantity",
    "invalid_unit_price",
    "missing_order_ts",
    "missing_category",
    "duplicate_order_id",
]


@dataclass(frozen=True)
class QualitySummary:
    selected_dates_count: int
    reports_found_count: int
    missing_dates: list[str]
    invalid_reports: list[dict[str, str]]
    total_input_rows: int
    total_valid_rows: int
    total_rejected_rows: int
    overall_reject_rate_pct: float | None
    daily_df: pd.DataFrame
    reason_counts: dict[str, int]
    matched_keys: list[str]


def _extract_report_date(key: str) -> date | None:
    match = _REPORT_FILENAME_RE.search(key)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_report_payload(raw_bytes: bytes, key: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Malformed JSON in {key}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {key}")

    status = payload.get("status")
    if status != "processed":
        error_msg = payload.get("error") or f"status={status!r}"
        raise ValueError(f"Report {key} is not in 'processed' state ({error_msg})")

    required_ints = ("total_rows", "valid_rows", "rejected_rows")
    for field in required_ints:
        val = payload.get(field)
        if not isinstance(val, int) or isinstance(val, bool) or val < 0:
            raise ValueError(f"Invalid {field!r} in {key}: {val!r}")

    reason_counts = payload.get("reason_counts")
    if not isinstance(reason_counts, dict):
        raise ValueError(f"Missing or invalid 'reason_counts' in {key}")

    cleaned_reasons: dict[str, int] = {}
    for reason, count in reason_counts.items():
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError(
                f"Invalid reason count for {reason!r} in {key}: {count!r}"
            )
        cleaned_reasons[str(reason)] = count

    return {
        "source_file": str(payload.get("source_file", key.rsplit("/", 1)[-1])),
        "total_rows": payload["total_rows"],
        "valid_rows": payload["valid_rows"],
        "rejected_rows": payload["rejected_rows"],
        "reason_counts": cleaned_reasons,
        "processed_at": str(payload.get("processed_at", "")),
    }


def fetch_quality_summary(
    start_date: date | str,
    end_date: date | str,
    bucket: str | None = None,
    prefix: str | None = None,
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> QualitySummary:
    start_str, end_str = validate_date_range(start_date, end_date)
    start_dt = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_str, "%Y-%m-%d").date()

    cfg = get_config()
    bucket_name = bucket or cfg.s3_bucket
    report_prefix = prefix or cfg.quality_report_prefix

    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    matched_objects: dict[date, str] = {}
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=report_prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                rep_date = _extract_report_date(key)
                if rep_date is not None and start_dt <= rep_date <= end_dt:
                    matched_objects[rep_date] = key
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        raise AwsAccessError(
            f"S3 list_objects_v2 failed ({code}) on s3://{bucket_name}/{report_prefix}: {exc}"
        ) from exc
    except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
        raise AwsAccessError(f"Failed listing S3 quality reports: {exc}") from exc

    all_selected_dates: list[date] = []
    cursor = start_dt
    while cursor <= end_dt:
        all_selected_dates.append(cursor)
        cursor += timedelta(days=1)

    daily_records: list[dict[str, Any]] = []
    invalid_reports: list[dict[str, str]] = []
    missing_dates: list[str] = []
    aggregated_reasons: dict[str, int] = {k: 0 for k in DEFAULT_REASON_ORDER}
    matched_keys: list[str] = []

    for day in all_selected_dates:
        day_str = day.isoformat()
        key = matched_objects.get(day)
        if key is None:
            missing_dates.append(day_str)
            continue

        matched_keys.append(key)
        try:
            resp = s3.get_object(Bucket=bucket_name, Key=key)
            raw_bytes = resp["Body"].read()
            parsed = _parse_report_payload(raw_bytes, key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "ClientError")
            raise AwsAccessError(
                f"S3 get_object failed ({code}) for s3://{bucket_name}/{key}: {exc}"
            ) from exc
        except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
            raise AwsAccessError(
                f"Failed reading S3 report s3://{bucket_name}/{key}: {exc}"
            ) from exc
        except ValueError as exc:
            invalid_reports.append({"date": day_str, "key": key, "reason": str(exc)})
            missing_dates.append(day_str)
            continue

        total_rows = parsed["total_rows"]
        rejected_rows = parsed["rejected_rows"]
        reject_rate_pct = (
            (rejected_rows / total_rows) * 100.0 if total_rows > 0 else None
        )

        daily_records.append(
            {
                "report_date": day_str,
                "source_file": parsed["source_file"],
                "total_rows": total_rows,
                "valid_rows": parsed["valid_rows"],
                "rejected_rows": rejected_rows,
                "reject_rate_pct": reject_rate_pct,
                "processed_at": parsed["processed_at"],
            }
        )

        for reason, count in parsed["reason_counts"].items():
            aggregated_reasons[reason] = aggregated_reasons.get(reason, 0) + count

    daily_df = pd.DataFrame(daily_records)
    total_input_rows = int(daily_df["total_rows"].sum()) if not daily_df.empty else 0
    total_valid_rows = int(daily_df["valid_rows"].sum()) if not daily_df.empty else 0
    total_rejected_rows = (
        int(daily_df["rejected_rows"].sum()) if not daily_df.empty else 0
    )

    overall_reject_rate_pct: float | None = (
        (total_rejected_rows / total_input_rows) * 100.0
        if not daily_df.empty and total_input_rows > 0
        else None
    )

    return QualitySummary(
        selected_dates_count=len(all_selected_dates),
        reports_found_count=len(daily_records),
        missing_dates=missing_dates,
        invalid_reports=invalid_reports,
        total_input_rows=total_input_rows,
        total_valid_rows=total_valid_rows,
        total_rejected_rows=total_rejected_rows,
        overall_reject_rate_pct=overall_reject_rate_pct,
        daily_df=daily_df,
        reason_counts=aggregated_reasons,
        matched_keys=matched_keys,
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def cached_fetch_quality_summary(start_str: str, end_str: str) -> QualitySummary:
    return fetch_quality_summary(start_str, end_str)
