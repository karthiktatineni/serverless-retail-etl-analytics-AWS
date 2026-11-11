from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

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
    )
except ImportError:
    from athena import (
        CACHE_TTL_SECONDS,
        AwsAccessError,
        create_aws_session,
        get_config,
    )

_INSIGHTS_FILENAME_RE = re.compile(r"(?:^|/)orders_insights_(\d{4}-\d{2}-\d{2})\.json$")
IST = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True)
class InsightsReport:
    found: bool
    s3_key: str | None
    report_date: str | None
    generated_at: str | None
    gemini_model: str | None
    executive_summary: str | None
    observations: list[str]
    recommendations: list[str]
    opportunities: list[str]
    caveats: list[str]
    is_stale: bool
    stale_reason: str | None
    error_message: str | None


def _extract_insights_date(key: str) -> date | None:
    match = _INSIGHTS_FILENAME_RE.search(key)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _as_str_list(val: Any) -> list[str]:
    if not isinstance(val, list):
        return []
    return [str(item) for item in val if item is not None and str(item).strip()]


def fetch_latest_insights_report(
    latest_partition_date: date | None = None,
    stale_hours: int = 36,
    now_utc: datetime | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    aws_profile: str | None = None,
    aws_region: str | None = None,
) -> InsightsReport:
    cfg = get_config()
    bucket_name = bucket or cfg.s3_bucket
    insights_prefix = (
        prefix
        or getattr(cfg, "insights_prefix", None)
        or os.getenv("INSIGHTS_PREFIX", "reports/insights/")
    )

    session = create_aws_session(aws_profile=aws_profile, aws_region=aws_region)
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    matched: list[tuple[date, str]] = []
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=insights_prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                rep_date = _extract_insights_date(key)
                if rep_date is not None:
                    matched.append((rep_date, key))
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        raise AwsAccessError(
            f"S3 list_objects_v2 failed ({code}) on s3://{bucket_name}/{insights_prefix}: {exc}"
        ) from exc
    except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
        raise AwsAccessError(f"Failed listing S3 AI insights reports: {exc}") from exc

    if not matched:
        return InsightsReport(
            found=False,
            s3_key=None,
            report_date=None,
            generated_at=None,
            gemini_model=None,
            executive_summary=None,
            observations=[],
            recommendations=[],
            opportunities=[],
            caveats=[],
            is_stale=False,
            stale_reason=None,
            error_message=None,
        )

    matched.sort(key=lambda item: item[0], reverse=True)

    last_parse_error: str | None = None
    for rep_date, key in matched:
        try:
            resp = s3.get_object(Bucket=bucket_name, Key=key)
            payload = json.loads(resp["Body"].read().decode("utf-8"))
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "ClientError")
            raise AwsAccessError(
                f"S3 get_object failed ({code}) for s3://{bucket_name}/{key}: {exc}"
            ) from exc
        except (MissingDependencyException, NoCredentialsError, BotoCoreError) as exc:
            raise AwsAccessError(
                f"Failed reading S3 AI insights report s3://{bucket_name}/{key}: {exc}"
            ) from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_parse_error = f"Malformed JSON in {key}: {exc}"
            continue

        if not isinstance(payload, dict) or payload.get("status") != "generated":
            last_parse_error = f"Report {key} is not in 'generated' status."
            continue

        insights_block = payload.get("insights")
        if not isinstance(insights_block, dict):
            insights_block = {}

        summary = (
            payload.get("summary")
            or insights_block.get("executive_summary")
            or ""
        ).strip()
        if not summary:
            last_parse_error = f"Report {key} is missing an executive summary."
            continue

        observations = _as_str_list(payload.get("observations"))
        if not observations:
            observations = (
                _as_str_list(insights_block.get("numerical_observations"))
                + _as_str_list(insights_block.get("revenue_and_order_trends"))
                + _as_str_list(insights_block.get("category_performance"))
                + _as_str_list(insights_block.get("customer_concentration"))
            )

        recommendations = _as_str_list(
            payload.get("recommendations") or insights_block.get("recommendations")
        )
        opportunities = _as_str_list(
            payload.get("opportunities") or insights_block.get("opportunities")
        )
        caveats = _as_str_list(
            (payload.get("data_coverage_and_limitations") or {}).get("caveats")
            or insights_block.get("limitations_and_caveats")
        )

        generated_at = str(payload.get("generated_at", "")).strip() or None
        report_date_str = str(payload.get("report_date") or rep_date.isoformat())
        gemini_model = str(payload.get("gemini_model", "")).strip() or None

        current_utc = now_utc or datetime.now(timezone.utc)
        latest_completed_ist = current_utc.astimezone(IST).date() - timedelta(days=1)

        stale_reasons: list[str] = []
        if latest_partition_date is not None:
            expected_report_date = min(latest_partition_date, latest_completed_ist)
            if rep_date < expected_report_date:
                stale_reasons.append(
                    f"Report covers {rep_date.isoformat()}, behind latest completed partition {expected_report_date.isoformat()}."
                )

        generated_dt = _parse_iso_datetime(generated_at)
        if generated_dt is None:
            stale_reasons.append("Report generation timestamp is missing or unparseable.")
        elif (current_utc - generated_dt) > timedelta(hours=stale_hours):
            age_hours = int((current_utc - generated_dt).total_seconds() // 3600)
            stale_reasons.append(
                f"Report was generated {age_hours} hours ago (exceeds {stale_hours}h freshness window)."
            )

        return InsightsReport(
            found=True,
            s3_key=key,
            report_date=report_date_str,
            generated_at=generated_at,
            gemini_model=gemini_model,
            executive_summary=summary,
            observations=observations,
            recommendations=recommendations,
            opportunities=opportunities,
            caveats=caveats,
            is_stale=bool(stale_reasons),
            stale_reason=" ".join(stale_reasons) if stale_reasons else None,
            error_message=None,
        )

    return InsightsReport(
        found=False,
        s3_key=None,
        report_date=None,
        generated_at=None,
        gemini_model=None,
        executive_summary=None,
        observations=[],
        recommendations=[],
        opportunities=[],
        caveats=[],
        is_stale=False,
        stale_reason=None,
        error_message=last_parse_error or "No valid AI insights reports found.",
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def cached_fetch_latest_insights_report(
    latest_partition_date_str: str | None = None,
) -> InsightsReport:
    latest_dt = (
        datetime.strptime(latest_partition_date_str, "%Y-%m-%d").date()
        if latest_partition_date_str
        else None
    )
    return fetch_latest_insights_report(latest_partition_date=latest_dt)

