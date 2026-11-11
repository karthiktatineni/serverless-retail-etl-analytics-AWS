
import json
import logging
import os
from datetime import datetime, timezone
from io import StringIO
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")

REJECT_RATE_THRESHOLD = float(
    os.environ.get("REJECT_RATE_THRESHOLD", "0.10")
)

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "category",
    "quantity",
    "unit_price",
    "order_ts",
]


def write_json(bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def validate_orders(df):
    missing_columns = [
        column for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    if df.empty:
        raise ValueError("CSV contains no data rows")

    df = df.copy()
    df["source_row_number"] = range(2, len(df) + 2)

    df["order_id"] = df["order_id"].astype("string").str.strip()
    df["category"] = df["category"].astype("string").str.strip()

    df["customer_id"] = pd.to_numeric(
        df["customer_id"], errors="coerce"
    )
    df["quantity"] = pd.to_numeric(
        df["quantity"], errors="coerce"
    )
    df["unit_price"] = pd.to_numeric(
        df["unit_price"], errors="coerce"
    )
    df["order_ts"] = pd.to_datetime(
        df["order_ts"], errors="coerce", utc=True
    )

    missing_order_id = (
        df["order_id"].isna() | df["order_id"].eq("")
    ).fillna(True).astype(bool)

    masks = {
        "missing_order_id": missing_order_id,
        "missing_customer_id": df["customer_id"].isna()
        | df["customer_id"].mod(1).ne(0),
        "invalid_quantity": df["quantity"].isna()
        | df["quantity"].le(0)
        | df["quantity"].mod(1).ne(0),
        "invalid_unit_price": df["unit_price"].isna()
        | df["unit_price"].le(0)
        | df["unit_price"].isin([float("inf"), float("-inf")]),
        "missing_order_ts": df["order_ts"].isna(),
        "missing_category": (
            df["category"].isna() | df["category"].eq("")
        ),
        "duplicate_order_id": (
            ~missing_order_id & df["order_id"].duplicated(keep="first")
        ),
    }

    masks = {
        name: mask.fillna(True).astype(bool)
        for name, mask in masks.items()
    }

    reason_lists = [[] for _ in range(len(df))]

    for reason, mask in masks.items():
        for position, invalid in enumerate(mask.tolist()):
            if invalid:
                reason_lists[position].append(reason)

    reject_reasons = [
        ";".join(reasons) for reasons in reason_lists
    ]

    df["reject_reason"] = reject_reasons

    invalid_mask = df["reject_reason"].ne("")

    valid = df.loc[~invalid_mask].copy()
    rejected = df.loc[invalid_mask].copy()

    if not valid.empty:
        valid["customer_id"] = valid["customer_id"].astype("int64")
        valid["quantity"] = valid["quantity"].astype("int64")
        valid["order_date"] = valid["order_ts"].dt.strftime(
            "%Y-%m-%d"
        )
        valid = valid.drop(columns=["reject_reason"])

    reason_counts = {
        name: int(mask.sum())
        for name, mask in masks.items()
    }

    return valid, rejected, reason_counts


def process_file(bucket, key):
    source_name = key.rsplit("/", 1)[-1]
    source_uri = f"s3://{bucket}/{key}"
    file_stem = source_name.rsplit(".", 1)[0]

    logger.info("Processing s3://%s/%s", bucket, key)

    try:
        df = wr.s3.read_csv(path=source_uri)

        valid, rejected, reason_counts = validate_orders(df)

    except (
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
        ValueError,
    ) as exc:
        report = {
            "status": "failed",
            "source_file": source_name,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "processed_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        write_json(
            bucket,
            f"reports/{file_stem}.json",
            report,
        )

        logger.error("File validation failed: %s", exc)
        return report

    total_rows = len(df)
    valid_rows = len(valid)
    rejected_rows = len(rejected)

    reject_rate = (
        rejected_rows / total_rows if total_rows else 0.0
    )

    if not valid.empty:
        wr.s3.to_parquet(
            df=valid,
            path=f"s3://{bucket}/curated/orders/",
            dataset=True,
            mode="overwrite_partitions",
            partition_cols=["order_date"],
            compression="snappy",
            index=False,
        )

    if not rejected.empty:
        csv_buffer = StringIO()
        rejected.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=f"quarantine/orders/{file_stem}.csv",
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

    report = {
        "status": "processed",
        "source_file": source_name,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "reject_rate": round(reject_rate, 6),
        "reason_counts": reason_counts,
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    write_json(
        bucket,
        f"reports/{file_stem}.json",
        report,
    )

    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    threshold = float(
        os.environ.get("REJECT_RATE_THRESHOLD", str(REJECT_RATE_THRESHOLD))
    )

    if reject_rate > threshold:
        message = json.dumps(report, indent=2)

        if topic_arn:
            try:
                sns.publish(
                    TopicArn=topic_arn,
                    Subject=f"ETL data quality alert: {source_name}"[:100],
                    Message=message,
                )
            except Exception:
                logger.exception(
                    "Failed to publish SNS alert for %s", source_name
                )
                raise
        else:
            logger.warning(
                "Reject rate exceeded threshold, "
                "but SNS_TOPIC_ARN is not configured."
            )

    logger.info("Processing report: %s", report)

    return report


def handler(event, context):
    records = event.get("Records", [])
    results = []

    for record in records:
        if record.get("eventSource") != "aws:s3":
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        if not key.startswith("raw/orders/"):
            continue

        if not key.lower().endswith(".csv"):
            continue

        results.append(process_file(bucket, key))

    return {
        "processed_files": len(results),
        "results": results,
    }
