import importlib.util
import json
import os
import sys
import types
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAMBDA_INGEST_PATH = PROJECT_ROOT / "src" / "lambda_ingest" / "lambda_function.py"


def load_ingest_module():
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = MagicMock(side_effect=[MagicMock(), MagicMock()])

    fake_wr = types.ModuleType("awswrangler")
    fake_wr.s3 = types.SimpleNamespace(
        read_csv=MagicMock(),
        to_parquet=MagicMock(),
    )

    with patch.dict(
        sys.modules,
        {"boto3": fake_boto3, "awswrangler": fake_wr},
    ):
        spec = importlib.util.spec_from_file_location(
            "lambda_ingest_under_test",
            LAMBDA_INGEST_PATH,
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


class TestLambdaIngestValidation(unittest.TestCase):
    def setUp(self):
        self.mod = load_ingest_module()
        self.mod.s3 = MagicMock()
        self.mod.sns = MagicMock()
        self.mod.wr.s3.read_csv = MagicMock()
        self.mod.wr.s3.to_parquet = MagicMock()

    def test_valid_csv(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "20261115-000001",
                    "customer_id": 1001.0,
                    "category": "Electronics",
                    "quantity": 2,
                    "unit_price": 49.99,
                    "order_ts": "2026-11-15 10:00:00",
                },
                {
                    "order_id": "20261115-000002",
                    "customer_id": "1002",
                    "category": "Books",
                    "quantity": 1,
                    "unit_price": 15.5,
                    "order_ts": "2026-11-15 12:30:00",
                },
            ]
        )

        valid, rejected, reason_counts = self.mod.validate_orders(df)

        self.assertEqual(len(valid), 2)
        self.assertEqual(len(rejected), 0)
        self.assertTrue(all(v == 0 for v in reason_counts.values()))
        self.assertEqual(valid["order_date"].tolist(), ["2026-11-15", "2026-11-15"])
        self.assertEqual(str(valid["customer_id"].dtype), "int64")
        self.assertEqual(str(valid["quantity"].dtype), "int64")
        self.assertNotIn("reject_reason", valid.columns)

    def test_missing_and_invalid_customer_id(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "customer_id": None,
                    "category": "Home",
                    "quantity": 1,
                    "unit_price": 10.0,
                    "order_ts": "2026-11-15 01:00:00",
                },
                {
                    "order_id": "o2",
                    "customer_id": 12.5,
                    "category": "Home",
                    "quantity": 1,
                    "unit_price": 10.0,
                    "order_ts": "2026-11-15 01:00:00",
                },
                {
                    "order_id": "o3",
                    "customer_id": "abc",
                    "category": "Home",
                    "quantity": 1,
                    "unit_price": 10.0,
                    "order_ts": "2026-11-15 01:00:00",
                },
            ]
        )

        valid, rejected, reason_counts = self.mod.validate_orders(df)

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(rejected), 3)
        self.assertEqual(reason_counts["missing_customer_id"], 3)

    def test_missing_timestamps(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "customer_id": 1001,
                    "category": "Sports",
                    "quantity": 1,
                    "unit_price": 20.0,
                    "order_ts": None,
                },
                {
                    "order_id": "o2",
                    "customer_id": 1002,
                    "category": "Sports",
                    "quantity": 1,
                    "unit_price": 20.0,
                    "order_ts": "not-a-timestamp",
                },
            ]
        )

        valid, rejected, reason_counts = self.mod.validate_orders(df)

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(rejected), 2)
        self.assertEqual(reason_counts["missing_order_ts"], 2)

    def test_invalid_quantities(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "customer_id": 1001,
                    "category": "Fashion",
                    "quantity": 0,
                    "unit_price": 25.0,
                    "order_ts": "2026-11-15 08:00:00",
                },
                {
                    "order_id": "o2",
                    "customer_id": 1002,
                    "category": "Fashion",
                    "quantity": -2,
                    "unit_price": 25.0,
                    "order_ts": "2026-11-15 08:00:00",
                },
                {
                    "order_id": "o3",
                    "customer_id": 1003,
                    "category": "Fashion",
                    "quantity": 1.5,
                    "unit_price": 25.0,
                    "order_ts": "2026-11-15 08:00:00",
                },
            ]
        )

        valid, rejected, reason_counts = self.mod.validate_orders(df)

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(rejected), 3)
        self.assertEqual(reason_counts["invalid_quantity"], 3)

    def test_zero_negative_and_infinite_prices(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "customer_id": 1001,
                    "category": "Beauty",
                    "quantity": 1,
                    "unit_price": 0,
                    "order_ts": "2026-11-15 09:00:00",
                },
                {
                    "order_id": "o2",
                    "customer_id": 1002,
                    "category": "Beauty",
                    "quantity": 1,
                    "unit_price": -10.5,
                    "order_ts": "2026-11-15 09:00:00",
                },
                {
                    "order_id": "o3",
                    "customer_id": 1003,
                    "category": "Beauty",
                    "quantity": 1,
                    "unit_price": float("inf"),
                    "order_ts": "2026-11-15 09:00:00",
                },
            ]
        )

        valid, rejected, reason_counts = self.mod.validate_orders(df)

        self.assertEqual(len(valid), 0)
        self.assertEqual(len(rejected), 3)
        self.assertEqual(reason_counts["invalid_unit_price"], 3)

    def test_duplicate_order_ids_and_missing_order_ids(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "dup-1",
                    "customer_id": 1001,
                    "category": "Books",
                    "quantity": 1,
                    "unit_price": 12.0,
                    "order_ts": "2026-11-15 09:00:00",
                },
                {
                    "order_id": "dup-1",
                    "customer_id": 1001,
                    "category": "Books",
                    "quantity": 1,
                    "unit_price": 12.0,
                    "order_ts": "2026-11-15 09:00:00",
                },
                {
                    "order_id": "",
                    "customer_id": 1002,
                    "category": "Books",
                    "quantity": 1,
                    "unit_price": 12.0,
                    "order_ts": "2026-11-15 09:00:00",
                },
                {
                    "order_id": None,
                    "customer_id": 1003,
                    "category": "Books",
                    "quantity": 1,
                    "unit_price": 12.0,
                    "order_ts": "2026-11-15 09:00:00",
                },
            ]
        )

        valid, rejected, reason_counts = self.mod.validate_orders(df)

        self.assertEqual(len(valid), 1)
        self.assertEqual(valid["order_id"].tolist(), ["dup-1"])
        self.assertEqual(len(rejected), 3)
        self.assertEqual(reason_counts["duplicate_order_id"], 1)
        self.assertEqual(reason_counts["missing_order_id"], 2)

    def test_missing_required_columns(self):
        df = pd.DataFrame([{"order_id": "o1", "customer_id": 1001}])
        self.mod.wr.s3.read_csv.return_value = df

        report = self.mod.process_file("test-bucket", "raw/orders/bad_cols.csv")

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "ValueError")
        self.assertIn("Missing required columns", report["error"])
        self.mod.s3.put_object.assert_called_once()
        self.assertEqual(
            self.mod.s3.put_object.call_args.kwargs["Key"],
            "reports/bad_cols.json",
        )

    def test_empty_csv_zero_bytes_and_header_only(self):
        # 0-byte CSV raises EmptyDataError in pandas/awswrangler
        self.mod.wr.s3.read_csv.side_effect = pd.errors.EmptyDataError("No columns to parse")
        report_zero = self.mod.process_file("test-bucket", "raw/orders/empty_zero.csv")
        self.assertEqual(report_zero["status"], "failed")
        self.assertEqual(report_zero["error_type"], "EmptyDataError")

        # Header-only CSV yields empty DataFrame
        self.mod.wr.s3.read_csv.side_effect = None
        self.mod.wr.s3.read_csv.return_value = pd.DataFrame(columns=self.mod.REQUIRED_COLUMNS)
        report_header = self.mod.process_file("test-bucket", "raw/orders/empty_header.csv")
        self.assertEqual(report_header["status"], "failed")
        self.assertEqual(report_header["error_type"], "ValueError")
        self.assertIn("no data rows", report_header["error"])

    def test_malformed_csv(self):
        def raise_parser_error(**kwargs):
            return pd.read_csv(StringIO('order_id,customer_id\n"unclosed quote,123\n'))

        self.mod.wr.s3.read_csv.side_effect = raise_parser_error

        report = self.mod.process_file("test-bucket", "raw/orders/malformed.csv")

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "ParserError")

    def test_process_file_outputs_and_sns_threshold(self):
        df = pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "customer_id": 1001,
                    "category": "Home",
                    "quantity": 1,
                    "unit_price": 10.0,
                    "order_ts": "2026-11-15 08:00:00",
                },
                {
                    "order_id": "o2",
                    "customer_id": None,
                    "category": "Home",
                    "quantity": 1,
                    "unit_price": 10.0,
                    "order_ts": "2026-11-15 08:00:00",
                },
            ]
        )
        self.mod.wr.s3.read_csv.return_value = df

        with patch.dict(
            os.environ,
            {
                "SNS_TOPIC_ARN": "arn:aws:sns:ap-southeast-2:123456789012:orders-etl-alerts",
                "REJECT_RATE_THRESHOLD": "0.10",
            },
        ):
            report = self.mod.process_file("test-bucket", "raw/orders/orders_2026-11-15.csv")

        self.assertEqual(report["status"], "processed")
        self.assertEqual(report["reject_rate"], 0.5)

        to_parquet_kwargs = self.mod.wr.s3.to_parquet.call_args.kwargs
        self.assertEqual(to_parquet_kwargs["mode"], "overwrite_partitions")
        self.assertEqual(to_parquet_kwargs["partition_cols"], ["order_date"])
        self.assertEqual(to_parquet_kwargs["compression"], "snappy")

        # Quarantine CSV and quality JSON both uploaded
        put_keys = [c.kwargs["Key"] for c in self.mod.s3.put_object.call_args_list]
        self.assertIn("quarantine/orders/orders_2026-11-15.csv", put_keys)
        self.assertIn("reports/orders_2026-11-15.json", put_keys)

        # Alert published with filename and rejection info
        self.mod.sns.publish.assert_called_once()
        pub_kwargs = self.mod.sns.publish.call_args.kwargs
        self.assertIn("orders_2026-11-15.csv", pub_kwargs["Subject"])
        payload = json.loads(pub_kwargs["Message"])
        self.assertEqual(payload["rejected_rows"], 1)
        self.assertEqual(payload["reject_rate"], 0.5)

    def test_no_sns_alert_below_threshold(self):
        rows = [
            {
                "order_id": f"o{i}",
                "customer_id": 1000 + i,
                "category": "Books",
                "quantity": 1,
                "unit_price": 20.0,
                "order_ts": "2026-11-15 08:00:00",
            }
            for i in range(20)
        ]
        rows[0]["quantity"] = -1  # 1/20 = 0.05 < 0.10
        self.mod.wr.s3.read_csv.return_value = pd.DataFrame(rows)

        with patch.dict(
            os.environ,
            {
                "SNS_TOPIC_ARN": "arn:aws:sns:ap-southeast-2:123456789012:orders-etl-alerts",
                "REJECT_RATE_THRESHOLD": "0.10",
            },
        ):
            report = self.mod.process_file("test-bucket", "raw/orders/orders_2026-11-15.csv")

        self.assertEqual(report["reject_rate"], 0.05)
        self.mod.sns.publish.assert_not_called()

    def test_sns_publish_error_is_logged_and_raised(self):
        self.mod.wr.s3.read_csv.return_value = pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "customer_id": None,
                    "category": "Home",
                    "quantity": 1,
                    "unit_price": 10.0,
                    "order_ts": "2026-11-15 08:00:00",
                }
            ]
        )
        self.mod.sns.publish.side_effect = RuntimeError("SNS access denied")

        with patch.dict(
            os.environ,
            {
                "SNS_TOPIC_ARN": "arn:aws:sns:ap-southeast-2:123456789012:orders-etl-alerts",
                "REJECT_RATE_THRESHOLD": "0.10",
            },
        ):
            with self.assertLogs(self.mod.logger, level="ERROR") as logs:
                with self.assertRaises(RuntimeError):
                    self.mod.process_file("test-bucket", "raw/orders/orders_2026-11-15.csv")

        self.assertTrue(any("Failed to publish SNS alert" in msg for msg in logs.output))

    def test_multi_record_handler_filters_and_decodes(self):
        event = {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": "b1"},
                        "object": {"key": "raw/orders/orders_2026-11-15.csv"},
                    },
                },
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": "b1"},
                        "object": {"key": "raw/orders/ignore.txt"},
                    },
                },
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": "b1"},
                        "object": {"key": "curated/orders/other.csv"},
                    },
                },
                {
                    "eventSource": "aws:sqs",
                    "body": "{}",
                },
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": "b1"},
                        "object": {"key": "raw/orders/orders+2026-11-16.csv"},
                    },
                },
            ]
        }

        with patch.object(
            self.mod,
            "process_file",
            side_effect=[{"source_file": "a"}, {"source_file": "b"}],
        ) as mock_process:
            res = self.mod.handler(event, None)

        self.assertEqual(res["processed_files"], 2)
        self.assertEqual(
            [c.args for c in mock_process.call_args_list],
            [
                ("b1", "raw/orders/orders_2026-11-15.csv"),
                ("b1", "raw/orders/orders 2026-11-16.csv"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
