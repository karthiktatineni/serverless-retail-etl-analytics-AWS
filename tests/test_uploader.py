from __future__ import annotations

import io
import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

from dashboard.uploader import (
    MAX_UPLOAD_SIZE_BYTES,
    UploadValidationError,
    generate_orders_csv_bytes,
    generate_orders_df,
    poll_for_quality_report,
    register_partition_via_athena,
    sanitize_filename,
    upload_csv_to_s3,
    validate_uploaded_csv,
)


class TestUploader(unittest.TestCase):
    def test_sanitize_filename(self):
        self.assertEqual(sanitize_filename("orders_2026-11-12.csv"), "orders_2026-11-12.csv")
        self.assertEqual(sanitize_filename("../../dangerous.csv"), "orders_dangerous.csv")
        self.assertEqual(sanitize_filename("sales data (v1).csv"), "orders_sales_data__v1_.csv")
        self.assertEqual(sanitize_filename("custom"), "orders_custom.csv")

    def test_validate_uploaded_csv_valid(self):
        csv_data = (
            "order_id,customer_id,category,quantity,unit_price,order_ts\n"
            "ORD-001,1001,Electronics,2,49.99,2026-11-12T10:00:00Z\n"
            "ORD-002,1002,Home,1,19.99,2026-11-12T11:00:00Z\n"
        ).encode("utf-8")

        result = validate_uploaded_csv(csv_data, "test.csv")
        self.assertTrue(result["valid"])
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["dates_detected"], ["2026-11-12"])
        self.assertEqual(len(result["sample_preview"]), 2)

    def test_validate_uploaded_csv_missing_columns(self):
        csv_data = (
            "order_id,quantity,unit_price\n"
            "ORD-001,2,49.99\n"
        ).encode("utf-8")

        with self.assertRaises(UploadValidationError) as ctx:
            validate_uploaded_csv(csv_data, "bad.csv")
        self.assertIn("missing required columns", str(ctx.exception).lower())

    def test_validate_uploaded_csv_empty(self):
        with self.assertRaises(UploadValidationError) as ctx:
            validate_uploaded_csv(b"", "empty.csv")
        self.assertIn("empty", str(ctx.exception).lower())

        header_only = b"order_id,customer_id,category,quantity,unit_price,order_ts\n"
        with self.assertRaises(UploadValidationError) as ctx:
            validate_uploaded_csv(header_only, "header.csv")
        self.assertIn("no data rows", str(ctx.exception).lower())

    def test_validate_uploaded_csv_oversized(self):
        oversized = b"a" * (MAX_UPLOAD_SIZE_BYTES + 10)
        with self.assertRaises(UploadValidationError) as ctx:
            validate_uploaded_csv(oversized, "huge.csv")
        self.assertIn("exceeds maximum allowed limit", str(ctx.exception).lower())

    @patch("dashboard.uploader.create_aws_session")
    def test_upload_csv_to_s3_success(self, mock_create_session):
        mock_s3 = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3
        mock_create_session.return_value = mock_session

        mock_s3.head_object.return_value = {
            "ContentLength": 100,
            "ETag": '"etag-123"',
            "ServerSideEncryption": "AES256",
        }

        meta = upload_csv_to_s3(
            file_bytes=b"sample,data\n1,2\n",
            filename="orders_2026-11-12.csv",
            bucket="test-bucket",
        )

        mock_s3.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="raw/orders/orders_2026-11-12.csv",
            Body=b"sample,data\n1,2\n",
            ContentType="text/csv",
            ServerSideEncryption="AES256",
        )
        self.assertEqual(meta["key"], "raw/orders/orders_2026-11-12.csv")
        self.assertEqual(meta["file_stem"], "orders_2026-11-12")

    @patch("dashboard.uploader.create_aws_session")
    def test_poll_for_quality_report_success(self, mock_create_session):
        mock_s3 = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3
        mock_create_session.return_value = mock_session

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({
            "status": "processed",
            "total_rows": 10,
            "valid_rows": 9,
            "rejected_rows": 1,
            "reject_rate": 0.1,
        }).encode("utf-8")
        mock_s3.get_object.return_value = {"Body": mock_body}

        report = poll_for_quality_report(
            bucket="test-bucket",
            file_stem="orders_2026-11-12",
            max_wait_seconds=5,
        )

        self.assertIsNotNone(report)
        self.assertEqual(report["status"], "processed")
        self.assertEqual(report["total_rows"], 10)

    @patch("dashboard.uploader.execute_athena_query")
    def test_register_partition_via_athena(self, mock_exec):
        mock_exec.return_value = MagicMock(query_execution_id="qid-999")

        qid = register_partition_via_athena(
            database="test_db",
            table="orders",
            order_date="2026-11-12",
            bucket="test-bucket",
            workgroup="test-wg",
        )

        self.assertEqual(qid, "qid-999")
        mock_exec.assert_called_once()
        call_kwargs = mock_exec.call_args[1]
        self.assertIn("ALTER TABLE test_db.orders ADD IF NOT EXISTS PARTITION (order_date = '2026-11-12')", call_kwargs["sql"])
        self.assertTrue(call_kwargs.get("allow_ddl"))

    def test_generate_orders_df(self):
        df = generate_orders_df(day="2026-11-12", rows=100, defect_rate=0.05)
        self.assertGreater(len(df), 100)
        self.assertIn("order_id", df.columns)
        self.assertIn("customer_id", df.columns)
        self.assertIn("category", df.columns)
        self.assertIn("quantity", df.columns)
        self.assertIn("unit_price", df.columns)
        self.assertIn("order_ts", df.columns)
        # Check invalid quantity was injected
        self.assertTrue((df["quantity"] == -1).any())
        # Check invalid unit price was injected
        self.assertTrue((df["unit_price"] == 0.0).any())

    def test_generate_orders_csv_bytes(self):
        csv_bytes, df, expected_defects = generate_orders_csv_bytes(day="2026-11-12", rows=50, defect_rate=0.1)
        self.assertIsInstance(csv_bytes, bytes)
        self.assertGreater(len(csv_bytes), 0)
        self.assertIn(b"order_id,customer_id,category,quantity,unit_price,order_ts", csv_bytes)
        self.assertEqual(expected_defects, 25)


if __name__ == "__main__":
    unittest.main()

