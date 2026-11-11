import json
import os
import unittest
from datetime import date, datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

from dashboard.athena import (
    CACHE_TTL_SECONDS,
    AwsAccessError,
    create_aws_session,
    execute_athena_query,
    validate_date_range,
)
from dashboard.insights import fetch_latest_insights_report
from dashboard.quality import _extract_report_date, fetch_quality_summary


class TestDashboardHelpers(unittest.TestCase):
    def test_validate_date_range(self):
        self.assertEqual(
            validate_date_range(date(2026, 11, 10), date(2026, 11, 11)),
            ("2026-11-10", "2026-11-11"),
        )
        with self.assertRaises(ValueError):
            validate_date_range("2026-11-11", "2026-11-10")
        with self.assertRaises(ValueError):
            validate_date_range("2026/11/10", "2026-11-11")

    def test_execute_athena_query_requires_order_date_filter(self):
        with self.assertRaises(ValueError):
            execute_athena_query("SELECT COUNT(*) FROM orders_analytics_db.orders;")

    def test_execute_athena_query_pagination_and_empty(self):
        mock_athena = MagicMock()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-123"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {
                    "DataScannedInBytes": 2048,
                    "EngineExecutionTimeInMillis": 250,
                },
            }
        }
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "ResultSet": {
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "order_date", "Type": "varchar"},
                            {"Name": "total_orders", "Type": "bigint"},
                        ]
                    },
                    "Rows": [
                        {"Data": [{"VarCharValue": "order_date"}, {"VarCharValue": "total_orders"}]},
                        {"Data": [{"VarCharValue": "2026-11-10"}, {"VarCharValue": "4800"}]},
                    ],
                }
            },
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "2026-11-11"}, {"VarCharValue": "4800"}]},
                    ]
                }
            },
        ]
        mock_athena.get_paginator.return_value = paginator

        mock_session = MagicMock()
        mock_session.client.return_value = mock_athena

        with patch("dashboard.athena.create_aws_session", return_value=mock_session):
            res = execute_athena_query(
                "SELECT order_date, COUNT(*) AS total_orders FROM orders_analytics_db.orders "
                "WHERE order_date BETWEEN '2026-11-10' AND '2026-11-11' GROUP BY order_date;"
            )

        self.assertEqual(res.query_execution_id, "qid-123")
        self.assertEqual(res.data_scanned_bytes, 2048)
        self.assertEqual(len(res.df), 2)
        self.assertEqual(res.df["total_orders"].tolist(), [4800, 4800])

    def test_execute_athena_query_failed_raises(self):
        mock_athena = MagicMock()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-fail"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "Syntax error"},
            }
        }
        mock_session = MagicMock()
        mock_session.client.return_value = mock_athena

        with patch("dashboard.athena.create_aws_session", return_value=mock_session):
            with self.assertRaises(AwsAccessError) as ctx:
                execute_athena_query(
                    "SELECT * FROM orders_analytics_db.orders WHERE order_date = '2026-11-10';"
                )
        self.assertIn("Syntax error", str(ctx.exception))

    def test_extract_report_date_filters_unrelated_keys(self):
        self.assertEqual(
            _extract_report_date("reports/orders_2026-11-10.json"),
            date(2026, 11, 10),
        )
        self.assertIsNone(_extract_report_date("reports/insights_2026-11-10.json"))
        self.assertIsNone(_extract_report_date("reports/orders_bad.json"))

    def test_fetch_quality_summary_handles_missing_and_malformed_reports(self):
        valid_payload = json.dumps(
            {
                "status": "processed",
                "source_file": "orders_2026-11-10.csv",
                "total_rows": 100,
                "valid_rows": 90,
                "rejected_rows": 10,
                "reason_counts": {
                    "missing_order_id": 0,
                    "missing_customer_id": 5,
                    "invalid_quantity": 5,
                    "invalid_unit_price": 0,
                    "missing_order_ts": 0,
                    "missing_category": 0,
                    "duplicate_order_id": 0,
                },
                "processed_at": "2026-11-10T00:00:00Z",
            }
        ).encode("utf-8")

        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "reports/orders_2026-11-10.json"},
                    {"Key": "reports/orders_2026-11-11.json"},
                    {"Key": "reports/insights_2026-11-10.json"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = paginator
        mock_s3.get_object.side_effect = [
            {"Body": BytesIO(valid_payload)},
            {"Body": BytesIO(b"{bad json")},
        ]

        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3

        with patch("dashboard.quality.create_aws_session", return_value=mock_session):
            summary = fetch_quality_summary("2026-11-10", "2026-11-12")

        self.assertEqual(summary.selected_dates_count, 3)
        self.assertEqual(summary.reports_found_count, 1)
        self.assertEqual(summary.missing_dates, ["2026-11-11", "2026-11-12"])
        self.assertEqual(len(summary.invalid_reports), 1)
        self.assertEqual(summary.total_input_rows, 100)
        self.assertEqual(summary.total_rejected_rows, 10)
        self.assertAlmostEqual(summary.overall_reject_rate_pct, 10.0)

    def test_fetch_latest_insights_report_missing(self):
        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = paginator
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3

        with patch("dashboard.insights.create_aws_session", return_value=mock_session):
            rep = fetch_latest_insights_report(latest_partition_date=date(2026, 11, 11))

        self.assertFalse(rep.found)
        self.assertIsNone(rep.error_message)

    def test_fetch_latest_insights_report_stale_and_valid(self):
        payload = json.dumps(
            {
                "status": "generated",
                "report_date": "2026-11-10",
                "generated_at": "2026-11-10T06:30:00+00:00",
                "gemini_model": "gemini-3-flash-preview",
                "summary": "Daily summary text.",
                "observations": ["Obs 1"],
                "recommendations": ["Rec 1"],
                "opportunities": ["Opp 1"],
                "data_coverage_and_limitations": {"caveats": ["Synthetic data"]},
            }
        ).encode("utf-8")

        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "reports/insights/orders_insights_2026-11-10.json"}]}
        ]
        mock_s3.get_paginator.return_value = paginator
        mock_s3.get_object.return_value = {"Body": BytesIO(payload)}
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3

        with patch("dashboard.insights.create_aws_session", return_value=mock_session):
            rep = fetch_latest_insights_report(
                latest_partition_date=date(2026, 11, 11),
                now_utc=datetime(2026, 11, 12, 0, 0, tzinfo=timezone.utc),
            )

        self.assertTrue(rep.found)
        self.assertTrue(rep.is_stale)
        self.assertIn("behind latest completed partition", rep.stale_reason)
        self.assertIn("freshness window", rep.stale_reason)
        self.assertEqual(rep.gemini_model, "gemini-3-flash-preview")
        self.assertEqual(rep.recommendations, ["Rec 1"])

    def test_fetch_latest_insights_nov11_not_stale_when_nov12_partition_exists(self):
        payload = json.dumps(
            {
                "status": "generated",
                "report_date": "2026-11-11",
                "generated_at": "2026-11-11T20:38:42+00:00",
                "gemini_model": "gemini-3-flash-preview",
                "summary": "Daily summary for Nov 11.",
                "observations": ["Obs 1"],
                "recommendations": ["Rec 1"],
            }
        ).encode("utf-8")

        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "reports/insights/orders_insights_2026-11-11.json"}]}
        ]
        mock_s3.get_paginator.return_value = paginator
        mock_s3.get_object.return_value = {"Body": BytesIO(payload)}
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3

        # On 2026-11-12 02:15 IST (2026-11-11 20:45 UTC), even if a 2026-11-12 Glue partition exists,
        # the latest completed reporting date is 2026-11-11, so Nov 11 insights must NOT be marked stale.
        with patch("dashboard.insights.create_aws_session", return_value=mock_session):
            rep = fetch_latest_insights_report(
                latest_partition_date=date(2026, 11, 12),
                now_utc=datetime(2026, 11, 11, 20, 45, 0, tzinfo=timezone.utc),
            )

        self.assertTrue(rep.found)
        self.assertFalse(rep.is_stale)
        self.assertIsNone(rep.stale_reason)
        self.assertEqual(rep.report_date, "2026-11-11")

    def test_fetch_latest_insights_report_malformed_json(self):
        mock_s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "reports/insights/orders_insights_2026-11-11.json"}]}
        ]
        mock_s3.get_paginator.return_value = paginator
        mock_s3.get_object.return_value = {"Body": BytesIO(b"{bad json")}
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3

        with patch("dashboard.insights.create_aws_session", return_value=mock_session):
            rep = fetch_latest_insights_report(latest_partition_date=date(2026, 11, 11))

        self.assertFalse(rep.found)
        self.assertIn("Malformed JSON", rep.error_message)

    def test_cache_ttl_defaults_to_one_hour(self):
        self.assertEqual(CACHE_TTL_SECONDS, 3600)

    def test_create_aws_session_render_env_creds_and_local_profile(self):
        from botocore.exceptions import ProfileNotFound

        with patch("dashboard.athena.load_dotenv"), patch(
            "dashboard.athena.boto3.Session"
        ) as mock_session:
            # Case 1: Render env-var credentials without AWS_PROFILE
            with patch.dict(
                os.environ,
                {
                    "AWS_ACCESS_KEY_ID": "test-key",
                    "AWS_SECRET_ACCESS_KEY": "test-secret",
                    "AWS_REGION": "ap-southeast-2",
                },
                clear=True,
            ):
                create_aws_session()
            mock_session.assert_called_once_with(region_name="ap-southeast-2")

            # Case 2: Blank AWS_PROFILE omits profile_name
            mock_session.reset_mock()
            with patch.dict(
                os.environ,
                {"AWS_PROFILE": "", "AWS_REGION": "ap-southeast-2"},
                clear=True,
            ):
                create_aws_session()
            mock_session.assert_called_once_with(region_name="ap-southeast-2")

            # Case 3: Local CLI profile takes precedence over stale OS env keys when AWS_PROFILE is set
            mock_session.reset_mock()
            with patch.dict(
                os.environ,
                {
                    "AWS_ACCESS_KEY_ID": "stale-key",
                    "AWS_SECRET_ACCESS_KEY": "stale-secret",
                    "AWS_PROFILE": "etl-project",
                    "AWS_REGION": "ap-southeast-2",
                },
                clear=True,
            ):
                create_aws_session()
            mock_session.assert_called_once_with(
                profile_name="etl-project",
                region_name="ap-southeast-2",
            )

            # Case 4: If profile is missing on Render but env creds exist, falls back cleanly
            mock_session.reset_mock()
            mock_session.side_effect = [ProfileNotFound(profile="etl-project"), MagicMock()]
            with patch.dict(
                os.environ,
                {
                    "AWS_ACCESS_KEY_ID": "render-key",
                    "AWS_SECRET_ACCESS_KEY": "render-secret",
                    "AWS_PROFILE": "etl-project",
                    "AWS_REGION": "ap-southeast-2",
                },
                clear=True,
            ):
                create_aws_session()
            self.assertEqual(mock_session.call_count, 2)
            self.assertEqual(mock_session.call_args_list[1].kwargs, {"region_name": "ap-southeast-2"})


if __name__ == "__main__":
    unittest.main()

