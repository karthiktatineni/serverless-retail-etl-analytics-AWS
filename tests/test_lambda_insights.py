import importlib.util
import io
import json
import os
import unittest
import urllib.error
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAMBDA_INSIGHTS_PATH = PROJECT_ROOT / "src" / "lambda_insights" / "lambda_function.py"


def load_insights_module():
    spec = importlib.util.spec_from_file_location(
        "lambda_insights_under_test",
        LAMBDA_INSIGHTS_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._SSM_CACHE.clear()
    return module


def sample_gemini_json():
    return {
        "executive_summary": "Simulated orders reached 4,800 with revenue of 1,868,497.74.",
        "numerical_observations": ["Total orders were 4,800 across 3,606 simulated customers."],
        "revenue_and_order_trends": ["Revenue rose by 28,881.35 (+1.57%) vs 2026-11-10."],
        "category_performance": ["Books led category revenue."],
        "customer_concentration": ["Top 10 simulated customers accounted for <2% of revenue."],
        "opportunities": ["Monitor category mix shifts over consecutive simulated days."],
        "recommendations": ["Continue automated validation monitoring on daily batches."],
        "limitations_and_caveats": ["All figures represent synthetic demonstration orders."],
    }


class TestLambdaInsights(unittest.TestCase):
    def setUp(self):
        self.mod = load_insights_module()

    def test_resolve_report_date_explicit_and_eventbridge_ist(self):
        self.assertEqual(
            self.mod.resolve_report_date({"date": "2026-11-11"}),
            "2026-11-11",
        )
        self.assertEqual(
            self.mod.resolve_report_date({"report_date": "2026-11-10"}),
            "2026-11-10",
        )
        # EventBridge Scheduler empty/scheduler payload uses yesterday in Asia/Kolkata
        now_ist = datetime(2026, 11, 12, 12, 0, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
        self.assertEqual(
            self.mod.resolve_report_date({"source": "aws.scheduler"}, now_ist=now_ist),
            "2026-11-11",
        )
        with self.assertRaises(ValueError):
            self.mod.resolve_report_date({"date": "11-11-2026"})

    def test_athena_query_failure_raises(self):
        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-fail"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "Table not found"}
            }
        }
        with self.assertRaises(self.mod.AthenaExecutionError) as ctx:
            self.mod.execute_athena_query(
                athena,
                "SELECT * FROM db.t WHERE order_date = '2026-11-11';",
                "db",
                "wg",
                poll_interval=0.0,
            )
        self.assertIn("Table not found", str(ctx.exception))

    def test_athena_query_timeout_stops_execution_and_raises(self):
        athena = MagicMock()
        athena.start_query_execution.return_value = {"QueryExecutionId": "qid-timeout"}
        athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }
        with patch.object(self.mod.time, "monotonic", side_effect=[0.0, 2.0]):
            with self.assertRaises(self.mod.AthenaExecutionError) as ctx:
                self.mod.execute_athena_query(
                    athena,
                    "SELECT * FROM db.t WHERE order_date = '2026-11-11';",
                    "db",
                    "wg",
                    timeout_seconds=1,
                    poll_interval=0.0,
                )
        athena.stop_query_execution.assert_called_once_with(QueryExecutionId="qid-timeout")
        self.assertIn("timed out", str(ctx.exception))

    def test_gemini_success_malformed_rate_limit_and_failure(self):
        payload = {"report_date": "2026-11-11", "kpis": {"total_orders": 10}}

        # 1. Success
        ok_body = json.dumps(
            {"candidates": [{"content": {"parts": [{"text": json.dumps(sample_gemini_json())}]}}]}
        ).encode("utf-8")
        ok_resp = MagicMock()
        ok_resp.__enter__.return_value.read.return_value = ok_body
        with patch.object(self.mod.urllib.request, "urlopen", return_value=ok_resp):
            res = self.mod.generate_gemini_insights("secret-key", "gemini-3-flash-preview", payload)
        self.assertIn("executive_summary", res)

        # 2. Malformed JSON from Gemini
        bad_body = json.dumps(
            {"candidates": [{"content": {"parts": [{"text": "{not valid json"}]}}]}
        ).encode("utf-8")
        bad_resp = MagicMock()
        bad_resp.__enter__.return_value.read.return_value = bad_body
        with patch.object(self.mod.urllib.request, "urlopen", return_value=bad_resp):
            with self.assertRaises(self.mod.GeminiIntegrationError):
                self.mod.generate_gemini_insights("secret-key", "gemini-3-flash-preview", payload)

        # 3. Rate limit HTTP 429 recovers on retry
        rate_err = urllib.error.HTTPError(
            url="https://example.com",
            code=429,
            msg="Too Many Requests",
            hdrs=None,
            fp=io.BytesIO(b"rate limited"),
        )
        with patch.object(
            self.mod.urllib.request,
            "urlopen",
            side_effect=[rate_err, ok_resp],
        ), patch.object(self.mod.time, "sleep"):
            retried = self.mod.generate_gemini_insights("secret-key", "gemini-3-flash-preview", payload)
        self.assertIn("recommendations", retried)

        # 4. Persistent API failure (400) raises without retrying
        bad_req_err = urllib.error.HTTPError(
            url="https://example.com",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=io.BytesIO(b"bad request"),
        )
        with patch.object(self.mod.urllib.request, "urlopen", side_effect=bad_req_err):
            with self.assertRaises(self.mod.GeminiIntegrationError):
                self.mod.generate_gemini_insights("secret-key", "gemini-3-flash-preview", payload)

    def _mock_aws_clients(self, partitions, athena_pages_by_query, quality_body=None):
        glue = MagicMock()
        glue_paginator = MagicMock()
        glue_paginator.paginate.return_value = [
            {"Partitions": [{"Values": [p]} for p in partitions]}
        ]
        glue.get_paginator.return_value = glue_paginator

        athena = MagicMock()
        qids = ["qid-kpis", "qid-cats", "qid-custs"]
        athena.start_query_execution.side_effect = [
            {"QueryExecutionId": qid} for qid in qids
        ]
        athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedInBytes": 1024},
            }
        }
        res_paginator = MagicMock()
        res_paginator.paginate.side_effect = athena_pages_by_query
        athena.get_paginator.return_value = res_paginator

        s3 = MagicMock()
        if quality_body is not None:
            s3.get_object.return_value = {"Body": io.BytesIO(quality_body)}
        else:
            from botocore.exceptions import ClientError

            s3.get_object.side_effect = ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                "GetObject",
            )

        ssm = MagicMock()
        ssm.get_parameter.return_value = {"Parameter": {"Value": "dummy-gemini-key"}}

        sns = MagicMock()
        sns.publish.return_value = {"MessageId": "sns-msg-123"}

        clients = {"glue": glue, "athena": athena, "s3": s3, "ssm": ssm, "sns": sns}
        return clients

    def test_missing_partition_skips_cleanly(self):
        clients = self._mock_aws_clients(partitions=["2026-11-10"], athena_pages_by_query=[])
        with patch.object(self.mod.boto3, "client", side_effect=lambda name: clients[name]):
            res = self.mod.handler({"date": "2026-11-11"}, None)

        self.assertEqual(res["status"], "skipped_missing_partition")
        clients["s3"].put_object.assert_not_called()
        clients["sns"].publish.assert_not_called()

    def test_end_to_end_handler_s3_sns_and_idempotent_reinvoke(self):
        kpi_pages = [
            {
                "ResultSet": {
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "order_date"},
                            {"Name": "total_orders"},
                            {"Name": "unique_customers"},
                            {"Name": "units_sold"},
                            {"Name": "total_revenue"},
                            {"Name": "average_order_value"},
                        ]
                    },
                    "Rows": [
                        {"Data": [{"VarCharValue": c} for c in ["order_date", "total_orders", "unique_customers", "units_sold", "total_revenue", "average_order_value"]]},
                        {"Data": [{"VarCharValue": v} for v in ["2026-11-10", "4800", "3589", "12084", "1839616.39", "383.25"]]},
                        {"Data": [{"VarCharValue": v} for v in ["2026-11-11", "4800", "3606", "11993", "1868497.74", "389.27"]]},
                    ],
                }
            }
        ]
        cat_pages = [
            {
                "ResultSet": {
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "category"},
                            {"Name": "total_orders"},
                            {"Name": "units_sold"},
                            {"Name": "revenue"},
                        ]
                    },
                    "Rows": [
                        {"Data": [{"VarCharValue": c} for c in ["category", "total_orders", "units_sold", "revenue"]]},
                        {"Data": [{"VarCharValue": v} for v in ["Books", "820", "2050", "320000.00"]]},
                    ],
                }
            }
        ]
        cust_pages = [
            {
                "ResultSet": {
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {"Name": "customer_id"},
                            {"Name": "total_orders"},
                            {"Name": "units_sold"},
                            {"Name": "revenue"},
                        ]
                    },
                    "Rows": [
                        {"Data": [{"VarCharValue": c} for c in ["customer_id", "total_orders", "units_sold", "revenue"]]},
                        {"Data": [{"VarCharValue": v} for v in ["6703", "3", "9", "2500.00"]]},
                    ],
                }
            }
        ]

        env = {
            "S3_BUCKET": "test-bucket",
            "INSIGHTS_PREFIX": "reports/insights/",
            "GEMINI_MODEL": "gemini-3-flash-preview",
            "SNS_TOPIC_ARN": "arn:aws:sns:ap-southeast-2:123456789012:orders-etl-daily-insights",
        }

        for _ in range(2):
            clients = self._mock_aws_clients(
                partitions=["2026-11-10", "2026-11-11"],
                athena_pages_by_query=[kpi_pages, cat_pages, cust_pages],
            )
            with patch.dict(os.environ, env), patch.object(
                self.mod.boto3, "client", side_effect=lambda name: clients[name]
            ), patch.object(
                self.mod, "generate_gemini_insights", return_value=sample_gemini_json()
            ):
                res = self.mod.handler({"date": "2026-11-11"}, None)

            self.assertEqual(res["status"], "generated")
            self.assertEqual(
                res["s3_report_uri"],
                "s3://test-bucket/reports/insights/orders_insights_2026-11-11.json",
            )
            clients["s3"].put_object.assert_called_once()
            put_kwargs = clients["s3"].put_object.call_args.kwargs
            self.assertEqual(
                put_kwargs["Key"],
                "reports/insights/orders_insights_2026-11-11.json",
            )
            saved = json.loads(put_kwargs["Body"].decode("utf-8"))
            self.assertEqual(saved["kpis"]["total_orders"], 4800)
            self.assertEqual(saved["previous_day_comparison"]["previous_date"], "2026-11-10")
            self.assertAlmostEqual(
                saved["previous_day_comparison"]["deltas"]["total_revenue_diff"],
                28881.35,
            )
            clients["sns"].publish.assert_called_once()
            pub_kwargs = clients["sns"].publish.call_args.kwargs
            self.assertIn("2026-11-11", pub_kwargs["Subject"])
            self.assertIn("orders_insights_2026-11-11.json", pub_kwargs["Message"])


if __name__ == "__main__":
    unittest.main()

