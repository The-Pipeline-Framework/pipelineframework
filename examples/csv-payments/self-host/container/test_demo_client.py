#!/usr/bin/env python3
import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("demo-client.py")
MODULE_SPEC = importlib.util.spec_from_file_location("csv_payments_demo_client", MODULE_PATH)
demo_client = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(demo_client)


class DemoClientOutputValidationTest(unittest.TestCase):

    def test_generated_output_requires_each_input_id_once(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "payments_3.csv.out"
            output.write_text("CSV Id,Recipient\n1,one\n2,two\n3,three\n", encoding="utf-8")

            demo_client.assert_output_record_count(output, 3)

    def test_generated_output_rejects_duplicate_input_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "payments_3.csv.out"
            output.write_text("CSV Id,Recipient\n1,one\n2,two\n2,three\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "duplicate CSV Id"):
                demo_client.assert_output_record_count(output, 3)

    def test_generated_output_rejects_missing_input_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "payments_3.csv.out"
            output.write_text("CSV Id,Recipient\n1,one\n2,two\n4,four\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "does not preserve the generated input IDs"):
                demo_client.assert_output_record_count(output, 3)

    def test_wait_status_reports_remote_outcome_unknown_without_waiting_for_fixture_expiry(self):
        args = SimpleNamespace(
            base_url="http://coordinator:8082",
            tenant_id="tenant-1",
            control_plane_token="token",
        )
        record = {"status": "REMOTE_OUTCOME_UNKNOWN", "executionId": "exec-1"}

        with patch.object(demo_client, "request", return_value=record), \
                patch.object(demo_client.time, "time", side_effect=[100.0, 100.0]):
            with self.assertRaisesRegex(RuntimeError, "remote outcome unknown"):
                demo_client.wait_status(args, "exec-1", 300)


if __name__ == "__main__":
    unittest.main()
