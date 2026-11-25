import importlib.util
import pathlib
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("compare_idm", pathlib.Path(__file__).with_name("compare_idm.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class FullTransferTests(unittest.TestCase):
    def run_fake(self, data, expected_size, expected_hash=None, exit_code=0):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = pathlib.Path(temporary.name)
        existing = directory / "previous.iso"
        existing.write_bytes(b"preserve previous user file")

        class Process:
            def __init__(self, args, **kwargs):
                assert kwargs["shell"] is False
                destination = pathlib.Path(args[args.index("--out") + 1])
                destination.write_bytes(data)

            def wait(self, **kwargs):
                return exit_code

        with patch.object(benchmark.subprocess, "Popen", Process):
            try:
                return benchmark.run_one("nibiru", pathlib.Path("engine"), "https://example.test/iso?token=private",
                                         directory, {"size_bytes": expected_size}, 16, 10, False, expected_hash)
            finally:
                self.assertEqual(existing.read_bytes(), b"preserve previous user file")

    def test_success_requires_exact_size_and_hash_and_removes_only_its_own_iso(self):
        row = self.run_fake(b"valid payload", 13)
        self.assertEqual(row["size_bytes"], 13)
        self.assertEqual(row["sha256"], benchmark.hashlib.sha256(b"valid payload").hexdigest())
        self.assertIsNone(row["output"])

    def test_truncated_transfer_never_becomes_a_speed_result(self):
        with self.assertRaisesRegex(ValueError, "incompleta"):
            self.run_fake(b"short", 100)

    def test_correct_size_with_wrong_hash_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "SHA-256 divergente"):
            self.run_fake(b"payload", 7, "0" * 64)

    def test_process_failure_is_rejected_even_when_file_length_matches(self):
        with self.assertRaisesRegex(ValueError, "codigo 1"):
            self.run_fake(b"payload", 7, exit_code=1)

    def test_comparison_uses_medians_and_declared_tolerance(self):
        rows = [{"engine": name, "elapsed_seconds": elapsed}
                for name, values in [("idm", [10, 11, 200]), ("nibiru", [10, 10.5, 12])]
                for elapsed in values]
        summary, comparison = benchmark.summarize(rows, ["idm", "nibiru"], 0.05)
        self.assertEqual(summary["idm"]["median_seconds"], 11)
        self.assertEqual(comparison["result"], "within_tolerance")


if __name__ == "__main__":
    unittest.main()
