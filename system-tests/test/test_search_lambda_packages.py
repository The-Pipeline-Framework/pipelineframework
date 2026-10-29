"""Package boundaries without AWS or allocating a 250 MiB test payload."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location("sizes", Path(__file__).parents[1] / "scripts/validate-search-lambda-packages.py")
SIZES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SIZES)


class LambdaPackageSizeTest(unittest.TestCase):
    def test_valid_package_reports_actual_compressed_and_expanded_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "module/target/function.zip"
            path.parent.mkdir(parents=True)
            with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("lib/app.jar", b"x" * 1000)
            result = SIZES.inspect_package(path)
            self.assertEqual("module", result["module"])
            self.assertEqual(1000, result["expandedBytes"])
            self.assertEqual(path.stat().st_size, result["compressedBytes"])
            self.assertEqual("S3", result["upload"])

    def test_enforces_sum_at_expanded_boundary(self):
        for expanded in (51 * 1024 * 1024, SIZES.MAX_EXPANDED_BYTES, SIZES.MAX_EXPANDED_BYTES + 1):
            with self.subTest(expanded=expanded), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "module/target/function.zip"
                path.parent.mkdir(parents=True)
                path.write_bytes(b"metadata is substituted in this boundary test")
                entries = [SimpleNamespace(file_size=expanded // 2, is_dir=lambda: False),
                           SimpleNamespace(file_size=expanded - expanded // 2, is_dir=lambda: False)]
                with patch.object(SIZES.zipfile, "ZipFile") as archive:
                    archive.return_value.__enter__.return_value.infolist.return_value = entries
                    if expanded > SIZES.MAX_EXPANDED_BYTES:
                        with self.assertRaisesRegex(ValueError, "250 MiB"):
                            SIZES.inspect_package(path)
                    else:
                        self.assertEqual(expanded, SIZES.inspect_package(path)["expandedBytes"])

    def test_missing_corrupt_and_empty_archives_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "function.zip"
            with self.assertRaises(FileNotFoundError): SIZES.inspect_package(path)
            path.write_bytes(b"not a ZIP")
            with self.assertRaises(zipfile.BadZipFile): SIZES.inspect_package(path)
            with zipfile.ZipFile(path, "w"): pass
            with self.assertRaisesRegex(ValueError, "no files"): SIZES.inspect_package(path)
            with zipfile.ZipFile(path, "w") as archive: archive.mkdir("lib/")
            with self.assertRaisesRegex(ValueError, "no files"): SIZES.inspect_package(path)


if __name__ == "__main__": unittest.main()
