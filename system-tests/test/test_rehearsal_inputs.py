import copy
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("rehearsal", ROOT / "system-tests/scripts/rehearsal-inputs.py")
rehearsal = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rehearsal)
CONFIG = rehearsal.release.load(ROOT / "system-tests/components.yml")


class RehearsalInputsTest(unittest.TestCase):
    def setUp(self):
        self.manifest = rehearsal.release.load(ROOT / "rehearsals/1052-candidate-runtime.json")
        self.source = {"schemaVersion": 1, "components": {}, "testHarnesses": {}, "images": {}, "candidates": []}
        for name, component in CONFIG["components"].items():
            pin = {"repository": component["repository"], "sha": "a" * 40}
            if component["kind"] == "maven":
                pin.update(mavenVersion="26.11.1-main." + "a" * 12, manifestDigest="sha256:" + "b" * 64)
                self.source["components"][name] = pin
            else:
                self.source["testHarnesses"][name] = pin
            self.source["candidates"].append({"component": name})
        self.run = {"path": ".github/workflows/system-test-full-train.yml", "head_branch": "main", "head_sha": "a" * 40, "event": "workflow_dispatch"}
        self.job = {"name": "Materialize immutable Maven inputs", "conclusion": "success"}
        self.artifacts = [{"name": rehearsal.ARTIFACT_NAMES[key], "expired": False, **value} for key, value in self.manifest["sourceArtifacts"].items()]

    def api(self, path, *args):
        if "/jobs?" in path:
            return json.dumps({"jobs": [self.job]})
        if "/artifacts?" in path:
            return json.dumps({"artifacts": self.artifacts})
        if "/contents/" in path:
            return json.dumps(CONFIG)
        return json.dumps(self.run)

    def test_source_materialization_is_provenance_not_full_coverage(self):
        # A Search-only input run is reusable, but its result is not rehearsal coverage.
        rehearsal.validate_source(self.manifest, CONFIG, self.api)
        self.job["conclusion"] = "failure"
        with self.assertRaisesRegex(ValueError, "materialization did not succeed"):
            rehearsal.validate_source(self.manifest, CONFIG, self.api)

    def test_source_branch_and_artifact_identity_cannot_drift(self):
        for field, value in (("head_branch", "feature"), ("path", ".github/workflows/other.yml"), ("event", "pull_request")):
            old = self.run[field]
            self.run[field] = value
            with self.assertRaisesRegex(ValueError, "trusted main"):
                rehearsal.validate_source(self.manifest, CONFIG, self.api)
            self.run[field] = old
        for field, value in (("expired", True), ("id", 1), ("digest", "sha256:" + "c" * 64)):
            old = self.artifacts[0][field]
            self.artifacts[0][field] = value
            with self.assertRaisesRegex(ValueError, "artifact is missing"):
                rehearsal.validate_source(self.manifest, CONFIG, self.api)
            self.artifacts[0][field] = old

    def test_mixed_overlay_retains_runtime_candidate_and_frozen_harnesses(self):
        original = copy.deepcopy(self.source)
        mixed = rehearsal.overlay(self.source, self.manifest, CONFIG)
        self.assertEqual(original, self.source)
        self.assertEqual("26.9.4", mixed["components"]["expansions"]["mavenVersion"])
        self.assertNotIn("manifestDigest", mixed["components"]["expansions"])
        self.assertEqual(original["components"]["runtime"], mixed["components"]["runtime"])
        self.assertEqual(original["testHarnesses"], mixed["testHarnesses"])
        self.assertNotIn("expansions", [pin["component"] for pin in mixed["candidates"]])

    def test_floating_versions_and_false_stable_overrides_fail(self):
        for version in ("26.11.1-SNAPSHOT", "LATEST", "26.11.1-main.bbbbbbbbbbbb"):
            self.source["components"]["runtime"]["mavenVersion"] = version
            with self.assertRaisesRegex(ValueError, "non-immutable"):
                rehearsal.overlay(self.source, self.manifest, CONFIG)
        self.source["components"]["runtime"]["mavenVersion"] = "26.11.1-main." + "a" * 12
        self.manifest["stableOverrides"]["expansions"]["mavenVersion"] = "26.11.1-SNAPSHOT"
        with self.assertRaisesRegex(ValueError, "stable version"):
            rehearsal.overlay(self.source, self.manifest, CONFIG)

    def test_streamed_archive_bytes_are_checksummed(self):
        artifact = {"id": 1, "digest": "sha256:" + hashlib.sha256(b"trusted").hexdigest()}
        def downloaded(command, stdout, check):
            stdout.write(b"trusted")
        with tempfile.TemporaryDirectory() as directory, patch.object(rehearsal.subprocess, "run", downloaded):
            destination = Path(directory) / "artifact.zip"
            rehearsal.download_artifact(artifact, destination)
            artifact["digest"] = "sha256:" + "0" * 64
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                rehearsal.download_artifact(artifact, destination)

    def test_merge_rejects_two_versions_of_one_owned_coordinate(self):
        artifact = {"groupId": "org.pipelineframework", "artifactId": "example", "packaging": "jar", "coordinate": "org.pipelineframework:example:1.0.0:jar"}
        candidate = {"artifacts": [artifact]}
        stable = {"artifacts": [{**artifact, "coordinate": "org.pipelineframework:example:2.0.0:jar"}]}
        with self.assertRaisesRegex(ValueError, "duplicate rehearsal coordinate"):
            rehearsal.merge_plans(candidate, stable)


if __name__ == "__main__":
    unittest.main()
