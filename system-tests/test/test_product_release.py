import copy
import hashlib
import importlib.util
import io
import json
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("product_release", ROOT / "system-tests/scripts/product-release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
CONFIG = release.load(ROOT / "system-tests/components.yml")


def manifest():
    components = {name: {"repository": value["repository"], "sha": "a" * 40, "mavenVersion": "26.10.1", "release": "reuse"} for name, value in CONFIG["components"].items() if value["kind"] == "maven"}
    harnesses = {name: {"repository": value["repository"], "sha": "c" * 40} for name, value in CONFIG["components"].items() if value["kind"] == "source"}
    return {"schemaVersion": 1, "productVersion": "26.12.3", "previousVersion": "26.10.1", "components": components, "testHarnesses": harnesses}


class ProductReleaseTest(unittest.TestCase):
    def setUp(self):
        self.selected = manifest()
        self.previous = copy.deepcopy(self.selected)
        self.previous.update(productVersion="26.10.1", previousVersion=None)
        self.selected["components"]["runtime"].update(mavenVersion="26.12.3", sha="b" * 40, release="new")

    def test_committed_selections_have_valid_ownership_and_previous_releases(self):
        for path in (ROOT / "releases").glob("*.json"):
            selected = release.selection(release.load(path), CONFIG)
            self.assertEqual(path.stem, selected["productVersion"])
            previous = None
            if selected["previousVersion"] is not None:
                previous = release.selection(release.load(ROOT / "releases" / (selected["previousVersion"] + ".json")), CONFIG)
            release.validate_previous(selected, previous)

    def test_runtime_only_plan_reuses_unchanged_blocks(self):
        release.selection(self.selected, CONFIG)
        release.validate_previous(self.selected, self.previous)
        plan = release.plan(self.selected, CONFIG)
        self.assertEqual(["runtime"], plan["publishComponents"])
        self.assertIn("blocks", plan["reuseComponents"])
        self.assertEqual(2, plan["centralPublishOperations"])
        self.assertEqual("26.10.1", self.selected["components"]["blocks"]["mavenVersion"])

    def test_dependency_order_only_includes_new_components(self):
        for key, version in [("contracts", "26.11.2"), ("compiler", "26.12.1")]:
            self.selected["components"][key].update(mavenVersion=version, sha="d" * 40, release="new")
        self.assertEqual(["contracts", "compiler", "runtime"], release.plan(self.selected, CONFIG)["publishComponents"])

    def test_already_published_components_are_not_published_again(self):
        artifacts = release.coordinates(self.selected, CONFIG, ROOT / "framework/bom/pom.xml")
        planned = release.plan(self.selected, CONFIG)
        pending = release.pending_publications(planned, artifacts, lambda url: True)
        self.assertEqual([], pending["publishComponents"])
        self.assertEqual(["runtime"], pending["alreadyPublishedComponents"])
        self.assertEqual(1, pending["centralPublishOperations"])
        fresh = release.pending_publications(planned, artifacts, lambda url: False)
        self.assertEqual(["runtime"], fresh["publishComponents"])
        count = 0
        def partial(url):
            nonlocal count
            count += 1
            return count == 1
        with self.assertRaisesRegex(ValueError, "partial Central publication"):
            release.pending_publications(planned, artifacts, partial)

    def test_reuse_identity_cannot_change(self):
        self.selected["components"]["blocks"]["sha"] = "e" * 40
        with self.assertRaisesRegex(ValueError, "reused blocks"):
            release.validate_previous(self.selected, self.previous)

    def test_new_component_cannot_reuse_version(self):
        self.selected["components"]["runtime"]["mavenVersion"] = "26.10.1"
        with self.assertRaisesRegex(ValueError, "overwrite"):
            release.validate_previous(self.selected, self.previous)

    def test_no_snapshots_ranges_candidate_versions_or_floating_sources(self):
        for version in ["26.11.1-SNAPSHOT", "LATEST", "[26,27)", "26.11.1-pr.1.abcdef123456"]:
            bad = copy.deepcopy(self.selected)
            bad["components"]["runtime"]["mavenVersion"] = version
            with self.assertRaisesRegex(ValueError, "plain stable"):
                release.selection(bad, CONFIG)
        self.selected["testHarnesses"]["references"]["sha"] = "main"
        with self.assertRaisesRegex(ValueError, "exact source SHA"):
            release.selection(self.selected, CONFIG)

    def test_missing_and_unknown_components_rejected(self):
        del self.selected["components"]["compiler"]
        with self.assertRaisesRegex(ValueError, "every maven"):
            release.selection(self.selected, CONFIG)

    def test_coordinate_versions_come_from_each_owner(self):
        artifacts = release.coordinates(self.selected, CONFIG, ROOT / "framework/bom/pom.xml")
        runtime = next(item for item in artifacts if item["artifactId"] == "pipelineframework")
        blocks = next(item for item in artifacts if item["artifactId"] == "graphql-agent")
        plugin = next(item for item in artifacts if item["artifactId"] == "pipelineframework-release-maven-plugin")
        self.assertEqual("26.12.3", runtime["version"])
        self.assertEqual("26.10.1", blocks["version"])
        self.assertEqual("maven-plugin", plugin["packaging"])
        self.assertTrue(any(name.endswith(".jar") for name in release.artifact_files(plugin)))

    def test_availability_is_mandatory_and_hashes_pin_the_downloaded_bytes(self):
        artifact = release.coordinates(self.selected, CONFIG, ROOT / "framework/bom/pom.xml")[0]
        with tempfile.TemporaryDirectory() as directory:
            observed = release.attest_artifacts([artifact], lambda url: url.encode(), Path(directory))
            for file in observed["artifacts"][0]["files"]:
                cached = Path(directory).joinpath(*artifact["groupId"].split("."), artifact["artifactId"], artifact["version"], file["name"])
                self.assertEqual(release.artifact_url(artifact, file["name"]).encode(), cached.read_bytes())
        for file in observed["artifacts"][0]["files"]:
            expected = hashlib.sha256(release.artifact_url(artifact, file["name"]).encode()).hexdigest()
            self.assertEqual(expected, file["sha256"])
        def missing(url):
            raise urllib.error.HTTPError(url, 404, "missing", {}, None)
        with self.assertRaisesRegex(ValueError, "artifact is unavailable"):
            release.attest_artifacts([artifact], missing)

    def test_freeze_changes_only_active_dependency_pins(self):
        source = '<project xmlns="http://maven.apache.org/POM/4.0.0"><!-- <tpf.version>old</tpf.version> --><version>26.11.1-SNAPSHOT</version><properties><tpf.version>26.11.1-SNAPSHOT</tpf.version></properties></project>'
        frozen = release.freeze_pom(source, {"tpf.version": "26.10.1"})
        self.assertIn('<!-- <tpf.version>old</tpf.version> -->', frozen)
        self.assertIn('<version>26.11.1-SNAPSHOT</version>', frozen)
        self.assertIn('<properties><tpf.version>26.10.1</tpf.version>', frozen)
        with self.assertRaisesRegex(ValueError, "exactly once"):
            release.freeze_pom(source.replace('</properties>', '<tpf.version>x</tpf.version></properties>'), {"tpf.version": "26.10.1"})

    def test_duplicate_json_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text('{"productVersion":"1.0.0","productVersion":"2.0.0"}')
            with self.assertRaisesRegex(ValueError, "duplicate JSON"):
                release.load(path)

    def test_source_tag_must_match_selected_sha(self):
        with self.assertRaisesRegex(ValueError, "immutable release tag"):
            release.verify_source_tags(self.selected, lambda path: json.dumps({"sha": "f" * 40}).encode())

    def test_publication_requires_successful_trusted_exact_selection_evidence(self):
        expected = release.evidence_identity(self.selected, ROOT / "framework/bom/pom.xml", ROOT / "system-tests/policy.yml")
        manifest_with_run = {**self.selected, "validationRunId": 123}
        run = {"status": "completed", "conclusion": "success", "event": "workflow_dispatch", "head_branch": "main", "path": ".github/workflows/system-test-full-train.yml"}
        tested = copy.deepcopy(expected)
        evidence_name = "release-selection.json"
        def api(path):
            if path.endswith("/zip"):
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, "w") as archive:
                    archive.writestr(evidence_name, json.dumps(tested))
                return buffer.getvalue()
            if "/artifacts?" in path:
                return json.dumps({"artifacts": [{"name": "tpf-full-train-resolved-set", "expired": False, "id": 5}]}).encode()
            return json.dumps(run).encode()
        release.verify_validation_run(manifest_with_run, expected, api)
        evidence_name = "rehearsal-selection.json"
        with self.assertRaisesRegex(ValueError, "rehearsal cannot approve publication"):
            release.verify_validation_run(manifest_with_run, expected, api)
        evidence_name = "release-selection.json"
        run["conclusion"] = "failure"
        with self.assertRaisesRegex(ValueError, "did not pass"):
            release.verify_validation_run(manifest_with_run, expected, api)
        run["conclusion"] = "success"
        run["head_branch"] = "feature"
        with self.assertRaisesRegex(ValueError, "trusted main"):
            release.verify_validation_run(manifest_with_run, expected, api)
        run["head_branch"] = "main"
        tested["selection"]["components"]["runtime"]["mavenVersion"] = "26.12.4"
        with self.assertRaisesRegex(ValueError, "different release selection"):
            release.verify_validation_run(manifest_with_run, expected, api)

    def test_evidence_changes_with_bom_or_policy_but_not_validation_run_id(self):
        original = release.evidence_identity(self.selected, ROOT / "framework/bom/pom.xml", ROOT / "system-tests/policy.yml")
        with tempfile.TemporaryDirectory() as directory:
            bom, policy = Path(directory) / "pom.xml", Path(directory) / "policy.yml"
            bom.write_bytes((ROOT / "framework/bom/pom.xml").read_bytes())
            policy.write_bytes((ROOT / "system-tests/policy.yml").read_bytes())
            selection_with_run = release.selection({**self.selected, "validationRunId": 123}, CONFIG)
            self.assertEqual(original, release.evidence_identity(selection_with_run, bom, policy))
            bom.write_text(bom.read_text().replace('<version>26.11.1-SNAPSHOT</version>', '<version>26.12.3</version>', 1))
            self.assertEqual(original, release.evidence_identity(self.selected, bom, policy))
            frozen_source = bom.read_text()
            bom.write_text(frozen_source.replace('<dependencyManagement>', '<properties><pipelineframework.blocks.version>unexpected</pipelineframework.blocks.version></properties><dependencyManagement>', 1))
            self.assertNotEqual(original, release.evidence_identity(self.selected, bom, policy))
            bom.write_text(frozen_source)
            policy.write_text(policy.read_text() + "\n")
            self.assertNotEqual(original, release.evidence_identity(self.selected, bom, policy))
            policy.write_bytes((ROOT / "system-tests/policy.yml").read_bytes())
            bom.write_text(bom.read_text().replace('<artifactId>pipelineframework-api</artifactId>', '<artifactId>changed-api</artifactId>'))
            self.assertNotEqual(original, release.evidence_identity(self.selected, bom, policy))

    def test_bom_must_match_each_independent_selected_version(self):
        source = (ROOT / "pom.xml").read_text()
        pins = {prop: self.selected["components"][name]["mavenVersion"] for name, prop in CONFIG["coordinationConsumerVersionProperties"].items()}
        frozen = release.freeze_pom(source, pins)
        frozen = frozen.replace('<version>26.11.1-SNAPSHOT</version>', '<version>26.12.3</version>', 1)
        with tempfile.TemporaryDirectory() as directory:
            pom = Path(directory) / "pom.xml"
            pom.write_text(frozen)
            release.verify_bom_pins(self.selected, CONFIG, pom)
            pom.write_text(frozen.replace('<pipelineframework.blocks.version>26.10.1</pipelineframework.blocks.version>', '<pipelineframework.blocks.version>26.12.3</pipelineframework.blocks.version>'))
            with self.assertRaisesRegex(ValueError, "blocks.version"):
                release.verify_bom_pins(self.selected, CONFIG, pom)


if __name__ == "__main__":
    unittest.main()
