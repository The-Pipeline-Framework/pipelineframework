import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("coverage", ROOT / "system-tests/scripts/verify-bom-coverage.py")
coverage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(coverage)


class BomCoverageTest(unittest.TestCase):
    """Exercise coverage failures against the actual BOM and ownership registry."""
    def setUp(self):
        """Load fresh configuration so each mutation remains local to its test."""
        self.config = coverage.release.load(ROOT / "system-tests/components.yml")
        self.exclusions = coverage.release.load(ROOT / "system-tests/bom-exclusions.json")
        self.bom = ROOT / "framework/bom/pom.xml"

    def test_current_registered_artifacts_are_covered(self):
        """Require the committed BOM and exclusions to cover the registry."""
        coverage.verify(self.config, self.bom, self.exclusions)

    def test_new_consumer_cannot_silently_escape_bom(self):
        """Reject a newly registered connector lacking a BOM entry or exclusion."""
        self.config["components"]["connectors"]["allowedCoordinates"].append("org.pipelineframework:new-connector:jar")
        with self.assertRaisesRegex(ValueError, "BOM coverage missing.*new-connector"):
            coverage.verify(self.config, self.bom, self.exclusions)

    def test_new_component_family_also_requires_coverage(self):
        """Apply coverage to new component families without a hard-coded owner list."""
        self.config["components"]["another"] = {"kind": "maven", "repository": "org/another", "allowedCoordinates": ["org.example:another:jar"]}
        with self.assertRaisesRegex(ValueError, "BOM coverage missing.*another"):
            coverage.verify(self.config, self.bom, self.exclusions)

    def test_exclusions_must_be_reasoned_and_current(self):
        """Reject blank exclusion reasons and coordinates absent from the registry."""
        coordinate = next(iter(self.exclusions))
        self.exclusions[coordinate] = " "
        with self.assertRaisesRegex(ValueError, "needs a reason"):
            coverage.verify(self.config, self.bom, self.exclusions)
        del self.exclusions[coordinate]
        self.exclusions["org.example:unknown:jar"] = "Unknown"
        with self.assertRaisesRegex(ValueError, "unknown/stale"):
            coverage.verify(self.config, self.bom, self.exclusions)

    def test_managed_coordinate_cannot_be_exempted(self):
        """Reject exclusions that contradict the BOM's managed coordinate set."""
        self.exclusions["org.pipelineframework:decision-query-connector:jar"] = "Do not mask managed coordinates"
        with self.assertRaisesRegex(ValueError, "must not be excluded"):
            coverage.verify(self.config, self.bom, self.exclusions)

    def test_removed_entry_or_wrong_family_pin_fails(self):
        """Reject both a wrong owner-family pin and removal of a required entry."""
        import xml.etree.ElementTree as ET
        model = ET.parse(self.bom)
        deps = model.find("m:dependencyManagement/m:dependencies", coverage.release.NS)
        target = next(d for d in deps if d.findtext("m:artifactId", namespaces=coverage.release.NS) == "decision-query-connector")
        with tempfile.TemporaryDirectory() as directory:
            pom = Path(directory) / "pom.xml"
            target.find("m:version", coverage.release.NS).text = "${pipelineframework.runtime.version}"
            model.write(pom)
            with self.assertRaisesRegex(ValueError, "bypasses component selection"):
                coverage.verify(self.config, pom, self.exclusions)
            deps.remove(target)
            model.write(pom)
            with self.assertRaisesRegex(ValueError, "BOM coverage missing.*decision-query-connector"):
                coverage.verify(self.config, pom, self.exclusions)
