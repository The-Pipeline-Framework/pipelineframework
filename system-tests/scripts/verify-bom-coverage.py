#!/usr/bin/env python3
"""Require every registered Maven artifact to be BOM-managed or explicitly excluded."""
import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("product_release", Path(__file__).with_name("product-release.py"))
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
ROOT = release.ROOT


def key(coordinate):
    group, artifact, packaging = coordinate.split(":")
    return group, artifact, "jar" if packaging == "maven-plugin" else packaging


def verify(config, bom, exclusions):
    # Also validates ownership, duplicates and each dependency's owner-family version property.
    pins = {"components": {name: {"repository": component["repository"], "mavenVersion": "test"}
                           for name, component in config["components"].items() if component["kind"] == "maven"}}
    managed = {key(":".join(item[field] for field in ("groupId", "artifactId", "packaging")))
               for item in release.coordinates(pins, config, bom)}
    owned = {key(coordinate): coordinate for component in config["components"].values()
             for coordinate in component.get("allowedCoordinates", [])}
    exempt = set()
    for coordinate, reason in exclusions.items():
        identity = key(coordinate)
        release.require(identity in owned and coordinate == owned[identity], f"unknown/stale BOM exclusion: {coordinate}")
        release.require(identity not in managed, f"BOM-managed coordinate must not be excluded: {coordinate}")
        release.require(isinstance(reason, str) and reason.strip(), f"BOM exclusion needs a reason: {coordinate}")
        release.require(identity not in exempt, f"duplicate BOM exclusion: {coordinate}")
        exempt.add(identity)
    missing = sorted(owned[identity] for identity in owned.keys() - managed - exempt)
    release.require(not missing, "BOM coverage missing; manage or explicitly classify: " + ", ".join(missing))
    return len(managed), len(exempt)


if __name__ == "__main__":
    managed, excluded = verify(release.load(ROOT / "system-tests/components.yml"),
                               ROOT / "framework/bom/pom.xml",
                               release.load(ROOT / "system-tests/bom-exclusions.json"))
    print(f"BOM coverage verified: {managed} managed coordinates, {excluded} reasoned exclusions.")
