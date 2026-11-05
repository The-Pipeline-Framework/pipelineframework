#!/usr/bin/env python3
"""Reuse attested Full Train inputs for a mixed candidate/stable rehearsal, never release approval."""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("product_release", Path(__file__).with_name("product-release.py"))
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
ROOT = release.ROOT
REPOSITORY = "The-Pipeline-Framework/pipelineframework"
API = f"repos/{REPOSITORY}"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
ARTIFACT_NAMES = {"resolution": "tpf-full-train-resolved-set", "inputs": "tpf-full-train-inputs"}
require = release.require


def validate_manifest(manifest):
    require(set(manifest) == {"schemaVersion", "sourceRunId", "sourceArtifacts", "stableOverrides"}, "invalid rehearsal manifest fields")
    require(type(manifest["schemaVersion"]) is int and manifest["schemaVersion"] == 1, "unsupported rehearsal schema")
    require(type(manifest["sourceRunId"]) is int and manifest["sourceRunId"] > 0, "sourceRunId must be positive")
    require(set(manifest["sourceArtifacts"]) == set(ARTIFACT_NAMES), "both frozen source artifacts are required")
    for artifact in manifest["sourceArtifacts"].values():
        require(set(artifact) == {"id", "digest"}, "invalid source artifact fields")
        require(type(artifact["id"]) is int and artifact["id"] > 0, "artifact id must be positive")
        require(isinstance(artifact["digest"], str) and DIGEST.fullmatch(artifact["digest"]), "artifact digest must be SHA-256")
    require(isinstance(manifest["stableOverrides"], dict) and manifest["stableOverrides"], "at least one stable override is required")


def validate_source(manifest, config, api=release.github):
    validate_manifest(manifest)
    base = f"{API}/actions/runs/{manifest['sourceRunId']}"
    run = json.loads(api(base))
    require(run.get("path") == ".github/workflows/system-test-full-train.yml" and run.get("head_branch") == "main" and run.get("event") in ("schedule", "workflow_dispatch"), "source must be the trusted main Full Train")
    require(isinstance(run.get("head_sha"), str) and release.SHA.fullmatch(run["head_sha"]), "source run SHA missing")
    jobs = json.loads(api(base + "/jobs?per_page=100"))["jobs"]
    require(any(job["name"] == "Materialize immutable Maven inputs" and job["conclusion"] == "success" for job in jobs), "source materialization did not succeed")
    artifacts = json.loads(api(base + "/artifacts?per_page=100"))["artifacts"]
    for key, expected in manifest["sourceArtifacts"].items():
        matches = [item for item in artifacts if item["id"] == expected["id"] and item["name"] == ARTIFACT_NAMES[key]]
        require(len(matches) == 1 and not matches[0]["expired"] and matches[0].get("digest") == expected["digest"], f"source {key} artifact is missing, expired or has a different digest")
    source_config = json.loads(api(f"{API}/contents/system-tests/components.yml?ref={run['head_sha']}", "-H", "Accept: application/vnd.github.raw+json"))
    require(source_config == config, "source component ownership changed; materialize fresh inputs")
    return run


def download_artifact(artifact, destination):
    with destination.open("wb") as output:
        subprocess.run(["gh", "api", f"{API}/actions/artifacts/{artifact['id']}/zip"], stdout=output, check=True)
    with destination.open("rb") as content:
        actual = "sha256:" + hashlib.file_digest(content, "sha256").hexdigest()
    require(actual == artifact["digest"], "downloaded source artifact checksum mismatch")


def overlay(source, manifest, config):
    result = copy.deepcopy(source)
    expected = config["components"]
    for field, kind in (("components", "maven"), ("testHarnesses", "source")):
        require(set(result[field]) == {name for name, value in expected.items() if value["kind"] == kind}, f"source {field} ownership is incomplete")
        for name, pin in result[field].items():
            require(pin["repository"] == expected[name]["repository"] and release.SHA.fullmatch(pin["sha"]), f"invalid frozen source pin: {name}")
            if kind == "maven":
                version = pin["mavenVersion"]
                require(release.VERSION.fullmatch(version) or re.fullmatch(r"\d+\.\d+\.\d+-main\." + pin["sha"][:12], version), f"non-immutable Maven version: {name}")
                require(DIGEST.fullmatch(pin.get("manifestDigest", "")), f"candidate manifest digest missing: {name}")
    for name, pin in manifest["stableOverrides"].items():
        require(name in result["components"], f"stable override is not a Maven component: {name}")
        require(set(pin) == {"repository", "sha", "mavenVersion"}, f"invalid stable override fields: {name}")
        require(pin["repository"] == expected[name]["repository"] and release.SHA.fullmatch(pin["sha"]), f"invalid stable ownership/source: {name}")
        require(release.VERSION.fullmatch(pin["mavenVersion"]), f"override must select a stable version: {name}")
        result["components"][name] = copy.deepcopy(pin)
    result["candidates"] = [pin for pin in result.get("candidates", []) if pin["component"] not in manifest["stableOverrides"]]
    result.pop("coordinationBom", None)
    require(any("manifestDigest" in pin for pin in result["components"].values()), "rehearsal must retain an immutable candidate")
    return result


def merge_plans(candidate, stable):
    entries, seen = [], set()
    for item in candidate["artifacts"] + stable["artifacts"]:
        key = (item["groupId"], item["artifactId"], "jar" if item["packaging"] == "maven-plugin" else item["packaging"])
        require(key not in seen, f"duplicate rehearsal coordinate: {key}")
        seen.add(key)
        entries.append(item)
    return {"schemaVersion": 1, "artifacts": sorted(entries, key=lambda item: item["coordinate"])}


def write(directory, name, value):
    (directory / name).write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "restore", "merge"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--candidate-plan", type=Path)
    args = parser.parse_args()
    config = release.load(ROOT / "system-tests/components.yml")
    if args.command == "merge":
        plan = merge_plans(release.load(args.candidate_plan), release.load(args.manifest))
        args.output.write_text(json.dumps(plan, indent=2) + "\n")
        return
    manifest = release.load(args.manifest)
    run = validate_source(manifest, config)
    args.output.mkdir(parents=True, exist_ok=True)
    key = "resolution" if args.command == "prepare" else "inputs"
    with tempfile.TemporaryDirectory() as temporary:
        archive = Path(temporary) / "source.zip"
        download_artifact(manifest["sourceArtifacts"][key], archive)
        with zipfile.ZipFile(archive) as bundle:
            require(bundle.namelist().count("resolved-set.json") == 1, "source resolution is missing or duplicated")
            source = json.loads(bundle.read("resolved-set.json"))
            resolved = overlay(source, manifest, config)
            if args.command == "restore":
                original = release.load(args.manifest.with_name("source-resolved-set.json"))
                require(all(source.get(field) == original.get(field) for field in ("components", "testHarnesses", "images")), "hydrated inputs do not match frozen source resolution")
                require(bundle.namelist().count("tpf-system-test-m2.tar.gz") == 1, "hydrated source archive missing or duplicated")
                with bundle.open("tpf-system-test-m2.tar.gz") as payload, (args.output / "tpf-system-test-m2.tar.gz").open("wb") as target:
                    shutil.copyfileobj(payload, target)
                return
    # Only source tag reads use the app token; coordinator artifacts use GITHUB_TOKEN.
    def source_api(*arguments):
        env = {**os.environ, "GH_TOKEN": os.environ.get("SOURCE_TOKEN", os.environ["GH_TOKEN"])}
        return subprocess.check_output(["gh", "api", *arguments], env=env)
    release.verify_source_tags({"components": manifest["stableOverrides"]}, source_api)
    stable = [item for item in release.coordinates(resolved, config, ROOT / "framework/bom/pom.xml") if item["component"] in manifest["stableOverrides"]]
    plan = release.attest_artifacts(stable, repository=args.repository)
    identity = release.evidence_identity(resolved, ROOT / "framework/bom/pom.xml", ROOT / "system-tests/policy.yml")
    for name, value in (("source-resolved-set.json", source), ("resolved-set.json", resolved), ("rehearsal-input.json", manifest), ("stable-materialization-plan.json", plan), ("rehearsal-selection.json", {"purpose": "rehearsal", "sourceRunSha": run["head_sha"], "input": manifest, **identity})):
        write(args.output, name, value)


if __name__ == "__main__":
    main()
