#!/usr/bin/env python3
"""Plan independent component releases and attest the selected stable product set."""
import argparse
import hashlib
import io
import json
import re
import subprocess
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
VERSION = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)")
SHA = re.compile(r"[0-9a-f]{40}")
CENTRAL = "https://repo.maven.apache.org/maven2/"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def evidence_identity(selected, bom, policy):
    model = ET.parse(bom).getroot()
    require(model.find("m:dependencyManagement", NS) is not None, "BOM dependency management missing")
    # Product version preparation may change these declarations after the train.
    # Properties, dependency management and publication behaviour must stay frozen.
    for path in ("m:parent/m:version", "m:version"):
        element = model.find(path, NS)
        if element is not None:
            element.text = "PRODUCT_VERSION"
    xml = ET.canonicalize(ET.tostring(model, encoding="unicode"), strip_text=True)
    identity = {"selection": selected, "bomSha256": hashlib.sha256(xml.encode()).hexdigest(), "policySha256": hashlib.sha256(Path(policy).read_bytes()).hexdigest(), "componentsSha256": hashlib.sha256(canonical(load(ROOT / "system-tests/components.yml")).encode()).hexdigest()}
    return {**identity, "selectionSha256": hashlib.sha256(canonical(identity).encode()).hexdigest()}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique)


def selection(manifest, config):
    require(isinstance(manifest, dict), "release manifest must be an object")
    require(set(manifest) <= {"schemaVersion", "productVersion", "previousVersion", "components", "testHarnesses", "validationRunId"}, "unknown release manifest fields")
    require({"schemaVersion", "productVersion", "previousVersion", "components", "testHarnesses"} <= set(manifest), "release manifest fields missing")
    require(type(manifest.get("schemaVersion")) is int and manifest["schemaVersion"] == 1, "unsupported release schema")
    require(isinstance(manifest.get("productVersion"), str) and VERSION.fullmatch(manifest["productVersion"]), "productVersion must be a plain stable version")
    previous = manifest.get("previousVersion")
    require(previous is None or isinstance(previous, str) and VERSION.fullmatch(previous), "previousVersion must be stable")
    expected = config["components"]
    for kind, field in [("maven", "components"), ("source", "testHarnesses")]:
        names = {key for key, value in expected.items() if value["kind"] == kind}
        require(isinstance(manifest.get(field), dict) and set(manifest[field]) == names, f"{field} must select every {kind} component exactly once")
        for name, pin in manifest[field].items():
            keys = {"repository", "sha", "mavenVersion", "release"} if kind == "maven" else {"repository", "sha"}
            require(isinstance(pin, dict) and set(pin) == keys, f"invalid fields for {name}")
            require(pin["repository"] == expected[name]["repository"], f"repository ownership mismatch for {name}")
            require(isinstance(pin["sha"], str) and SHA.fullmatch(pin["sha"]), f"{name} requires an exact source SHA")
            if kind == "maven":
                require(isinstance(pin["mavenVersion"], str) and VERSION.fullmatch(pin["mavenVersion"]), f"{name} must select a plain stable Maven version")
                require(pin["release"] in ("new", "reuse"), f"invalid release action for {name}")
    return {key: value for key, value in manifest.items() if key != "validationRunId"}


def validate_previous(manifest, previous):
    if manifest.get("previousVersion") is None:
        require(previous is None, "bootstrap selection cannot have a previous manifest")
        return
    require(previous is not None and previous["productVersion"] == manifest["previousVersion"], "previous release manifest does not match previousVersion")
    require(manifest["productVersion"] != previous["productVersion"], "product release version is already selected")
    for name, pin in manifest["components"].items():
        old = previous["components"][name]
        if pin["release"] == "reuse":
            require(all(pin[key] == old[key] for key in ("repository", "sha", "mavenVersion")), f"reused {name} must retain the previous published identity")
        else:
            require(pin["mavenVersion"] != old["mavenVersion"], f"new {name} cannot overwrite an existing version")
            require(pin["sha"] != old["sha"], f"new {name} requires a new release source commit")


def coordinates(manifest, config, bom):
    def ownership_key(coordinate):
        group, artifact, packaging = coordinate.split(":")
        return f"{group}:{artifact}:{'jar' if packaging == 'maven-plugin' else packaging}"
    owners = {}
    for name, component in config["components"].items():
        for coordinate in component.get("allowedCoordinates", []):
            key = ownership_key(coordinate)
            require(key not in owners, f"duplicate coordinate ownership: {coordinate}")
            owners[key] = name
    result = []
    seen = set()
    for dependency in ET.parse(bom).findall("m:dependencyManagement/m:dependencies/m:dependency", NS):
        def value(name, default=""):
            return (dependency.findtext(f"m:{name}", default, NS) or default).strip()
        group, artifact, packaging = value("groupId"), value("artifactId"), value("type", "jar")
        coordinate = f"{group}:{artifact}:{packaging}"
        owner = owners.get(ownership_key(coordinate))
        require(owner is not None, f"BOM coordinate has no component owner: {coordinate}")
        require(value("version") == "${pipelineframework." + owner + ".version}", f"BOM coordinate bypasses component selection: {coordinate}")
        require(not value("classifier"), f"release selector does not support a classified BOM coordinate: {coordinate}")
        require(coordinate not in seen, f"duplicate BOM coordinate: {coordinate}")
        seen.add(coordinate)
        result.append({"component": owner, "repository": manifest["components"][owner]["repository"], "groupId": group, "artifactId": artifact, "packaging": packaging, "version": manifest["components"][owner]["mavenVersion"]})
    require(result, "BOM declares no managed coordinates")
    return sorted(result, key=lambda item: (item["groupId"], item["artifactId"], item["packaging"]))


def fetch(url):
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def artifact_files(artifact):
    extension = "jar" if artifact["packaging"] == "maven-plugin" else artifact["packaging"]
    stem = f"{artifact['artifactId']}-{artifact['version']}"
    return sorted({f"{stem}.pom", f"{stem}.{extension}"})


def artifact_url(artifact, name):
    return CENTRAL + "/".join([*artifact["groupId"].split("."), artifact["artifactId"], artifact["version"], name])


def available(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60):
            return True
    except urllib.error.HTTPError as error:
        error.close()
        if error.code == 404:
            return False
        raise


def pending_publications(publication_plan, artifacts, exists=available):
    result = {**publication_plan, "publishComponents": [], "alreadyPublishedComponents": []}
    for component in publication_plan["publishComponents"]:
        owned = [item for item in artifacts if item["component"] == component]
        require(owned, f"new component has no BOM coordinates: {component}")
        observed = [exists(artifact_url(item, file)) for item in owned for file in artifact_files(item)]
        require(all(observed) or not any(observed), f"partial Central publication for {component}; recover the existing deployment, do not republish")
        result["alreadyPublishedComponents" if all(observed) else "publishComponents"].append(component)
    result["centralPublishOperations"] = len(result["publishComponents"]) + 1
    return result


def attest_artifacts(artifacts, download=fetch, repository=None):
    result = []
    for artifact in artifacts:
        files = []
        for name in artifact_files(artifact):
            url = artifact_url(artifact, name)
            try:
                content = download(url)
            except urllib.error.HTTPError as error:
                error.close()
                raise ValueError(f"selected release artifact is unavailable: {url} (HTTP {error.code})") from error
            if repository is not None:
                destination = repository.joinpath(*artifact["groupId"].split("."), artifact["artifactId"], artifact["version"], name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content)
            files.append({"name": name, "sha256": hashlib.sha256(content).hexdigest()})
        result.append({**artifact, "coordinate": ":".join(artifact[key] for key in ("groupId", "artifactId", "version", "packaging")), "files": files})
    return {"schemaVersion": 1, "artifacts": result}


def plan(manifest, config):
    pending = {key for key, pin in manifest["components"].items() if pin["release"] == "new"}
    order = []
    while pending:
        ready = sorted(key for key in pending if not pending.intersection(config["components"][key]["consumerVersionProperties"]))
        require(ready, "component publication dependency cycle")
        order.extend(ready)
        pending.difference_update(ready)
    return {"productVersion": manifest["productVersion"], "publishComponents": order, "reuseComponents": sorted(set(manifest["components"]) - set(order)), "centralPublishOperations": len(order) + 1}


def freeze_pom(source, properties):
    root = ET.fromstring(source)
    declared = root.find("m:properties", NS)
    require(declared is not None, "POM root properties missing")
    for name in properties:
        require(len(declared.findall(f"m:{name}", NS)) == 1, f"POM root must declare {name} exactly once")
    # Match active XML only while preserving all existing formatting and comments.
    pieces = re.split(r"(<!--[\s\S]*?-->)", source)
    for name, version in properties.items():
        pattern = re.compile(r"(<" + re.escape(name) + r">)\s*[^<]*\s*(</" + re.escape(name) + r">)")
        count = sum(len(pattern.findall(piece)) for piece in pieces[::2])
        require(count == 1, f"POM must declare {name} exactly once; found {count}")
        pieces[::2] = [pattern.sub(lambda match: match[1] + version + match[2], piece) for piece in pieces[::2]]
    return "".join(pieces)


def verify_bom_pins(manifest, config, root_pom):
    root = ET.parse(root_pom).getroot()
    require(root.findtext("m:version", namespaces=NS) == manifest["productVersion"], "coordinator version does not match productVersion")
    properties = root.find("m:properties", NS)
    require(properties is not None, "coordinator properties missing")
    for component, prop in config["coordinationConsumerVersionProperties"].items():
        found = properties.findall(f"m:{prop}", NS)
        require(len(found) == 1 and found[0].text == manifest["components"][component]["mavenVersion"], f"BOM property {prop} does not match release selection")


def github(*args):
    return subprocess.check_output(["gh", "api", *args])


def verify_validation_run(manifest, expected, api=github):
    run_id = manifest.get("validationRunId")
    require(type(run_id) is int and run_id > 0, "validationRunId must identify a successful selected-release full train")
    base = "repos/The-Pipeline-Framework/pipelineframework/actions/runs/" + str(run_id)
    run = json.loads(api(base))
    require(run.get("conclusion") == "success" and run.get("status") == "completed", "release validation run did not pass")
    require(run.get("event") == "workflow_dispatch" and run.get("head_branch") == "main" and run.get("path") == ".github/workflows/system-test-full-train.yml", "release evidence must come from the trusted main full-train workflow")
    artifacts = json.loads(api(base + "/artifacts?per_page=100"))["artifacts"]
    matches = [item for item in artifacts if item["name"] == "tpf-full-train-resolved-set" and not item["expired"]]
    require(len(matches) == 1, "release validation evidence artifact missing or expired")
    archive = api("repos/The-Pipeline-Framework/pipelineframework/actions/artifacts/" + str(matches[0]["id"]) + "/zip")
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        tested = json.loads(bundle.read("release-selection.json"))
    require(tested == expected, "successful train tested a different release selection, BOM or coverage policy")


def verify_source_tags(selected, api=github):
    for component, pin in selected["components"].items():
        commit = json.loads(api(f"repos/{pin['repository']}/commits/v{pin['mavenVersion']}"))
        require(commit.get("sha") == pin["sha"], f"{component} source SHA does not match its immutable release tag")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["plan", "freeze", "resolve", "gate", "notes"])
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("resolved"))
    parser.add_argument("--component", default="coordination")
    parser.add_argument("--pom", type=Path)
    parser.add_argument("--repository", type=Path, help="Save resolved public files here for the single Maven hydration")
    args = parser.parse_args()
    config = load(ROOT / "system-tests/components.yml")
    manifest = load(args.manifest)
    selected = selection(manifest, config)
    previous = None
    if selected.get("previousVersion") is not None:
        previous = selection(load(ROOT / "releases" / (selected["previousVersion"] + ".json")), config)
    validate_previous(selected, previous)
    identity = evidence_identity(selected, ROOT / "framework/bom/pom.xml", ROOT / "system-tests/policy.yml")
    publication_plan = plan(selected, config)
    if args.command == "plan":
        print(json.dumps(pending_publications(publication_plan, coordinates(selected, config, ROOT / "framework/bom/pom.xml")), indent=2))
    elif args.command == "notes":
        print("## Tested component selection\n\n| Component | Version | Publication |\n| --- | --- | --- |")
        for name, pin in sorted(selected["components"].items()):
            print(f"| {name} | {pin['mavenVersion']} | {pin['release']} |")
        if manifest.get("validationRunId"):
            print(f"\n[Selected-release full train](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/{manifest['validationRunId']})")
    elif args.command == "freeze":
        require(args.pom is not None, "freeze requires --pom; only the named file is edited")
        if args.component == "coordination":
            mapping = config["coordinationConsumerVersionProperties"]
        else:
            require(args.component in publication_plan["publishComponents"], "do not edit or release a reused component")
            mapping = config["components"][args.component]["consumerVersionProperties"]
        pins = {prop: selected["components"][name]["mavenVersion"] for name, prop in mapping.items()}
        args.pom.write_text(freeze_pom(args.pom.read_text(), pins))
    elif args.command == "resolve":
        verify_source_tags(selected)
        args.output.mkdir(parents=True, exist_ok=True)
        materialized = attest_artifacts(coordinates(selected, config, ROOT / "framework/bom/pom.xml"), repository=args.repository)
        resolved = {"schemaVersion": 1, "components": {key: {k: v for k, v in pin.items() if k != "release"} for key, pin in selected["components"].items()}, "testHarnesses": selected["testHarnesses"], "images": {}, "candidates": [], "releaseSelectionSha256": identity["selectionSha256"]}
        for name, value in [("resolved-set.json", resolved), ("materialization-plan.json", materialized), ("release-selection.json", identity)]:
            (args.output / name).write_text(json.dumps(value, indent=2) + "\n")
    else:
        require(selected.get("previousVersion") is not None, "publication requires an explicit previous product release")
        verify_bom_pins(selected, config, ROOT / "pom.xml")
        verify_validation_run(manifest, identity)
        for artifact in load(ROOT / "framework/public-artifacts.json")["publicArtifacts"]:
            artifact_id = artifact["artifactId"]
            product_pom = f"org/pipelineframework/{artifact_id}/{selected['productVersion']}/{artifact_id}-{selected['productVersion']}.pom"
            require(not available(CENTRAL + product_pom), f"{artifact_id} is already published; recover only the failed downstream step")
        # Existing publication verification additionally resolves every managed coordinate.
        print(f"Verified tested independent release selection for {selected['productVersion']}: {identity['selectionSha256']}")


if __name__ == "__main__":
    main()
