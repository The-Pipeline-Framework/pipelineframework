#!/usr/bin/env python3
import argparse
import hashlib
import json
import random
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

PAYLOAD_ENCODING = "application/tpf-transition+json"
CSV_FOLDER_TYPE = "org.pipelineframework.csv.grpc.PipelineTypes$CsvFolder"


def request(method, url, token=None, body=None, timeout=10):
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported URL scheme for {method}: {parsed_url.scheme}")
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            if not raw:
                return None
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {raw}") from exc


def wait_health(args):
    deadline = time.time() + args.timeout_seconds
    url = f"{args.base_url}/q/health/live"
    last_error = None
    while time.time() < deadline:
        try:
            request("GET", url, timeout=2)
            print(f"{args.name} is healthy at {args.base_url}")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {args.name} at {url}: {last_error}")


def locate_artifact(args):
    target_dir = Path(args.target_dir)
    matches = []
    for candidate in target_dir.rglob("*.jar"):
        if "tpf-container-ha" in candidate.parts:
            continue
        try:
            with zipfile.ZipFile(candidate) as jar:
                with jar.open("META-INF/pipeline/pipeline-contract.json") as contract_file:
                    contract = json.load(contract_file)
                    if contract.get("pipelineId") == args.pipeline_id:
                        matches.append(candidate)
        except (zipfile.BadZipFile, KeyError, json.JSONDecodeError):
            continue
    if not matches:
        raise RuntimeError(f"No JAR with pipelineId={args.pipeline_id} contract found under {target_dir}")
    selected = max(matches, key=lambda path: (path.stat().st_mtime, path.stat().st_size, str(path)))
    print(selected.resolve())


def create_release(args):
    artifact_path = Path(args.artifact_path).resolve()
    descriptor_path = Path(args.output).resolve()
    if not artifact_path.is_file():
        raise RuntimeError(f"Release artifact not found: {artifact_path}")
    with zipfile.ZipFile(artifact_path) as jar:
        with jar.open("META-INF/pipeline/pipeline-contract.json") as contract_file:
            contract = json.load(contract_file)
    if contract.get("pipelineId") != args.pipeline_id:
        raise RuntimeError(
            f"Artifact pipelineId={contract.get('pipelineId')} does not match {args.pipeline_id}")
    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    release_version = args.release_version or contract["contractVersion"]
    descriptor = {
        "schemaVersion": 1,
        "pipelineId": args.pipeline_id,
        "contractVersion": contract["contractVersion"],
        "releaseVersion": release_version,
        "artifacts": [
            {
                "artifactId": "csv-payments-pipeline-runtime",
                "kind": "jar",
                "uri": str(artifact_path),
                "digest": f"sha256:{digest}",
                "stepIds": [step.get("authoredName") for step in contract.get("steps", [])],
                "capabilities": ["local-transition-execution", "rest-transition-worker"],
            }
        ],
    }
    descriptor_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor_path.write_text(json.dumps(descriptor, indent=2, sort_keys=True), encoding="utf-8")
    print(descriptor_path)


def register_activate(args):
    base = f"{args.base_url}/tpf/admin/tenants/{args.tenant_id}/pipelines/{args.pipeline_id}/releases"
    registered = request(
        "POST",
        f"{base}/register",
        token=args.admin_token,
        body={"releaseDescriptorPath": str(Path(args.release_descriptor_path).resolve())},
        timeout=30,
    )
    release_version = registered["releaseVersion"]
    request("POST", f"{base}/{release_version}/activate", token=args.admin_token, timeout=30)
    print(f"Registered and activated CSV release {release_version}")
    if args.worker_id:
        register_worker(args, registered)


def register_worker(args, release):
    base = f"{args.base_url}/tpf/admin/tenants/{args.tenant_id}/pipelines/{args.pipeline_id}/workers"
    body = {
        "workerId": args.worker_id,
        "contractVersion": release["contractVersion"],
        "releaseVersion": release["releaseVersion"],
        "protocol": args.worker_protocol,
        "endpoint": args.worker_endpoint,
        "artifactId": release.get("primaryArtifactId", ""),
        "artifactDigest": release.get("primaryArtifactDigest", ""),
    }
    registered = request("POST", f"{base}/register", token=args.admin_token, body=body, timeout=30)
    print(
        "Registered worker "
        f"{registered['workerId']} ({registered['protocol']}) for release {registered['releaseVersion']}"
    )


def encoded_payload(payload, payload_type):
    return {
        "payloadTypeId": payload_type,
        "payloadEncoding": PAYLOAD_ENCODING,
        "payload": json.dumps(payload, separators=(",", ":")),
    }


def default_idempotency_key(args, folder):
    """Build a stable key for safe command retries; callers may override it explicitly."""
    input_dir = Path(folder)
    parts = [args.pipeline_id, str(input_dir)]
    for path in sorted(input_dir.glob("*.csv")):
        content = path.read_bytes()
        parts.append(f"{path.name}:{len(content)}:{hashlib.sha256(content).hexdigest()}")
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"csv-{digest[:24]}"


def submit_csv_folder(args):
    folder = str(Path(args.input_dir).resolve())
    body = {
        "pipelineId": args.pipeline_id,
        "inputShape": "UNI",
        "inputPayload": encoded_payload({"path": folder}, CSV_FOLDER_TYPE),
        "idempotencyKey": args.idempotency_key or default_idempotency_key(args, folder),
        "outputStreaming": False,
    }
    accepted = request(
        "POST",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions",
        token=args.control_plane_token,
        body=body,
        timeout=30,
    )
    print(f"Submitted CSV execution {accepted['executionId']} for folder {folder}")
    return accepted["executionId"]


def wait_status(args, execution_id, timeout_seconds):
    deadline = time.time() + timeout_seconds
    last = None
    last_error = None
    interval = 1.0
    url = f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{execution_id}"
    while time.time() < deadline:
        try:
            last = request("GET", url, token=args.control_plane_token, timeout=10)
        except Exception as exc:
            last_error = exc
        else:
            last_error = None
            status = last["status"]
            if status == "SUCCEEDED":
                print(f"Execution {execution_id} succeeded")
                return last
            if status in {"FAILED", "DLQ"}:
                raise RuntimeError(f"Execution {execution_id} failed: {json.dumps(last, sort_keys=True)}")
        sleep_for = min(interval + random.uniform(0, 0.25), max(0.0, deadline - time.time()))
        if sleep_for > 0:
            time.sleep(sleep_for)
        interval = min(interval * 2, 30.0)
    raise RuntimeError(f"Execution {execution_id} did not complete; last={last}; last_error={last_error}")


def prepare_input(args):
    input_dir = Path(args.input_dir).resolve()
    input_dir.mkdir(parents=True, exist_ok=True)
    # The demo bind-mounts this host directory into non-root containers; the
    # output step writes sibling .out files next to the copied CSV input.
    input_dir.chmod(0o777)
    for candidate in input_dir.glob("*.csv"):
        candidate.unlink()
    for candidate in input_dir.glob("*.out"):
        candidate.unlink()
    source = Path(args.source_csv).resolve()
    if not source.is_file():
        raise RuntimeError(f"Source CSV not found: {source}")
    target = input_dir / source.name
    shutil.copyfile(source, target)
    target.chmod(0o666)
    print(f"Prepared CSV input {target}")
    return input_dir


def wait_output(input_dir, timeout_seconds):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        outputs = sorted(Path(input_dir).glob("*.out"))
        non_empty = [path for path in outputs if path.stat().st_size > 0]
        if non_empty:
            for output in non_empty:
                print(f"Observed CSV output {output} ({output.stat().st_size} bytes)")
            return non_empty
        time.sleep(1)
    raise RuntimeError(f"No non-empty .out files appeared under {input_dir}")


def inspect_result(args, execution_id):
    result = request(
        "GET",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{execution_id}/result",
        token=args.control_plane_token,
        timeout=30,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def run_flow(args):
    input_dir = prepare_input(args)
    execution_id = submit_csv_folder(args)
    wait_status(args, execution_id, args.timeout_seconds)
    inspect_result(args, execution_id)
    wait_output(input_dir, args.timeout_seconds)


def main():
    parser = argparse.ArgumentParser(description="CSV Payments self-hosted coordinator demo client")
    sub = parser.add_subparsers(dest="command", required=True)

    health = sub.add_parser("wait-health")
    health.add_argument("--base-url", required=True)
    health.add_argument("--name", required=True)
    health.add_argument("--timeout-seconds", type=int, default=60)
    health.set_defaults(func=wait_health)

    locate = sub.add_parser("locate-artifact")
    locate.add_argument("--target-dir", required=True)
    locate.add_argument("--pipeline-id", required=True)
    locate.set_defaults(func=locate_artifact)

    release = sub.add_parser("create-release")
    release.add_argument("--pipeline-id", required=True)
    release.add_argument("--artifact-path", required=True)
    release.add_argument("--output", required=True)
    release.add_argument("--release-version")
    release.set_defaults(func=create_release)

    reg = sub.add_parser("register-activate")
    reg.add_argument("--base-url", required=True)
    reg.add_argument("--tenant-id", required=True)
    reg.add_argument("--pipeline-id", required=True)
    reg.add_argument("--admin-token", required=True)
    reg.add_argument("--release-descriptor-path", required=True)
    reg.add_argument("--worker-id")
    reg.add_argument("--worker-protocol", default="rest")
    reg.add_argument("--worker-endpoint", default="http://worker:8182")
    reg.set_defaults(func=register_activate)

    run = sub.add_parser("run-flow")
    run.add_argument("--base-url", required=True)
    run.add_argument("--tenant-id", required=True)
    run.add_argument("--pipeline-id", required=True)
    run.add_argument("--control-plane-token", required=True)
    run.add_argument("--input-dir", required=True)
    run.add_argument("--source-csv", required=True)
    run.add_argument("--idempotency-key")
    run.add_argument("--timeout-seconds", type=int, default=240)
    run.set_defaults(func=run_flow)

    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
