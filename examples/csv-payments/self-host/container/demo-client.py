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
CSV_INPUT_FILE_TYPE = "org.pipelineframework.csv.grpc.PipelineTypes$CsvPaymentsInputFile"


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


def default_idempotency_key(args, input_file):
    """Build a stable key for safe command retries; callers may override it explicitly."""
    path = Path(input_file).resolve()
    content = path.read_bytes()
    parts = [
        args.pipeline_id,
        str(path),
        f"{path.name}:{len(content)}:{hashlib.sha256(content).hexdigest()}",
    ]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"csv-{digest[:24]}"


def submit_csv_input_file(args, input_file):
    path = Path(input_file).resolve()
    folder = path.parent
    body = {
        "pipelineId": args.pipeline_id,
        "inputShape": "UNI",
        "inputPayload": encoded_payload({
            "filepath": str(path),
            "csvFolderPath": str(folder),
        }, CSV_INPUT_FILE_TYPE),
        "idempotencyKey": args.idempotency_key or default_idempotency_key(args, path),
        "outputStreaming": False,
    }
    accepted = request(
        "POST",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions",
        token=args.control_plane_token,
        body=body,
        timeout=30,
    )
    print(f"Submitted CSV execution {accepted['executionId']} for file {path}")
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
    if args.record_count > 0:
        target = input_dir / f"payments_{args.record_count}.csv"
        with target.open("w", encoding="utf-8", newline="") as generated:
            generated.write("ID,Recipient,Amount,Currency\n")
            for record_id in range(1, args.record_count + 1):
                generated.write(f"{record_id},Admission Profile {record_id},12.34,EUR\n")
    else:
        if not args.source_csv:
            raise RuntimeError("--source-csv is required when --record-count is not positive")
        source = Path(args.source_csv).resolve()
        if not source.is_file():
            raise RuntimeError(f"Source CSV not found: {source}")
        target = input_dir / source.name
        shutil.copyfile(source, target)
    target.chmod(0o666)
    print(f"Prepared CSV input {target}")
    return target


def wait_output(output_dir, output_name, timeout_seconds):
    deadline = time.time() + timeout_seconds
    output = Path(output_dir).resolve() / output_name
    while time.time() < deadline:
        if output.is_file() and output.stat().st_size > 0:
            print(f"Observed CSV output {output} ({output.stat().st_size} bytes)")
            return output
        time.sleep(1)
    raise RuntimeError(f"No non-empty CSV output appeared at {output}")


def assert_output_record_count(output, expected_record_count):
    if expected_record_count <= 0:
        return
    with output.open("r", encoding="utf-8") as generated:
        actual_record_count = max(0, sum(1 for _ in generated) - 1)
    if actual_record_count != expected_record_count:
        raise RuntimeError(
            f"CSV output {output} has {actual_record_count} records; expected {expected_record_count}")
    print(f"CSV output record count matches expected {expected_record_count}")


def validate_output_path(output_dir, output_file_name, record_count, timeout_seconds):
    output = wait_output(output_dir, output_file_name, timeout_seconds)
    assert_output_record_count(output, record_count)


def validate_output(args):
    validate_output_path(args.output_dir, args.output_file_name, args.record_count, args.timeout_seconds)


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
    input_file = prepare_input(args)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file_name = f"{input_file.name}.out"
    output_file = output_dir / output_file_name
    if output_file.exists():
        output_file.unlink()
    execution_id = submit_csv_input_file(args, input_file)
    wait_status(args, execution_id, args.timeout_seconds)
    inspect_result(args, execution_id)
    if not args.defer_output_validation:
        validate_output_path(output_dir, output_file_name, args.record_count, args.timeout_seconds)


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
    run.add_argument("--output-dir", required=True)
    run.add_argument("--source-csv")
    run.add_argument("--record-count", type=int, default=0)
    run.add_argument("--idempotency-key")
    run.add_argument("--timeout-seconds", type=int, default=240)
    run.add_argument("--defer-output-validation", action="store_true")
    run.set_defaults(func=run_flow)

    validate = sub.add_parser("validate-output")
    validate.add_argument("--output-dir", required=True)
    validate.add_argument("--output-file-name", required=True)
    validate.add_argument("--record-count", type=int, default=0)
    validate.add_argument("--timeout-seconds", type=int, default=240)
    validate.set_defaults(func=validate_output)

    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
