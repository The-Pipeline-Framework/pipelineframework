#!/usr/bin/env python3
import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

PAYLOAD_ENCODING = "application/tpf-transition+json"
ORDER_TYPE = "org.pipelineframework.restaurantapproval.common.dto.PlaceRestaurantOrderRequestDto"


def request(method, url, token=None, body=None, timeout=10):
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
            time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {args.name} at {url}: {last_error}")


def locate_bundle(args):
    target_dir = Path(args.target_dir)
    matches = []
    for candidate in target_dir.rglob("*.jar"):
        if {"tpf-self-host", "transition-worker-split-it"} & set(candidate.parts):
            continue
        try:
            with zipfile.ZipFile(candidate) as jar:
                with jar.open("META-INF/pipeline/pipeline-contract.json") as contract_file:
                    contract = json.load(contract_file)
                    if contract.get("pipelineId") == args.pipeline_id:
                        matches.append(candidate)
        except (zipfile.BadZipFile, KeyError, json.JSONDecodeError):
            continue
    if matches:
        selected = max(matches, key=lambda path: path.stat().st_mtime)
        print(selected.resolve())
        return
    raise RuntimeError(f"No JAR with pipelineId={args.pipeline_id} contract found under {target_dir}")


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
    contract_version = contract["contractVersion"]
    release_version = args.release_version or contract_version
    descriptor = {
        "schemaVersion": 1,
        "pipelineId": args.pipeline_id,
        "contractVersion": contract_version,
        "releaseVersion": release_version,
        "artifacts": [
            {
                "artifactId": "restaurant-approval-monolith",
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
    )
    release_version = registered["releaseVersion"]
    request("POST", f"{base}/{release_version}/activate", token=args.admin_token)
    print(f"Registered and activated release {release_version}")
    if args.worker_id:
        register_worker_from_release(args, registered)


def register_worker_from_release(args, release):
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
    registered = request("POST", f"{base}/register", token=args.admin_token, body=body)
    print(
        "Registered worker "
        f"{registered['workerId']} ({registered['protocol']}) for release {registered['releaseVersion']}"
    )


def encoded_payload(payload, payload_type=None):
    if payload is None:
        return {
            "payloadTypeId": "null",
            "payloadEncoding": PAYLOAD_ENCODING,
            "payload": "null",
        }
    if isinstance(payload, dict) and (payload_type is None or payload_type == "java.util.Map"):
        items = {str(key): encoded_payload(value) for key, value in payload.items()}
        return {
            "payloadTypeId": "java.util.Map",
            "payloadEncoding": PAYLOAD_ENCODING,
            "payload": json.dumps({"items": items}, separators=(",", ":")),
        }
    if isinstance(payload, list) and (payload_type is None or payload_type == "java.util.List"):
        return {
            "payloadTypeId": "java.util.List",
            "payloadEncoding": PAYLOAD_ENCODING,
            "payload": json.dumps({"items": [encoded_payload(item) for item in payload]}, separators=(",", ":")),
        }
    if payload_type is None:
        payload_type = scalar_type_id(payload)
    return {
        "payloadTypeId": payload_type,
        "payloadEncoding": PAYLOAD_ENCODING,
        "payload": json.dumps(payload, separators=(",", ":")),
    }


def scalar_type_id(payload):
    if isinstance(payload, str):
        return "java.lang.String"
    if isinstance(payload, bool):
        return "java.lang.Boolean"
    if isinstance(payload, int):
        return "java.lang.Long"
    if isinstance(payload, float):
        return "java.lang.Double"
    return type(payload).__name__


def auth(args):
    return args.control_plane_token


def submit_order(args, customer_name, restaurant_name):
    request_id = str(uuid.uuid4())
    order = {
        "requestId": request_id,
        "customerName": customer_name,
        "restaurantName": restaurant_name,
        "items": "Margherita Pizza, Sparkling Water",
        "totalAmount": "27.50",
        "currency": "EUR",
    }
    body = {
        "pipelineId": args.pipeline_id,
        "inputShape": "RAW",
        "inputPayload": encoded_payload(order, ORDER_TYPE),
        "idempotencyKey": f"order-{request_id}",
        "outputStreaming": False,
    }
    result = request(
        "POST",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions",
        token=auth(args),
        body=body,
    )
    print(f"Submitted execution {result['executionId']} for {customer_name}")
    return result["executionId"]


def wait_status(args, execution_id, target_status, timeout_seconds=None):
    timeout_seconds = timeout_seconds or getattr(args, "timeout_seconds", 30)
    deadline = time.time() + timeout_seconds
    last = None
    url = f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{execution_id}"
    while time.time() < deadline:
        last = request("GET", url, token=auth(args))
        status = last["status"]
        if status == target_status:
            print(f"Execution {execution_id} reached {target_status}")
            return last
        if status in {"FAILED", "DLQ"}:
            raise RuntimeError(f"Execution {execution_id} failed: {last}")
        time.sleep(0.2)
    raise RuntimeError(f"Execution {execution_id} did not reach {target_status}; last={last}")


def wait_terminal_failure(args, execution_id, timeout_seconds=30):
    deadline = time.time() + timeout_seconds
    last = None
    url = f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{execution_id}"
    while time.time() < deadline:
        last = request("GET", url, token=auth(args))
        if last["status"] in {"FAILED", "DLQ"}:
            print(f"Execution {execution_id} reached terminal failure: {json.dumps(last, sort_keys=True)}")
            return last
        time.sleep(0.2)
    raise RuntimeError(f"Execution {execution_id} did not fail terminally; last={last}")


def pending_interaction(args, execution_id, timeout_seconds=None):
    timeout_seconds = timeout_seconds or getattr(args, "timeout_seconds", 30)
    deadline = time.time() + timeout_seconds
    url = (
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/interactions/pending"
        f"?stepId={args.await_step_id}"
    )
    while time.time() < deadline:
        interactions = request("GET", url, token=auth(args))
        for interaction in interactions:
            if interaction.get("executionId") == execution_id:
                payload = interaction.get("requestPayload") or {}
                print(f"Pending interaction {interaction['interactionId']} for order {payload.get('orderId')}")
                return interaction
        time.sleep(0.2)
    raise RuntimeError(f"No pending interaction found for execution {execution_id}")


def query_pending(args):
    url = (
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/interactions/pending"
        f"?stepId={args.await_step_id}"
    )
    interactions = request("GET", url, token=auth(args))
    if args.execution_id:
        interactions = [
            interaction for interaction in interactions
            if interaction.get("executionId") == args.execution_id
        ]
    print(json.dumps(interactions, indent=2, sort_keys=True))


def inspect_status(args):
    status = request(
        "GET",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{args.execution_id}",
        token=auth(args),
    )
    print(json.dumps(status, indent=2, sort_keys=True))


def inspect_result(args):
    result = request(
        "GET",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{args.execution_id}/result",
        token=auth(args),
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def complete(args, interaction, decision):
    order_id = (interaction.get("requestPayload") or {}).get("orderId")
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if decision == "accepted":
        decision_payload = {
            "accepted": {
                "orderId": order_id,
                "decidedAt": now,
                "note": "Approved by Cafe TPF",
            }
        }
    else:
        decision_payload = {
            "declined": {
                "orderId": order_id,
                "decidedAt": now,
                "note": "Need more prep time",
                "declineReason": "Kitchen is overloaded tonight",
            }
        }
    body = {
        "interactionId": interaction["interactionId"],
        "idempotencyKey": f"complete-{interaction['interactionId']}",
        "responsePayload": encoded_payload(decision_payload, "java.util.Map"),
        "actor": "restaurant-self-host-demo",
    }
    completed = request(
        "POST",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/interactions/complete",
        token=auth(args),
        body=body,
    )
    print(f"Completed interaction {completed['interactionId']} as {decision}")


def complete_invalid_decision(args, interaction):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    decision_payload = {
        "accepted": {
            "orderId": "not-a-uuid",
            "decidedAt": now,
            "note": "This completion is intentionally invalid for the incident demo.",
        }
    }
    body = {
        "interactionId": interaction["interactionId"],
        "idempotencyKey": f"complete-invalid-{interaction['interactionId']}",
        "responsePayload": encoded_payload(decision_payload, "java.util.Map"),
        "actor": "restaurant-self-host-incident-demo",
    }
    completed = request(
        "POST",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/interactions/complete",
        token=auth(args),
        body=body,
    )
    print(f"Completed interaction {completed['interactionId']} with an intentionally invalid decision payload")


def result_payload(args, execution_id):
    result = request(
        "GET",
        f"{args.base_url}/tpf/control-plane/tenants/{args.tenant_id}/executions/{execution_id}/result",
        token=auth(args),
    )
    payload = result["resultPayload"]["payload"]
    return json.loads(payload)


def run_one(args, decision, customer_name, restaurant_name, expected_outcome):
    execution_id = submit_order(args, customer_name, restaurant_name)
    wait_status(args, execution_id, "WAITING_EXTERNAL")
    interaction = pending_interaction(args, execution_id)
    if interaction.get("stepId") != args.await_step_id:
        raise RuntimeError(f"Unexpected await step: {interaction.get('stepId')}")
    complete(args, interaction, decision)
    wait_status(args, execution_id, "SUCCEEDED")
    result = result_payload(args, execution_id)
    if result.get("outcome") != expected_outcome:
        raise RuntimeError(f"Expected outcome={expected_outcome}, got result={result}")
    print(f"Terminal result for {execution_id}: {json.dumps(result, sort_keys=True)}")


def run_flows(args):
    run_one(args, "accepted", "Ada Lovelace", "Cafe TPF", "APPROVED")
    run_one(args, "declined", "Grace Hopper", "Bistro Queue", "DECLINED")


def wait_log_contains(log_file, execution_id, timeout_seconds=20):
    if not log_file:
        return
    path = Path(log_file)
    deadline = time.time() + timeout_seconds
    needle = "Execution moved to DLQ"
    while time.time() < deadline:
        if path.exists():
            contents = path.read_text(errors="replace")
            if needle in contents and execution_id in contents:
                print(f"Observed DLQ publication in {path}: {needle} / execution={execution_id}")
                return
        time.sleep(0.2)
    raise RuntimeError(f"Did not observe DLQ log line for execution {execution_id} in {path}")


def run_incident(args):
    execution_id = submit_order(args, "Katherine Johnson", "Faulty Kitchen")
    wait_status(args, execution_id, "WAITING_EXTERNAL")
    interaction = pending_interaction(args, execution_id)
    complete_invalid_decision(args, interaction)
    terminal = wait_terminal_failure(args, execution_id)
    if terminal.get("errorCode") in {None, ""}:
        raise RuntimeError(f"Expected terminal failure to include errorCode; got {terminal}")
    if terminal.get("errorMessage") in {None, ""}:
        raise RuntimeError(f"Expected terminal failure to include errorMessage; got {terminal}")
    wait_log_contains(args.log_file, execution_id)
    print("Incident triage summary:")
    print(json.dumps({
        "executionId": execution_id,
        "status": terminal.get("status"),
        "attempt": terminal.get("attempt"),
        "errorCode": terminal.get("errorCode"),
        "errorMessage": terminal.get("errorMessage"),
        "operatorAction": "Inspect the failure and re-submit corrected business input; built-in DLQ replay is not provided yet.",
    }, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description="Restaurant Approval self-hosted coordinator demo client")
    sub = parser.add_subparsers(dest="command", required=True)

    health = sub.add_parser("wait-health")
    health.add_argument("--base-url", required=True)
    health.add_argument("--name", required=True)
    health.add_argument("--timeout-seconds", type=int, default=60)
    health.set_defaults(func=wait_health)

    locate = sub.add_parser("locate-bundle")
    locate.add_argument("--target-dir", required=True)
    locate.add_argument("--pipeline-id", required=True)
    locate.set_defaults(func=locate_bundle)

    reg = sub.add_parser("register-activate")
    reg.add_argument("--base-url", required=True)
    reg.add_argument("--tenant-id", required=True)
    reg.add_argument("--pipeline-id", required=True)
    reg.add_argument("--admin-token", required=True)
    reg.add_argument("--release-descriptor-path", required=True)
    reg.add_argument("--worker-id")
    reg.add_argument("--worker-protocol", default="local")
    reg.add_argument("--worker-endpoint", default="in-process")
    reg.set_defaults(func=register_activate)

    release = sub.add_parser("create-release")
    release.add_argument("--pipeline-id", required=True)
    release.add_argument("--artifact-path", required=True)
    release.add_argument("--output", required=True)
    release.add_argument("--release-version")
    release.set_defaults(func=create_release)

    flows = sub.add_parser("run-flows")
    flows.add_argument("--base-url", required=True)
    flows.add_argument("--tenant-id", required=True)
    flows.add_argument("--pipeline-id", required=True)
    flows.add_argument("--await-step-id", required=True)
    flows.add_argument("--control-plane-token", required=True)
    flows.set_defaults(func=run_flows)

    status = sub.add_parser("status")
    status.add_argument("--base-url", required=True)
    status.add_argument("--tenant-id", required=True)
    status.add_argument("--control-plane-token", required=True)
    status.add_argument("--execution-id", required=True)
    status.set_defaults(func=inspect_status)

    pending = sub.add_parser("pending")
    pending.add_argument("--base-url", required=True)
    pending.add_argument("--tenant-id", required=True)
    pending.add_argument("--await-step-id", required=True)
    pending.add_argument("--control-plane-token", required=True)
    pending.add_argument("--execution-id")
    pending.set_defaults(func=query_pending)

    result = sub.add_parser("result")
    result.add_argument("--base-url", required=True)
    result.add_argument("--tenant-id", required=True)
    result.add_argument("--control-plane-token", required=True)
    result.add_argument("--execution-id", required=True)
    result.set_defaults(func=inspect_result)

    incident = sub.add_parser("run-incident")
    incident.add_argument("--base-url", required=True)
    incident.add_argument("--tenant-id", required=True)
    incident.add_argument("--pipeline-id", required=True)
    incident.add_argument("--await-step-id", required=True)
    incident.add_argument("--control-plane-token", required=True)
    incident.add_argument("--log-file")
    incident.add_argument("--timeout-seconds", type=int, default=90)
    incident.set_defaults(func=run_incident)

    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
