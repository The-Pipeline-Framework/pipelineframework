#!/usr/bin/env python3
"""Exercise a deployed Lambda HTTP control plane with a payload larger than 6 MiB."""

import argparse
import hashlib
import json
import pathlib
import urllib.request
import uuid


def post_json(url, value, headers):
    request = urllib.request.Request(
        url,
        data=json.dumps(value).encode("utf-8"),
        headers={**headers, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def replace_reference(value, reference):
    if value == "{{PAYLOAD_REFERENCE}}":
        return reference
    if isinstance(value, dict):
        return {key: replace_reference(item, reference) for key, item in value.items()}
    if isinstance(value, list):
        return [replace_reference(item, reference) for item in value]
    return value


def at_path(value, path):
    for component in path.split("."):
        value = value[component]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upload-url", required=True, help="Base /upload route")
    parser.add_argument("--download-url", required=True, help="Full /download route")
    parser.add_argument("--bearer-token", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--file", required=True, type=pathlib.Path)
    parser.add_argument("--media-type", required=True)
    parser.add_argument("--reference-field", default="payload_ref")
    parser.add_argument("--admission-url")
    parser.add_argument("--admission-template", type=pathlib.Path)
    parser.add_argument("--result-reference-path")
    args = parser.parse_args()

    length = args.file.stat().st_size
    if length <= 6 * 1024 * 1024:
        parser.error("smoke payload must exceed Lambda's 6 MiB synchronous request limit")
    digest = hashlib.sha256()
    with args.file.open("rb") as source:
        for chunk in iter(lambda: source.read(64 * 1024), b""):
            digest.update(chunk)
    checksum = digest.hexdigest()
    headers = {
        "Authorization": "Bearer " + args.bearer_token,
        "X-Tenant-Id": args.tenant,
        "X-Scope-Id": args.scope,
    }
    base = args.upload_url.rstrip("/")
    reservation = post_json(base + "/initiate", {
        "contentType": args.media_type,
        "bytes": length,
        "sha256": checksum,
        "idempotencyKey": str(uuid.uuid4()),
    }, headers)
    token = reservation["token"]
    part_size = reservation["partSizeBytes"]
    with args.file.open("rb") as source:
        part_number = 1
        while chunk := source.read(part_size):
            capability = post_json(base + "/part", {
                "token": token, "partNumber": part_number,
            }, headers)
            request = urllib.request.Request(
                capability["url"], data=chunk, headers=capability["headers"], method="PUT"
            )
            with urllib.request.urlopen(request, timeout=300) as response:
                if response.status not in (200, 201):
                    raise RuntimeError(f"S3 part {part_number} returned {response.status}")
            part_number += 1
    completed = post_json(base + "/complete", {"token": token}, headers)
    reference = completed[args.reference_field]
    if reference["sizeBytes"] != length or reference["checksum"].lower() != checksum:
        raise RuntimeError("completed reference differs from uploaded content")

    if args.admission_url:
        if not args.admission_template or not args.result_reference_path:
            parser.error("admission requires --admission-template and --result-reference-path")
        template = json.loads(args.admission_template.read_text(encoding="utf-8"))
        admission = replace_reference(template, reference)
        if admission == template:
            parser.error("admission template needs a {{PAYLOAD_REFERENCE}} value")
        admitted = post_json(args.admission_url, admission, headers)
        reference = at_path(admitted, args.result_reference_path)

    capability = post_json(args.download_url, reference, headers)
    if (capability["bytes"] != reference["sizeBytes"] or
            capability["sha256"].lower() != reference["checksum"].lower()):
        raise RuntimeError("download capability differs from requested reference")
    download_request = urllib.request.Request(capability["url"], headers=capability["headers"])
    downloaded = hashlib.sha256()
    downloaded_bytes = 0
    with urllib.request.urlopen(download_request, timeout=300) as response:
        while chunk := response.read(64 * 1024):
            downloaded.update(chunk)
            downloaded_bytes += len(chunk)
    if downloaded_bytes != capability["bytes"] or downloaded.hexdigest() != capability["sha256"].lower():
        raise RuntimeError("downloaded content differs from authorised reference")
    print(json.dumps({"uploadedBytes": length, "downloadedBytes": downloaded_bytes,
                      "sha256": downloaded.hexdigest(), "parts": part_number - 1}))


if __name__ == "__main__":
    main()
