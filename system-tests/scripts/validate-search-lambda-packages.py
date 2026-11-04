#!/usr/bin/env python3
"""Check the S3-backed Lambda package limit without cloud access or extraction."""
import argparse
import json
from pathlib import Path
import zipfile

MAX_EXPANDED_BYTES = 250 * 1024 * 1024



def inspect_package(path):
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if not entries or all(entry.is_dir() for entry in entries):
            raise ValueError(f"Lambda package contains no files: {path}")
        expanded = sum(entry.file_size for entry in entries)
    if expanded > MAX_EXPANDED_BYTES:
        raise ValueError(f"Lambda package exceeds the 250 MiB expanded limit: {path} ({expanded} bytes)")
    return {"module": path.parent.parent.name, "compressedBytes": path.stat().st_size,
            "expandedBytes": expanded, "upload": "S3"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    packages = sorted(args.root.glob("*/target/function.zip"))
    if not packages:
        parser.error("the build produced no Lambda ZIPs")
    results = [inspect_package(package) for package in packages]
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
