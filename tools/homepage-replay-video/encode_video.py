#!/usr/bin/env python3
"""Encode captured homepage cinematic PNG frames into video assets."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

import imageio_ffmpeg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--fps", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--format", required=True, choices=("webm", "mp4"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    input_pattern = args.input_dir / "frame_%04d.png"
    args.output.parent.mkdir(parents=True, exist_ok=True)

    command = [
        ffmpeg,
        "-y",
        "-framerate",
        str(args.fps),
        "-i",
        str(input_pattern),
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
    ]

    if args.format == "mp4":
        command += ["-c:v", "libx264", "-preset", "slow", "-crf", "21"]
    else:
        command += ["-c:v", "libvpx-vp9", "-b:v", "0", "-crf", "31", "-row-mt", "1"]

    command.append(str(args.output))
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
