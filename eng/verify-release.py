#!/usr/bin/env python3
"""Bind a preview release to a successful main-branch CI run and its exact packages."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "yarasaa/EventGuard"


def source_version():
    return ET.parse(ROOT / "Directory.Build.props").findtext(".//Version")


def validate_candidate(version, commit):
    if version != source_version() or not re.fullmatch(r"\d+\.\d+\.\d+-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*", version):
        raise ValueError("Release must use the exact preview version in Directory.Build.props.")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Release requires a full Git commit SHA.")


def validate_run(run, commit):
    expected = {"status": "completed", "conclusion": "success", "head_sha": commit,
                "head_branch": "main", "path": ".github/workflows/ci.yml"}
    if any(run.get(key) != value for key, value in expected.items()):
        raise ValueError("CI must have passed for this exact main-branch commit and workflow.")
    if run.get("event") not in ("push", "workflow_dispatch"):
        raise ValueError("Only main-branch push or manual CI runs can qualify a release.")
    if any(run.get(key, {}).get("full_name") != REPOSITORY for key in ("repository", "head_repository")):
        raise ValueError("CI must belong to the release repository, including its source repository.")


def package_names(version):
    return {f"{p.stem}.{version}.nupkg" for p in (ROOT / "src").glob("*/*.csproj")
            if ET.parse(p).findtext(".//PackageReadmeFile") == "README.md"}


def package_hashes(folder, version):
    expected = package_names(version)
    entries = list(folder.iterdir())
    if len(expected) != 16 or {p.name for p in entries} - {"release.json"} != expected:
        raise ValueError("Release directory must contain exactly the 16 candidate packages and optional release.json.")
    if any(p.is_symlink() or not p.is_file() for p in entries):
        raise ValueError("Release entries must be regular files, not directories or symbolic links.")
    return {name: hashlib.sha256((folder / name).read_bytes()).hexdigest() for name in sorted(expected)}


def verify_manifest(folder, version, commit):
    hashes = package_hashes(folder, version)
    manifest = json.loads((folder / "release.json").read_text())
    expected = {"schemaVersion": 1, "repository": REPOSITORY, "commit": commit,
                "version": version, "sha256": hashes}
    if manifest != expected:
        raise ValueError("Release manifest, version, commit or package hashes do not match.")


def inspect_packages(folder, version):
    subprocess.run([sys.executable, str(ROOT / "eng/verify-package-contents.py"),
                    "--packages", str(folder), "--version", version], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("record", "check-run", "check-artifacts"))
    parser.add_argument("--version", default=source_version())
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run", type=Path)
    parser.add_argument("--packages", type=Path)
    args = parser.parse_args()
    validate_candidate(args.version, args.commit)
    if args.mode == "check-run":
        if args.run is None:
            parser.error("check-run requires --run")
        validate_run(json.loads(args.run.read_text()), args.commit)
    else:
        if args.packages is None:
            parser.error("record and check-artifacts require --packages")
        if args.mode == "record":
            hashes = package_hashes(args.packages, args.version)
            inspect_packages(args.packages, args.version)
            manifest = {"schemaVersion": 1, "repository": REPOSITORY, "commit": args.commit,
                        "version": args.version, "sha256": hashes}
            (args.packages / "release.json").write_text(json.dumps(manifest, indent=2) + "\n")
        else:
            verify_manifest(args.packages, args.version, args.commit)
            inspect_packages(args.packages, args.version)
    print(f"Release check passed: {args.mode}, {args.version}, {args.commit}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Release check failed: {error}")
