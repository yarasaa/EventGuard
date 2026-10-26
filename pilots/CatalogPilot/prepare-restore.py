#!/usr/bin/env python3
"""Bind only source-built EventGuard hashes to this local pilot build.

The checked-in lock template keeps exact dependency versions and upstream hashes.
Locally packed EventGuard archives differ between source checkouts/build hosts. Derive
their hashes from the explicit local feed, then let NuGet validate the entire resulting
lock in locked mode. Never change third-party entries or use this for published packages.
"""

import base64
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[2]
APP = Path(__file__).resolve().parent


def main() -> None:
    lock = json.loads((APP / "packages.lock.json").read_text())
    packages = {}
    for framework in lock["dependencies"].values():
        for name, entry in framework.items():
            if not name.startswith("EventGuard."):
                continue
            version = entry["resolved"]
            key = (name, version)
            if key not in packages:
                path = ROOT / ".artifacts/packages" / f"{name}.{version}.nupkg"
                if not path.is_file():
                    raise ValueError(f"Build the local packages first: missing {path.name}")
                with zipfile.ZipFile(path) as archive:
                    nuspecs = [n for n in archive.namelist() if n.endswith(".nuspec")]
                    if len(nuspecs) != 1:
                        raise ValueError(f"Invalid local package: {path.name}")
                    metadata = ET.fromstring(archive.read(nuspecs[0]))
                    if metadata.findtext("{*}metadata/{*}id") != name or metadata.findtext("{*}metadata/{*}version") != version:
                        raise ValueError(f"Local package identity mismatch: {path.name}")
                packages[key] = base64.b64encode(hashlib.sha512(path.read_bytes()).digest()).decode()
            entry["contentHash"] = packages[key]
    if len(packages) != 6:
        raise ValueError("Review the pilot's expected six EventGuard package dependencies.")
    content = json.dumps(lock, indent=2) + "\n"
    identity = hashlib.sha256(content.encode()).hexdigest()
    destination = ROOT / ".artifacts/catalog-pilot-restore" / identity
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "packages.lock.json").write_text(content)
    print(destination)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, ET.ParseError, zipfile.BadZipFile) as error:
        print(f"Pilot restore preparation failed: {error}", file=sys.stderr)
        raise SystemExit(1)
