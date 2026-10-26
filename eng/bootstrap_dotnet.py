#!/usr/bin/env python3
"""Install pinned official .NET builds into this checkout; Python 3.12+."""

import hashlib
import json
import platform
import sys
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / ".tools" / "dotnet"
DOWNLOADS = ROOT / ".tools" / "downloads"
METADATA = "https://builds.dotnet.microsoft.com/dotnet/release-metadata/{channel}/releases.json"


def install(component: str, version: str, rid: str) -> None:
    installed = DESTINATION / {"sdk": "sdk", "runtime": "shared/Microsoft.NETCore.App", "aspnetcore-runtime": "shared/Microsoft.AspNetCore.App"}[component] / version
    if installed.is_dir():
        print(f"Already installed: {component} {version}", flush=True)
        return
    channel = ".".join(version.split(".")[:2])
    with urllib.request.urlopen(METADATA.format(channel=channel), timeout=60) as response:
        releases = json.load(response)["releases"]
    candidates = []
    for release in releases:
        candidates.extend(release.get("sdks", []) if component == "sdk" else [release.get(component, {})])
    selected = next(item for item in candidates if item.get("version") == version)
    artifact = next(item for item in selected["files"] if item["rid"] == rid and item["url"].endswith(".tar.gz"))
    if not artifact["url"].startswith("https://builds.dotnet.microsoft.com/dotnet/"):
        raise RuntimeError("Unexpected download origin in official release metadata.")
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    archive = DOWNLOADS / f"{component}-{version}-{rid}.tar.gz"
    digest = hashlib.sha512()
    print(f"Downloading {component} {version} ({rid})", flush=True)
    try:
        with urllib.request.urlopen(artifact["url"], timeout=60) as response, archive.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
        if digest.hexdigest().lower() != artifact["hash"].lower():
            raise RuntimeError("SHA-512 verification failed; archive will not be extracted.")
        DESTINATION.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, "r:gz") as package:
            package.extractall(DESTINATION, filter="data")
        print(f"Verified and installed: {component} {version}", flush=True)
    finally:
        archive.unlink(missing_ok=True)


def main() -> None:
    if sys.version_info < (3, 12):
        raise SystemExit("Python 3.12 or newer is required.")
    system = {"Darwin": "osx", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "x64", "AMD64": "x64"}.get(platform.machine())
    if not system or not architecture:
        raise SystemExit("Bootstrap supports macOS/Linux x64/arm64. On Windows, use WSL2 or install .NET manually.")
    sdk = json.loads((ROOT / "global.json").read_text())["sdk"]["version"]
    install("sdk", sdk, f"{system}-{architecture}")
    install("runtime", "8.0.31", f"{system}-{architecture}")
    install("aspnetcore-runtime", "8.0.31", f"{system}-{architecture}")


if __name__ == "__main__":
    main()
