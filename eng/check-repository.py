#!/usr/bin/env python3
"""Check public source hygiene, README links and NuGet documentation metadata."""

from fnmatch import fnmatchcase
from pathlib import Path
import json
import re
import struct
import subprocess
import sys
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def source_hygiene_errors(path: Path, content: str) -> list[str]:
    """Return locations and rule names only; never echo possible credential values."""
    errors = []
    parts = {part.lower() for part in path.parts}
    private_directories = {
        ".artifacts", ".tools", ".cache", ".codex", ".agents", ".git", ".kube", ".ssh",
        ".aws", ".azure", ".vs", ".idea", "bin", "obj", "testresults", "__pycache__",
    }
    private_names = (
        ".ds_store", "*.evidence.json", "*.log", "*.user", "*.suo", "*.trx", "*.coverage*",
        "*.pfx", "*.p12", "*.key", "*.pem", "*.p8", "*.jks", "*.keystore", "*.crt", "*.cer",
        "*.nupkg", "*.snupkg", "*.dll", "*.pdb", "*.exe", "*.dmp", "*.dump", "*.bak",
        "*.zip", "*.tar", "*.tar.gz", "*.tgz", "*.db", "*.sqlite", "*.sqlite3",
        "appsettings.local.json", "appsettings.*.local.json", "compose.override.y*ml",
        "secrets.json", "credentials.json", "credentials.yaml", "credentials.yml",
        "kubeconfig", "kubeconfig.*", "*.kubeconfig", "pull-secret*",
    )
    name = path.name.lower()
    if (parts & private_directories or path.parts[0].lower() in {"docs", "research"}
            or any(fnmatchcase(name, pattern) for pattern in private_names)
            or ((name == ".env" or name.startswith(".env.")) and name != ".env.example")):
        errors.append(f"Local-only or generated file in source candidate: {path}")

    # High-confidence signatures supplement manual review, not a comprehensive secret detector.
    # Development fixture passwords are intentional and documented in the README.
    rules = {
        "private-key": r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----",
        "github-token": r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b",
        "aws-access-key": r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
        "slack-token": r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
        "api-token": r"\b(?:sk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,}|AIza[A-Za-z0-9_-]{35})\b",
        "nuget-token": r"\boy2[A-Za-z0-9]{40,}\b",
        "jwt": r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b",
        "machine-home-path": r"/(?:Users|home)/[A-Za-z0-9_.-]+/|[A-Za-z]:\\Users\\[A-Za-z0-9_.-]+\\",
        "macos-temporary-path": r"/(?:private/)?var/folders/[A-Za-z0-9]+/[A-Za-z0-9_-]+/",
    }
    for line_number, line in enumerate(content.splitlines(), 1):
        for rule, pattern in rules.items():
            if re.search(pattern, line):
                errors.append(f"Review {rule} at {path}:{line_number}")
    return errors


def main() -> None:
    # Include new files without staging; exclude ignored local artifacts and deleted files.
    output = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
    )
    files = sorted({Path(p.decode()) for p in output.split(b"\0")
                    if p and ((ROOT / p.decode()).is_file() or (ROOT / p.decode()).is_symlink())})
    errors = []
    markdown = [str(p) for p in files if p.suffix.lower() == ".md"]
    if markdown != ["README.md"]:
        errors.append(f"Expected one maintained Markdown file, README.md; found {markdown}")

    readme = (ROOT / "README.md").read_text()
    # The README uses plain English headings without explicit HTML anchors.
    prose = re.sub(r"```.*?```", "", readme, flags=re.S)
    headings = re.findall(r"^#{1,6} (.+)$", prose, re.M)
    anchors = {re.sub(r"[^\w\- ]", "", h.lower()).replace(" ", "-") for h in headings}
    for target in re.findall(r"\]\(([^)]+)\)", prose):
        url = urlsplit(target)
        if url.scheme or url.netloc:
            continue
        path = ROOT / unquote(url.path) if url.path else ROOT / "README.md"
        if not path.exists():
            errors.append(f"Missing README link target: {target}")
        if path == ROOT / "README.md" and url.fragment and unquote(url.fragment) not in anchors:
            errors.append(f"Missing README heading: {target}")
        if path.is_file() and path.relative_to(ROOT) not in files:
            errors.append(f"README links to a file outside the source candidate: {target}")

    projects = []
    for path in sorted((ROOT / "src").glob("*/*.csproj")):
        tree = ET.parse(path)
        readmes = tree.findall(".//PackageReadmeFile")
        if not readmes:
            continue
        projects.append(path)
        if len(readmes) != 1 or readmes[0].text != "README.md":
            errors.append(f"Invalid package README metadata: {path.relative_to(ROOT)}")
        includes = [n for n in tree.findall(".//None")
                    if n.get("Pack", "").lower() == "true"
                    and (path.parent / n.get("Include", "")).resolve() == ROOT / "README.md"]
        if len(includes) != 1 or includes[0].get("PackagePath") != "/":
            errors.append(f"Package must embed the root README: {path.relative_to(ROOT)}")
        licenses = [n for n in tree.findall(".//None")
                    if n.get("Pack", "").lower() == "true"
                    and (path.parent / n.get("Include", "")).resolve() == ROOT / "LICENSE"]
        if len(licenses) != 1 or licenses[0].get("PackagePath") != "/":
            errors.append(f"Package must embed LICENSE: {path.relative_to(ROOT)}")
    if len(projects) != 16:
        errors.append(f"Review the package inventory: expected 16, found {len(projects)}")
    props = ET.parse(ROOT / "Directory.Build.props")
    if props.findtext(".//PackageLicenseExpression") != "MIT":
        errors.append("Package license expression must match LICENSE (MIT).")
    if props.findtext(".//PackageIcon") != "eventguard.png":
        errors.append("Packages must use the shared EventGuard icon.")
    if not (ROOT / "LICENSE").read_text().startswith("MIT License\n"):
        errors.append("Missing MIT license text.")
    version = props.findtext(".//Version")
    matrix = json.loads((ROOT / "eng/support-matrix.json").read_text())
    if matrix.get("schemaVersion") != 1 or matrix.get("releaseStage") != "experimental":
        errors.append("Review the support matrix format and release stage.")
    for provider, project in (("postgresql", "EventGuard.PostgreSql"), ("sqlserver", "EventGuard.SqlServer")):
        latest = max(int(p.name.split("_", 1)[0]) for p in (ROOT / "src" / project / "Migrations").glob("*.sql"))
        if matrix["stores"][provider]["schema"] != latest:
            errors.append(f"Support matrix schema differs from {provider} migrations.")
    for module, folder in (("jobs", "Jobs"), ("externalActions", "External"), ("snapshots", "Snapshots"), ("workflows", "Workflows")):
        for provider in ("PostgreSql", "SqlServer"):
            latest = max(int(p.name.split("_", 1)[0]) for p in (ROOT / "src" / ("EventGuard." + provider) / folder / "Migrations").glob("*.sql"))
            if matrix["optionalModules"][module]["schema"] != latest:
                errors.append(f"Support matrix differs from {provider} {module} migrations.")
    for project in projects:
        frameworks = ET.parse(project).findtext(".//TargetFrameworks", "").split(";")
        if frameworks != matrix["frameworks"]:
            errors.append(f"Support matrix frameworks differ from {project.stem}.")
    if f"**Status:** experimental `{version}`" not in readme:
        errors.append("README version differs from the package version.")
    pilot = ET.parse(ROOT / "pilots/CatalogPilot/CatalogPilot.csproj")
    if any(n.get("Version") != version for n in pilot.findall(".//PackageReference")
           if n.get("Include", "").startswith("EventGuard.")):
        errors.append("Pilot package versions differ from the source package version.")
    pilot_lock = json.loads((ROOT / "pilots/CatalogPilot/packages.lock.json").read_text())
    if any(entry.get("resolved") != version for framework in pilot_lock["dependencies"].values()
           for name, entry in framework.items() if name.startswith("EventGuard.")):
        errors.append("Pilot lock file refers to a different EventGuard version; reevaluate its local package restore.")
    template = ET.parse(ROOT / "templates/EventGuard.Worker/EventGuardWorker.csproj")
    if template.findall(".//ProjectReference"):
        errors.append("The worker template must consume NuGet packages without project references.")
    if any(n.get("Version") != version for n in template.findall(".//PackageReference")
           if n.get("Include", "").startswith("EventGuard.")):
        errors.append("Worker template package versions differ from the source package version.")

    for path in files:
        source = ROOT / path
        if source.is_symlink():
            errors.append(f"Symlink requires review before inclusion in public source: {path}")
            continue
        if path == Path("src/EventGuard.Dashboard/wwwroot/eventguard.png"):
            data = source.read_bytes()
            if (len(data) < 33 or len(data) > 1_000_000 or data[:8] != b"\x89PNG\r\n\x1a\n"
                    or data[12:16] != b"IHDR"):
                errors.append("The shared logo must be a PNG smaller than 1 MB.")
            else:
                width, height = struct.unpack(">II", data[16:24])
                if width != height or not 32 <= width <= 2048 or data[25] != 6:
                    errors.append("The shared logo must be square RGBA PNG, 32–2048 pixels.")
            continue
        try:
            content = source.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            errors.append(f"Unexpected binary file in public source: {path}")
            continue
        if "\0" in content:
            errors.append(f"Unexpected binary content in public source: {path}")
        errors.extend(source_hygiene_errors(path, content))
    if "file://" in readme:
        errors.append("README contains a machine-specific file link.")

    if errors:
        raise SystemExit("Repository checks failed:\n- " + "\n- ".join(errors))
    print(f"Repository checks passed: {len(files)} source files, one README, {len(projects)} packages, MIT metadata and public-source hygiene.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.CalledProcessError, ET.ParseError) as error:
        print(f"Repository check could not complete: {error}", file=sys.stderr)
        raise SystemExit(1)
