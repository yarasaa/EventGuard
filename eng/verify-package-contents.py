#!/usr/bin/env python3
"""Reject source, deployment, test and credential files from the installed NuGet payload."""
import argparse
import json
from pathlib import Path
import re
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def inspect(path, package, version):
    errors = []
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            errors.append('Duplicate archive entries')
        for name in names:
            allowed = name in {'_rels/.rels', '[Content_Types].xml', 'README.md', 'LICENSE', package + '.nuspec'}
            allowed |= bool(re.fullmatch(r'package/services/metadata/core-properties/(?:[0-9a-f]+|nuget)\.psmdcp', name))
            allowed |= name in {f'lib/{tfm}/{package}.{suffix}' for tfm in ['net8.0', 'net10.0'] for suffix in ['dll', 'xml']}
            if not allowed:
                errors.append('Unexpected package entry: ' + name)
        for required in ['README.md', 'LICENSE', package + '.nuspec', f'lib/net8.0/{package}.dll', f'lib/net10.0/{package}.dll']:
            if required not in names:
                errors.append('Missing package entry: ' + required)
        if package + '.nuspec' in names:
            manifest = ET.fromstring(archive.read(package + '.nuspec'))
            if manifest.findtext('{*}metadata/{*}id') != package or manifest.findtext('{*}metadata/{*}version') != version:
                errors.append('Package identity does not match the selected source version')
            if manifest.findtext('{*}metadata/{*}projectUrl') != 'https://github.com/yarasaa/EventGuard':
                errors.append('Package project URL must identify the public repository')
            repository = manifest.find('{*}metadata/{*}repository')
            if repository is None or repository.get('type') != 'git' or repository.get('url') != 'https://github.com/yarasaa/EventGuard':
                errors.append('Package repository metadata is missing or incorrect')
            license = manifest.find('{*}metadata/{*}license')
            if license is None or license.get('type') != 'expression' or license.text != 'MIT':
                errors.append('Package license must be the MIT expression')
            if manifest.findtext('{*}metadata/{*}readme') != 'README.md':
                errors.append('Package README metadata is missing')
            for dependency in manifest.findall('.//{*}dependency'):
                if dependency.get('id', '').startswith('EventGuard.') and dependency.get('version') != version:
                    errors.append('Mixed EventGuard dependency version: ' + dependency.get('id'))
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packages', type=Path, default=ROOT / '.artifacts/packages')
    parser.add_argument('--version', default=ET.parse(ROOT / 'Directory.Build.props').findtext('.//Version'))
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    projects = [p.stem for p in sorted((ROOT / 'src').glob('*/*.csproj'))
                if ET.parse(p).findtext('.//PackageReadmeFile') == 'README.md']
    report = {'version': args.version, 'packages': [], 'passed': False}
    for package in projects:
        path = args.packages / f'{package}.{args.version}.nupkg'
        errors = inspect(path, package, args.version) if path.is_file() else ['Package is missing']
        report['packages'].append({'id': package, 'errors': errors})
    # Prove that the allowlist would reject an accidental deployment file, even if all real packages pass.
    with tempfile.TemporaryDirectory(prefix='eventguard-package-gate-') as folder:
        probe = Path(folder) / 'forbidden.nupkg'
        with zipfile.ZipFile(probe, 'w') as archive:
            archive.writestr('deploy/openshift/credentials.yaml', 'synthetic negative control')
        report['deploymentFileRejected'] = any(e.startswith('Unexpected package entry: deploy/') for e in inspect(probe, 'Probe', '1'))
    report['passed'] = len(projects) == 16 and report['deploymentFileRejected'] and all(not p['errors'] for p in report['packages'])
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    for package in report['packages']:
        print(package['id'] + ': ' + ('clean' if not package['errors'] else '; '.join(package['errors'])))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
