#!/usr/bin/env python3
"""Compare every packed library with an explicit, immutable previous package set."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def check_gate(output):
    """Prove the installed SDK rejects a removed public method before trusting green results."""
    probe = output / 'gate-probe'
    probe.mkdir()
    (probe / 'Probe.csproj').write_text('<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><TargetFramework>net8.0</TargetFramework><PackageId>EventGuard.ApiGateProbe</PackageId><IsPackable>true</IsPackable></PropertyGroup></Project>')
    source = probe / 'Public.cs'
    source.write_text('public class PublicApi { public void RequiredMethod(string input) {} }')
    base = ['bash', str(ROOT / 'eng/dotnet.sh'), 'pack', str(probe / 'Probe.csproj'), '-c', 'Release',
            '-p:UseSharedCompilation=false']
    first = subprocess.run(base + ['-p:Version=1.0.0', '--output', str(probe / 'baseline')], cwd=ROOT, text=True, capture_output=True, timeout=120)
    if first.returncode:
        (probe / 'baseline.log').write_text(first.stdout + first.stderr)
        return False
    source.write_text('public class PublicApi {}')
    removed = subprocess.run(base + ['-p:Version=1.0.1', '--output', str(probe / 'candidate'),
        '-p:EnablePackageValidation=true', '-p:RunApiCompat=true', '-p:DisablePackageBaselineValidation=false',
        '-p:PackageValidationBaselinePath=' + str(probe / 'baseline/EventGuard.ApiGateProbe.1.0.0.nupkg'),
        '-p:_ApiCompatValidatePackageSemaphoreFile=' + str(probe / 'removed.semaphore')],
        cwd=ROOT, text=True, capture_output=True, timeout=120)
    log = removed.stdout + removed.stderr
    (probe / 'removed.log').write_text(log)
    return removed.returncode != 0 and 'CP0002' in log and 'RequiredMethod' in log


def package_identity(path):
    with zipfile.ZipFile(path) as archive:
        names = [n for n in archive.namelist() if n.endswith('.nuspec')]
        if len(names) != 1:
            raise ValueError('Expected one package manifest: ' + path.name)
        root = ET.fromstring(archive.read(names[0]))
        metadata = root.find('{*}metadata')
        return metadata.findtext('{*}id'), metadata.findtext('{*}version')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--baseline-version', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    baseline, output = args.baseline.resolve(), args.output.resolve()
    if output.exists():
        parser.error('Choose a new output directory; evidence is never overwritten.')
    current = ET.parse(ROOT / 'Directory.Build.props').findtext('.//Version')
    projects = [p for p in sorted((ROOT / 'src').glob('*/*.csproj'))
                if ET.parse(p).findtext('.//PackageReadmeFile') == 'README.md']
    inputs = []
    for project in projects:
        package = baseline / f'{project.stem}.{args.baseline_version}.nupkg'
        if not package.is_file() or package_identity(package) != (project.stem, args.baseline_version):
            parser.error('Missing or mismatched baseline: ' + package.name)
        inputs.append((project, package))
    output.mkdir(parents=True)
    report = {'baselineVersion': args.baseline_version, 'candidateVersion': current,
              'method': 'SDK package/API compatibility; parameter names included; no generated suppressions',
              'behavioralCompatibilityProven': False, 'packages': [], 'passed': False}
    try:
        report['removedPublicMethodRejected'] = check_gate(output)
        if not report['removedPublicMethodRejected']:
            raise RuntimeError('API gate self-check failed; see gate-probe logs.')
        for project, package in inputs:
            target = output / 'packages' / f'{project.stem}.{current}.nupkg'
            command = ['bash', str(ROOT / 'eng/dotnet.sh'), 'pack', str(project), '-c', 'Release',
                       '--no-build', '--output', str(target.parent),
                       '-p:EnablePackageValidation=true', '-p:ApiCompatEnableRuleCannotChangeParameterName=true',
                       '-p:RunApiCompat=true', '-p:DisablePackageBaselineValidation=false', '-p:ApiCompatGenerateSuppressionFile=false',
                       '-p:PackageValidationBaselinePath=' + str(package),
                       # Fresh semaphore prevents an earlier Pack from hiding a changed baseline.
                       '-p:_ApiCompatValidatePackageSemaphoreFile=' + str(output / (project.stem + '.semaphore'))]
            with (output / (project.stem + '.log')).open('x') as log:
                result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=120)
            item = {'id': project.stem, 'baselineSha256': hashlib.sha256(package.read_bytes()).hexdigest(),
                    'exitCode': result.returncode, 'passed': result.returncode == 0 and target.is_file()
                    and (output / (project.stem + '.semaphore')).is_file()}
            if target.is_file():
                item['candidateSha256'] = hashlib.sha256(target.read_bytes()).hexdigest()
                item['passed'] &= package_identity(target) == (project.stem, current)
            report['packages'].append(item)
            print(project.stem + ': ' + ('compatible' if item['passed'] else 'FAILED'), flush=True)
        report['passed'] = len(report['packages']) == 16 and all(p['passed'] for p in report['packages'])
    finally:
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
