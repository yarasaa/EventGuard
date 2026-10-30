#!/usr/bin/env python3
"""Run three application-owned models using only candidate NuGet packages and synthetic service behavior."""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from package_fixture import candidate_nuget_config, verify_restored_candidates

ROOT = Path(__file__).resolve().parents[1]


def bulk_readme_example():
    """Compile the actual README registration, submission and provider-adapter snippets."""
    section = (ROOT / 'README.md').read_text().split('### Bulk service calls and partial results (dev.30)\n', 1)[1].split('\n### ', 1)[0]
    snippets = re.findall(r'```csharp\n(.*?)```', section, re.S)
    if len(snippets) != 3:
        raise ValueError('Review the three bulk README examples before changing the onboarding fixture.')
    registration, submission, adapter = snippets
    imports = '\n'.join(line for line in registration.splitlines() if line.startswith('using '))
    body = '\n'.join(line for line in registration.splitlines() if not line.startswith('using '))
    return imports + '\n' + adapter + '''
public static class BulkReadmeExample
{
    public static void Register(IServiceCollection services, string authorizedTenant)
    {
''' + body + '''
    }
    public static async Task Enqueue(SqlServerSession session, ProducerScope scope, Guid stableActionId,
        string customerKey, string requestedPlan, ExternalResultTarget resultTarget, CancellationToken cancellationToken)
    {
''' + submission + '''
    }
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packages', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    packages, output = args.packages.resolve(), args.output.resolve()
    if output.exists():
        parser.error('Choose a new output directory; evidence is not overwritten.')
    version = ET.parse(ROOT / 'Directory.Build.props').findtext('.//Version')
    libraries = ['EventGuard.Core', 'EventGuard.Runtime', 'EventGuard.Hosting', 'EventGuard.PostgreSql', 'EventGuard.SqlServer']
    if any(not (packages / f'{p}.{version}.nupkg').is_file() for p in libraries):
        parser.error('Pack the current version first.')
    output.mkdir(parents=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('DOTNET_', 'EVENTGUARD_', 'MSBUILD', 'NUGET_'))}
    dotnet = ROOT / '.tools/dotnet/dotnet'
    env.update(DOTNET_ROOT=str(dotnet.parent), DOTNET_CLI_HOME=str(output / 'cli-home'), NUGET_PACKAGES=str(output / 'nuget-cache'),
               DOTNET_SKIP_FIRST_TIME_EXPERIENCE='1', DOTNET_CLI_TELEMETRY_OPTOUT='1')
    report = {'version': version, 'packageOnly': True, 'freshCache': True, 'independentHumanTrial': False,
              'realExternalService': False, 'bulkReadmeExamplesCompiled': False, 'restoredCandidates': {}, 'cases': [], 'passed': False}

    def run(command, cwd):
        label = ' '.join(str(x) for x in command)
        print(f'Composition: {label}', flush=True)
        try:
            completed = subprocess.run([str(x) for x in command], cwd=cwd, env=env, capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired as error:
            # TimeoutExpired can retain bytes even when text=True.
            def decode(value):
                return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
            details = decode(error.stdout) + '\n' + decode(error.stderr)
            with (output / 'commands.log').open('a') as log:
                log.write(f'$ {label}\n{details}\nTimed out after 300 seconds.\n')
            print(details[-16000:], file=sys.stderr, flush=True)
            raise RuntimeError(f'Composition command timed out: {label}; see {output / "commands.log"}.') from error
        with (output / 'commands.log').open('a') as log:
            log.write(f'$ {label}\n' + completed.stdout + '\n' + completed.stderr + '\n')
        if completed.returncode:
            print((completed.stdout + '\n' + completed.stderr)[-16000:], file=sys.stderr, flush=True)
            raise RuntimeError(f'Composition command exited {completed.returncode}: {label}; see {output / "commands.log"}.')
        return completed.stdout

    try:
        with tempfile.TemporaryDirectory(prefix='eventguard-composable-') as folder:
            root = Path(folder)
            (root / 'NuGet.Config').write_text(candidate_nuget_config(packages))
            for framework in ['net8.0', 'net10.0']:
                project = root / framework; project.mkdir()
                refs = ''.join(f'<PackageReference Include="{name}" Version="{version}"/>' for name in libraries)
                (project / 'Composition.csproj').write_text(f'<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>{framework}</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable><TreatWarningsAsErrors>true</TreatWarningsAsErrors></PropertyGroup><ItemGroup>{refs}</ItemGroup></Project>')
                (project / 'Program.cs').write_text((ROOT / 'eng/fixtures/composable-execution/Program.cs.txt').read_text())
                (project / 'BulkReadmeExample.cs').write_text(bulk_readme_example())
                run([dotnet, 'restore'], project)
                report['restoredCandidates'][framework] = verify_restored_candidates(
                    project / 'obj/project.assets.json', packages, env['NUGET_PACKAGES'], version)
                run([dotnet, 'build', '-c', 'Release', '--no-restore', '-p:UseSharedCompilation=false'], project)
                for provider in ['postgresql', 'sqlserver']:
                    result = run([dotnet, project / f'bin/Release/{framework}/Composition.dll', provider], project)
                    case = json.loads(next(line for line in result.splitlines() if line.startswith('{')))
                    case['framework'] = framework
                    report['cases'].append(case)
        report['bulkReadmeExamplesCompiled'] = True
        report['passed'] = len(report['cases']) == 4 and all(c['allSucceeded'] and c['uniqueEffects'] == 9 and c['Submits'] == 3 and c['projectedStatuses'] == 3 and c['failedStatusWrites'] == 1 and c['nativeResultProjection'] and c['preparation']['Selected'] == 205 and c['preparation']['Batches'] == 3 and c['preparation']['Actions'] == c['preparation']['Submits'] == 205 and 3 <= c['preparation']['RemoteCalls'] < 205 and c['preparation']['Workflow'] == 'Completed' and c['preparation']['NativeExternalStorage'] for c in report['cases'])
    finally:
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
