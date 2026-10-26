#!/usr/bin/env python3
"""Exercise a new business DTO/handler outside the checkout using only packaged public APIs."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packages', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--database-container', default='eventguard-qualification-postgres')
    parser.add_argument('--backup-restore', action='store_true', help='Restore a native backup into another new database, then redeliver both messages.')
    args = parser.parse_args()
    output, packages = args.output.resolve(), args.packages.resolve()
    if output.exists() or not args.database_container.startswith('eventguard-qualification-'):
        parser.error('Use a new output directory and dedicated test database container.')
    version = ET.parse(ROOT / 'Directory.Build.props').findtext('.//Version')
    for package in ('EventGuard.Hosting', 'EventGuard.PostgreSql'):
        if not (packages / f'{package}.{version}.nupkg').exists():
            parser.error('Build the current package set first.')
    output.mkdir(parents=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('EVENTGUARD_', 'EventGuard__', 'DOTNET_', 'ASPNETCORE_', 'Dashboard__'))}
    dotnet = ROOT / '.tools/dotnet/dotnet'
    env.update(DOTNET_ROOT=str(dotnet.parent), DOTNET_CLI_HOME=str(output / 'cli-home'),
               NUGET_PACKAGES=str(output / 'nuget-cache'), DOTNET_SKIP_FIRST_TIME_EXPERIENCE='1', DOTNET_CLI_TELEMETRY_OPTOUT='1')
    docker = ['bash', str(ROOT / 'eng/docker.sh'), 'exec', args.database_container,
              'psql', '-U', 'eventguard', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-Atc']
    report = {'independentHumanTrial': False, 'customBusinessModel': True, 'freshPackageCache': True,
              'projectReferences': False, 'version': version, 'cases': [], 'passed': False}

    def run(command, **kwargs):
        r = subprocess.run([str(x) for x in command], capture_output=True, text=True, env=env, timeout=240, **kwargs)
        with (output / 'commands.log').open('a') as log:
            log.write(r.stdout + '\n' + r.stderr + '\n')
        if r.returncode:
            raise RuntimeError('Isolated model check failed; see commands.log.')
        return r.stdout

    try:
        with tempfile.TemporaryDirectory(prefix='eventguard-custom-model-') as directory:
            root = Path(directory)
            (root / 'NuGet.Config').write_text(f'<configuration><packageSources><clear/><add key="candidate" value="{escape(str(packages))}"/><add key="nuget.org" value="https://api.nuget.org/v3/index.json"/></packageSources></configuration>')
            for framework, hosting in [('net8.0', '8.0.1'), ('net10.0', '10.0.12')]:
                project = root / framework; project.mkdir()
                (project / 'Model.csproj').write_text(f'''<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>{framework}</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable></PropertyGroup><ItemGroup><PackageReference Include="EventGuard.Hosting" Version="{version}"/><PackageReference Include="EventGuard.PostgreSql" Version="{version}"/><PackageReference Include="Microsoft.Extensions.Hosting" Version="{hosting}"/></ItemGroup></Project>''')
                (project / 'Program.cs').write_text((ROOT / 'eng/fixtures/custom-model/Program.cs.txt').read_text())
                run([dotnet, 'build', '-c', 'Release'], cwd=project)
                database = 'eventguard_test_model_' + uuid.uuid4().hex
                restored = 'eventguard_test_model_' + uuid.uuid4().hex
                try:
                    run(docker + [f'CREATE DATABASE {database}'])
                    env['EVENTGUARD_TEST_CONNECTION'] = f'Host=127.0.0.1;Port=55432;Database={database};Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=8'
                    result = run([dotnet, project / 'bin/Release' / framework / 'Model.dll'], cwd=project)
                    case = json.loads(next(line for line in result.splitlines() if line.startswith('{')))
                    case['framework'] = framework
                    report['cases'].append(case)
                    if args.backup_restore:
                        archive = output / (framework + '.dump')
                        prefix = ['bash', str(ROOT / 'eng/docker.sh'), 'exec', '-i', args.database_container]
                        with archive.open('xb') as stream:
                            subprocess.run(prefix + ['pg_dump', '-U', 'eventguard', '-d', database, '-Fc'], env=env, stdout=stream, check=True, timeout=120)
                        run(docker + [f'CREATE DATABASE {restored}'])
                        with archive.open('rb') as stream:
                            subprocess.run(prefix + ['pg_restore', '-U', 'eventguard', '-d', restored, '--exit-on-error'], env=env, stdin=stream, check=True, timeout=120)
                        env['EVENTGUARD_TEST_CONNECTION'] = env['EVENTGUARD_TEST_CONNECTION'].replace(database, restored)
                        result = run([dotnet, project / 'bin/Release' / framework / 'Model.dll', 'verify-restored'], cwd=project)
                        case['backupRestore'] = json.loads(next(line for line in result.splitlines() if line.startswith('{')))
                finally:
                    run(docker + [f'DROP DATABASE IF EXISTS {database} WITH (FORCE)'])
                    run(docker + [f'DROP DATABASE IF EXISTS {restored} WITH (FORCE)'])
            report['passed'] = len(report['cases']) == 2 and all(c['passed'] for c in report['cases'])
    finally:
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
