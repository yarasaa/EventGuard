#!/usr/bin/env python3
"""Run an existing guarded load pod and retain results plus sampled cluster resources."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--context', required=True)
parser.add_argument('--expected-server', required=True)
parser.add_argument('--namespace', required=True)
parser.add_argument('--pod', default='eventguard-load')
parser.add_argument('--database-namespace', required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--name', required=True)
parser.add_argument('--timeout-seconds', type=int, default=1800)
parser.add_argument('load_options', nargs=argparse.REMAINDER)
args = parser.parse_args()
if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,49}', args.name):
    parser.error('Use a short lowercase case name.')
if not args.namespace.startswith('eventguard-load-') or not 30 <= args.timeout_seconds <= 7200:
    parser.error('Select a dedicated eventguard-load-* namespace and a bounded timeout.')
if args.output.exists():
    parser.error('Choose a new evidence directory; previous results are never overwritten.')
cli = shutil.which('oc')
if not cli:
    parser.error('Put the official oc CLI on PATH first.')
base = [cli, '--context', args.context, '--request-timeout=15s']

def call(*command, timeout=25):
    result = subprocess.run(base + list(command), text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'OpenShift {command[0]} failed (exit {result.returncode}).')
    return result.stdout

server = call('config', 'view', '--minify', '-o', 'jsonpath={.clusters[0].cluster.server}').strip()
if server != args.expected_server:
    parser.error('Selected context does not match the explicitly expected API server.')
pod = json.loads(call('get', 'pod', args.pod, '-n', args.namespace, '-o', 'json'))
node_name = pod['spec'].get('nodeName')
statuses = pod['status'].get('containerStatuses', [])
if not node_name or not statuses or not all(c['ready'] for c in statuses):
    parser.error('The load pod must already be running and ready.')
args.output.mkdir(parents=True)
remote = '/results/' + args.name
options = args.load_options[1:] if args.load_options[:1] == ['--'] else args.load_options
if '--output' in options:
    parser.error('The evidence wrapper owns --output.')

def cgroup(namespace, target):
    try:
        value = call('exec', '-n', namespace, target, '--', 'cat', '/sys/fs/cgroup/cpu.stat')
        return {key: int(value) for key, value in (line.split() for line in value.splitlines())}
    except (RuntimeError, subprocess.TimeoutExpired):
        return None

def database_stats():
    sql = "SELECT row_to_json(t) FROM (SELECT xact_commit,xact_rollback,blks_read,blks_hit,temp_bytes,deadlocks FROM pg_stat_database WHERE datname='eventguard_test') t"
    try:
        return json.loads(call('exec', '-n', args.database_namespace, 'deployment/eventguard-postgres',
                               '--', 'psql', '-U', 'eventguard', '-d', 'eventguard', '-Atc', sql))
    except (RuntimeError, ValueError, subprocess.TimeoutExpired):
        return None

samples = []
def sample():
    summary = json.loads(call('get', '--raw', f'/api/v1/nodes/{node_name}/proxy/stats/summary'))
    node = summary['node']
    selected = [p for p in summary.get('pods', []) if
                (p['podRef']['namespace'] == args.namespace and p['podRef']['name'] == args.pod)
                or (p['podRef']['namespace'] == args.database_namespace and p['podRef']['name'].startswith('eventguard-postgres-'))]
    point = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'nodeCpu': node.get('cpu'), 'nodeMemory': node.get('memory'), 'nodeFs': node.get('fs'),
             'pods': [{'name': p['podRef']['name'], 'namespace': p['podRef']['namespace'],
                       'cpu': p.get('cpu'), 'memory': p.get('memory'),
                       'containers': [{k: c.get(k) for k in ('name', 'cpu', 'memory')} for c in p.get('containers', [])]}
                      for p in selected]}
    samples.append(point)
    if node.get('fs', {}).get('availableBytes', 2**63) < 2 * 2**30:
        raise RuntimeError('Stopped: VM free disk fell below 2 GiB.')
    conditions = json.loads(call('get', 'node', node_name, '-o', 'json'))['status']['conditions']
    if any(c['type'] in ('MemoryPressure', 'DiskPressure') and c['status'] == 'True' for c in conditions):
        raise RuntimeError('Stopped: node reported memory or disk pressure.')
    if shutil.disk_usage(args.output).free < 3 * 2**30:
        raise RuntimeError('Stopped: host free disk fell below 3 GiB.')

database_deployment = json.loads(call('get', 'deployment', 'eventguard-postgres', '-n', args.database_namespace, '-o', 'json'))
database_container = database_deployment['spec']['template']['spec']['containers'][0]
environment = {'context': args.context, 'server': server, 'namespace': args.namespace, 'node': node_name,
               'image': pod['spec']['containers'][0]['image'], 'resources': pod['spec']['containers'][0]['resources'],
               'databaseImage': database_container['image'], 'databaseResources': database_container['resources'],
               'scc': pod['metadata'].get('annotations', {}).get('openshift.io/scc'),
               'sampleIntervalSeconds': 5, 'nodeStatisticsAreSharedWithPlatform': True,
               'loadOptions': options, 'databaseStatsBefore': database_stats(),
               'loadCpuBefore': cgroup(args.namespace, args.pod),
               'databaseCpuBefore': cgroup(args.database_namespace, 'deployment/eventguard-postgres')}
process = None
failure = None
started = time.monotonic()
try:
    sample()
    # The shell writes its PID and exec replaces it with the one bounded load process.
    command = base + ['exec', '-n', args.namespace, args.pod, '--', 'sh', '-c',
                     f'echo $$ > {remote}.pid; exec dotnet /app/EventGuard.LoadTests.dll "$@"',
                     'load', '--output', remote + '.json'] + options
    with (args.output / 'run.log').open('x') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, text=True)
        while process.poll() is None:
            if time.monotonic() - started > args.timeout_seconds:
                raise RuntimeError('Stopped: experiment wall-clock deadline exceeded.')
            time.sleep(5)
            sample()
        environment['exitCode'] = process.returncode
except BaseException as error:
    failure = type(error).__name__ + ': ' + str(error)
finally:
    if process is not None and process.poll() is None:
        # Verify the remote PID's command before signalling only this experiment.
        try:
            pid = int(call('exec', '-n', args.namespace, args.pod, '--', 'cat', remote + '.pid'))
            cmdline = call('exec', '-n', args.namespace, args.pod, '--', 'cat', f'/proc/{pid}/cmdline')
            if '/app/EventGuard.LoadTests.dll' in cmdline and remote + '.json' in cmdline:
                call('exec', '-n', args.namespace, args.pod, '--', 'kill', '-INT', str(pid))
                try:
                    process.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    call('exec', '-n', args.namespace, args.pod, '--', 'kill', '-TERM', str(pid))
                    process.wait(timeout=10)
        except (RuntimeError, ValueError, subprocess.TimeoutExpired):
            failure = (failure or '') + ' Remote cancellation needs inspection.'
        if process.poll() is None:
            process.terminate()
    environment.update({'elapsedSeconds': time.monotonic() - started, 'wrapperFailure': failure,
                        'databaseStatsAfter': database_stats(),
                        'loadCpuAfter': cgroup(args.namespace, args.pod),
                        'databaseCpuAfter': cgroup(args.database_namespace, 'deployment/eventguard-postgres')})
    (args.output / 'resources.json').write_text(json.dumps(samples, indent=2) + '\n')
    try:
        result = call('exec', '-n', args.namespace, args.pod, '--', 'cat', remote + '.json')
        json.loads(result)
        (args.output / 'results.json').write_text(result)
    except (RuntimeError, ValueError, subprocess.TimeoutExpired):
        failure = failure or 'No readable load result.'
    environment['wrapperFailure'] = failure
    (args.output / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')
print(json.dumps({'output': str(args.output), 'exitCode': environment.get('exitCode'), 'failure': failure}))
raise SystemExit(1 if failure else environment.get('exitCode', 1))
