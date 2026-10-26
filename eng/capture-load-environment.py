#!/usr/bin/env python3
"""Read-only, narrowly selected host/container metadata; never records environment variables or credentials."""
import datetime
import json
import os
import platform
import subprocess


def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE, timeout=15).strip()


def docker(*args):
    return run('bash', 'eng/docker.sh', *args)


host = {'platform': platform.platform(), 'machine': platform.machine(), 'logicalCpus': os.cpu_count()}
if platform.system() == 'Darwin':
    for key in ('machdep.cpu.brand_string', 'hw.physicalcpu', 'hw.logicalcpu', 'hw.memsize'):
        host[key] = run('sysctl', '-n', key)
info = json.loads(docker('info', '--format', '{{json .}}'))
engine = {key: info.get(key) for key in ('ServerVersion', 'NCPU', 'MemTotal', 'Architecture', 'KernelVersion', 'OperatingSystem')}
containers = []
for name in ('eventguard-local-postgres-1', 'eventguard-local-sqlserver-1'):
    try:
        item = json.loads(docker('inspect', name))[0]
    except subprocess.CalledProcessError:
        containers.append({'name': name, 'metadataUnavailable': True})
        continue
    image = json.loads(docker('image', 'inspect', item['Image']))[0]
    containers.append({'name': name, 'imageReference': item['Config']['Image'], 'imageId': item['Image'],
                       'imageArchitecture': image['Architecture'], 'imageOs': image['Os'],
                       'running': item['State']['Running'], 'resourceLimits': {
                           key: item['HostConfig'].get(key) for key in ('Memory', 'NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus')},
                       'mounts': [{'type': m['Type'], 'destination': m['Destination']} for m in item.get('Mounts', [])]})
# Names/images only, to disclose shared machine load without dumping environment/command lines.
others = [json.loads(line) for line in docker('ps', '--format', '{{json .Names}}').splitlines()]
print(json.dumps({'capturedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'host': host,
                  'dockerEngine': engine, 'databaseContainers': containers, 'otherRunningContainers': others,
                  'limitations': ['Shared desktop and Docker VM, not dedicated benchmark hardware.',
                                  'No DB CPU/wait/WAL/network peak measurement; process metrics cover the harness client only.',
                                  'No broker traffic or HA/failover. Arrival schedule and observed delivery, if enabled, are described in results.json.']}, indent=2))
