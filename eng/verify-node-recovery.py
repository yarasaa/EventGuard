#!/usr/bin/env python3
"""Crash one node of a fresh three-node kind fixture during a real business transaction.

This is a shared-host Kubernetes drill, not physical-server or database-HA qualification.
Only the newly created kind cluster and generated test schema are mutated.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
NODE_IMAGE = 'kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', type=Path, required=True)
    parser.add_argument('--kubectl', type=Path, required=True)
    parser.add_argument('--database-container', default='eventguard-qualification-postgres')
    parser.add_argument('--image', default='eventguard-qualification-worker:dev23')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or not args.database_container.startswith('eventguard-qualification-'):
        parser.error('Use a new output directory and the dedicated qualification database.')
    if shutil.disk_usage(ROOT).free < 6 * 2**30:
        parser.error('At least 6 GiB free host disk is required.')
    output.mkdir(parents=True)
    name = 'eg-qualification-' + uuid.uuid4().hex[:8]
    namespace = 'eventguard-qualification'
    schema = 'eg_cluster_' + uuid.uuid4().hex
    env = os.environ | {'KUBECONFIG': str(output / 'kubeconfig')}
    docker = ['bash', str(ROOT / 'eng/docker.sh')]
    kind = [str(args.kind.resolve())]
    kubectl = [str(args.kubectl.resolve()), '--context', 'kind-' + name, '--request-timeout=20s']
    report = {'method': 'Three kind nodes on one host; one worker node killed during an open SQL transaction',
              'startedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'physicalHostFailover': False, 'databaseFailover': False, 'cluster': name,
              'schema': schema, 'passed': False, 'checks': [], 'cleanup': {}}
    created, attached = False, False

    def run(command, *, stdin=None, timeout=180):
        result = subprocess.run(command, input=stdin, text=True, capture_output=True, env=env, cwd=ROOT, timeout=timeout)
        if result.returncode:
            # Keep diagnostics local; manifests and environment may contain fixture credentials.
            with (output / 'errors.log').open('a') as log:
                log.write(result.stdout + '\n' + result.stderr + '\n')
            raise RuntimeError('Fixture command failed: ' + Path(command[0]).name)
        return result.stdout.strip()

    def sql(query):
        return run(docker + ['exec', args.database_container, 'psql', '-U', 'eventguard', '-d', 'eventguard_test',
                            '-v', 'ON_ERROR_STOP=1', '-Atc', query], timeout=30)

    def wait(predicate, seconds=120):
        stop = time.monotonic() + seconds
        while time.monotonic() < stop:
            if predicate():
                return
            time.sleep(2)
        raise TimeoutError('Bounded cluster check did not complete.')

    def apply(items):
        run(kubectl + ['apply', '-f', '-'], stdin=json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': items}))

    def pod(role, node):
        return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': role, 'namespace': namespace},
                'spec': {'nodeSelector': {'kubernetes.io/hostname': node}, 'restartPolicy': 'Never',
                         'automountServiceAccountToken': False, 'terminationGracePeriodSeconds': 5,
                         'securityContext': {'runAsNonRoot': True, 'runAsUser': 10001, 'seccompProfile': {'type': 'RuntimeDefault'}},
                         'containers': [{'name': 'worker', 'image': args.image, 'imagePullPolicy': 'Never',
                                         'command': ['dotnet', '/app/EventGuard.ClusterWorker.dll', role],
                                         'envFrom': [{'secretRef': {'name': 'database'}}],
                                         'securityContext': {'readOnlyRootFilesystem': True, 'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
                                         'resources': {'requests': {'cpu': '25m', 'memory': '64Mi'}, 'limits': {'cpu': '500m', 'memory': '256Mi'}},
                                         'volumeMounts': [{'name': 'tmp', 'mountPath': '/tmp'}]}],
                         'volumes': [{'name': 'tmp', 'emptyDir': {'sizeLimit': '32Mi'}}]}}

    try:
        inspect = json.loads(run(docker + ['inspect', args.database_container]))[0]
        if inspect['Config']['Image'].split(':')[0] != 'postgres' or not inspect['State']['Running']:
            raise ValueError('Expected the running disposable PostgreSQL fixture.')
        config = output / 'kind.json'
        config.write_text(json.dumps({'kind': 'Cluster', 'apiVersion': 'kind.x-k8s.io/v1alpha4',
                                     'nodes': [{'role': 'control-plane'}, {'role': 'worker'}, {'role': 'worker'}]}))
        created = True
        run(kind + ['create', 'cluster', '--name', name, '--kubeconfig', env['KUBECONFIG'], '--config', str(config),
                    '--image', NODE_IMAGE, '--wait', '180s'], timeout=300)
        nodes = run(kind + ['get', 'nodes', '--name', name]).splitlines()
        if len(nodes) != 3:
            raise RuntimeError('Expected exactly three isolated fixture nodes.')
        for node in nodes:
            info = json.loads(run(docker + ['inspect', node]))[0]
            if info['Config']['Labels'].get('io.x-k8s.kind.cluster') != name:
                raise RuntimeError('Node ownership mismatch.')
            memory = '1536m' if 'control-plane' in node else '1024m'
            run(docker + ['update', '--cpus=1', '--memory=' + memory, '--memory-swap=' + memory, node])
        run(kind + ['load', 'docker-image', args.image, '--name', name], timeout=180)
        run(docker + ['network', 'connect', 'kind', args.database_container]); attached = True
        info = json.loads(run(docker + ['inspect', args.database_container]))[0]
        address = info['NetworkSettings']['Networks']['kind']['IPAddress']
        run(kubectl + ['create', 'namespace', namespace])
        connection = 'Host=eventguard-test-database.eventguard-qualification.svc;Port=5432;Database=eventguard_test;Username=eventguard;Password=eventguard-local-only;Maximum Pool Size=8;Timeout=10;Command Timeout=15'
        apply([
            {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'database', 'namespace': namespace},
             'stringData': {'EVENTGUARD_TEST_CONNECTION': connection, 'EVENTGUARD_TEST_SCHEMA': schema}},
            {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'eventguard-test-database', 'namespace': namespace},
             'spec': {'ports': [{'name': 'postgres', 'port': 5432}]}},
            {'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSlice',
             'metadata': {'name': 'database', 'namespace': namespace, 'labels': {'kubernetes.io/service-name': 'eventguard-test-database'}},
             'addressType': 'IPv4', 'ports': [{'name': 'postgres', 'protocol': 'TCP', 'port': 5432}],
             'endpoints': [{'addresses': [address], 'conditions': {'ready': True}}]}
        ])
        workers = sorted(n for n in nodes if 'control-plane' not in n)
        apply([pod('initialize', workers[1])])
        def initialized():
            phase = json.loads(run(kubectl + ['get', 'pod', 'initialize', '-n', namespace, '-o', 'json']))['status']['phase']
            if phase == 'Failed':
                (output / 'initialize.log').write_text(run(kubectl + ['logs', 'initialize', '-n', namespace]))
                raise RuntimeError('Fixture initialization failed; see initialize.log.')
            return phase == 'Succeeded'
        wait(initialized)
        apply([pod('hold', workers[0])])
        wait(lambda: sql(f"SELECT count(*) FROM {schema}.signals WHERE stage='transaction-open'") == '1')
        apply([pod('peer', workers[1])])
        run(kubectl + ['wait', '--for=condition=Ready', 'pod/peer', '-n', namespace, '--timeout=90s'])
        time.sleep(5)  # Expired lease must not permit a peer to bypass the open transaction's row lock.
        if sql(f'SELECT count(*) FROM {schema}.effects') != '0':
            raise RuntimeError('An effect became visible while the first transaction was still held.')
        report['checks'].append('expired lease cannot bypass a live business transaction')
        report['nodes'] = json.loads(run(kubectl + ['get', 'nodes', '-o', 'json']))
        report['podsBeforeCrash'] = json.loads(run(kubectl + ['get', 'pods', '-n', namespace, '-o', 'json']))
        crash = time.monotonic()
        run(docker + ['kill', '--signal=KILL', workers[0]])
        wait(lambda: sql(f'SELECT count(*) FROM {schema}.works WHERE state=3') == '1', 120)
        report['recoverySeconds'] = time.monotonic() - crash
        observed = sql(f"SELECT (SELECT count(*) FROM {schema}.effects),(SELECT count(*) FROM {schema}.outbox_messages),(SELECT attempt_count FROM {schema}.works),(SELECT count(*) FROM {schema}.attempts WHERE status='Abandoned' AND reason='lease-recovered'),(SELECT count(*) FROM {schema}.effects e JOIN {schema}.works w ON w.id=e.work_id WHERE e.fingerprint=w.fingerprint)")
        report['oracle'] = observed
        if observed != '1|1|2|1|1':
            raise RuntimeError('Recovery did not preserve exactly one committed effect and outbox intent.')
        report['checks'].append('surviving node recovered the abandoned claim; exactly one committed effect and intent')
        run(docker + ['start', workers[0]])
        run(kubectl + ['wait', '--for=condition=Ready', 'node/' + workers[0], '--timeout=120s'], timeout=150)
        # The killed pod is restartPolicy Never. Restoring the node must not replay its committed work.
        time.sleep(3)
        if sql(f'SELECT count(*) FROM {schema}.effects') != '1':
            raise RuntimeError('A restored node duplicated the effect.')
        report['checks'].append('restored node did not repeat the committed effect')
        report['passed'] = True
    except Exception as error:
        report['errorType'] = type(error).__name__
        report['error'] = str(error)
    finally:
        if created:
            try:
                run(kind + ['delete', 'cluster', '--name', name], timeout=120)
                report['cleanup']['clusterRemoved'] = True
            except Exception:
                report['cleanup']['clusterRemoved'] = False
        if attached:
            try:
                run(docker + ['network', 'disconnect', 'kind', args.database_container])
                report['cleanup']['databaseDisconnected'] = True
            except Exception:
                report['cleanup']['databaseDisconnected'] = False
        try:
            sql(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
            report['cleanup']['schemaRemoved'] = True
        except Exception:
            report['cleanup']['schemaRemoved'] = False
        report['passed'] &= all(report['cleanup'].values())
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'output': str(output), 'error': report.get('error')}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
