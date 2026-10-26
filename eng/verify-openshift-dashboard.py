#!/usr/bin/env python3
"""Bounded HTTPS/OIDC/replica/TLS rehearsal on the existing local CRC fixture only.

Creates an isolated namespace, two dashboard replicas and a disposable Keycloak realm.
Uses a SELECT-only identity for the existing synthetic worker schema. Never changes CRC
credentials, router certificates or application records. Fixture Secrets are removed with
the namespace. Requires requests, openssl, oc, CRC and the published dashboard context.
"""
import argparse
import base64
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import traceback
import uuid
from urllib.parse import urljoin, urlparse, parse_qs
import requests

ROOT = Path(__file__).resolve().parents[1]


class Forms(HTMLParser):
    def __init__(self, html):
        super().__init__(); self.forms = []; self.current = None; self.feed(html)
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'form':
            self.current = {'action': attrs.get('action', ''), 'fields': {}}; self.forms.append(self.current)
        elif tag == 'input' and self.current is not None and attrs.get('name'):
            self.current['fields'][attrs['name']] = attrs.get('value', '')
    def handle_endtag(self, tag):
        if tag == 'form': self.current = None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--oc', type=Path, required=True)
    p.add_argument('--kubeconfig', type=Path, required=True)
    p.add_argument('--context', default='admin')
    p.add_argument('--published-dashboard', type=Path, required=True)
    p.add_argument('--work-id', type=uuid.UUID, required=True, help='A completed synthetic product.changed record in the existing CRC worker fixture.')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    output = args.output.resolve(); published = args.published_dashboard.resolve()
    if output.exists() or not (published / 'EventGuard.Dashboard.dll').is_file() or not (published / 'Dockerfile').is_file():
        p.error('A fresh output directory and published dashboard Docker context are required.')
    output.mkdir(parents=True, mode=0o700)
    namespace = 'eg-dashboard-qualification-' + secrets.token_hex(4)
    role = 'eg_qual_' + secrets.token_hex(8)
    schema = 'eventguard_crc_verified'
    env = os.environ | {'KUBECONFIG': str(args.kubeconfig.resolve())}
    oc = [str(args.oc.resolve()), '--context', args.context, '--request-timeout=30s']
    report = {'namespace': namespace, 'keyRingStorage': 'RWO shared by replicas on the single CRC node; not multi-node storage qualification', 'environment': 'CRC single node', 'realOidc': False,
              'productionIdentityProvider': False, 'physicalHostFailover': False, 'readRoleName': role,
              'dashboardRollout': 'maxSurge=0, maxUnavailable=1; fits two replica slots on local CRC',
              'checks': [], 'passed': False, 'cleanup': {}}
    created = False; role_created = False

    def stage(value):
        report['stage'] = value
        (output / 'progress.json').write_text(json.dumps({'stage': value, 'namespace': namespace}) + '\n')
        print(value, flush=True)

    def run(command, stdin=None, timeout=120, expected=0):
        r = subprocess.run(command, input=stdin, text=True, capture_output=True, env=env, cwd=ROOT, timeout=timeout)
        if r.returncode != expected:
            # Errors can include fixture settings. Keep the output private, never echo raw logs.
            with (output / 'errors.log').open('a') as f: f.write(r.stdout + '\n' + r.stderr + '\n')
            raise RuntimeError('Fixture command failed: ' + Path(command[0]).name)
        return r.stdout.strip()

    def apply(items):
        run(oc + ['apply', '-f', '-'], json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': items}))

    def resource(kind, name, **values):
        return {'apiVersion': 'v1', 'kind': kind, 'metadata': {'name': name, 'namespace': namespace}, **values}

    def sql(query):
        return run(oc + ['exec', '-i', '-n', 'eventguard-local-test', 'deployment/eventguard-postgres', '--',
                        'psql', '-U', 'eventguard', '-d', 'eventguard', '-v', 'ON_ERROR_STOP=1', '-At'], query)

    def wait(check, seconds=120):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            value = check()
            if value: return value
            time.sleep(2)
        raise TimeoutError('Bounded fixture check timed out.')

    def get(kind, name):
        return json.loads(run(oc + ['get', kind, name, '-n', namespace, '-o', 'json']))

    def deployment(name, image, container, volumes, replicas=1, selector=None):
        container.update(name=name, image=image, resources={'requests': {'cpu': '50m', 'memory': '128Mi'},
            'limits': {'cpu': '1', 'memory': '640Mi' if name == 'idp' else '384Mi'}},
            securityContext={'runAsNonRoot': True, 'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']},
                             'readOnlyRootFilesystem': name != 'idp'})
        return {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': name, 'namespace': namespace},
                'spec': {'replicas': replicas, **({'strategy': {'type': 'RollingUpdate', 'rollingUpdate': {'maxSurge': 0, 'maxUnavailable': 1}}} if replicas == 2 else {}),
                         'selector': {'matchLabels': {'app': name}},
                         'template': {'metadata': {'labels': {'app': name}}, 'spec': {
                             'automountServiceAccountToken': False, 'securityContext': {'seccompProfile': {'type': 'RuntimeDefault'}},
                             'containers': [container], 'volumes': volumes}}}}

    def rollout(name):
        run(oc + ['rollout', 'status', 'deployment/' + name, '-n', namespace, '--timeout=240s', '--request-timeout=0'], timeout=260)

    try:
        cluster = json.loads(run(oc + ['config', 'view', '--minify', '-o', 'json']))
        if cluster['clusters'][0]['cluster']['server'] != 'https://api.crc.testing:6443':
            raise ValueError('Only the existing local CRC test cluster is allowed.')
        worker = json.loads(run(oc + ['get', 'deployment', 'eventguard-worker', '-n', 'eventguard-local-test', '-o', 'json']))
        worker_env = {v['name']: v.get('value') for c in worker['spec']['template']['spec']['containers'] for v in c.get('env', [])}
        if worker_env.get('EventGuard__Schema') != schema:
            raise ValueError('Expected the verified synthetic worker schema.')
        # Refuse to run against a fixture that no longer contains its known synthetic record.
        if sql(f"SELECT count(*) FROM {schema}.works WHERE id='{args.work_id}' AND state=3") != '1':
            raise ValueError('Expected the existing completed synthetic fixture.')
        config = json.loads(sql(f"SELECT json_build_object('EventGuard__Tenant',p.tenant,'EventGuard__Consumer',p.consumer,'EventGuard__Source',p.source,'EventType',p.event_type) FROM {schema}.policy_bindings p JOIN {schema}.works w ON w.scope_id=p.scope_id WHERE w.id='{args.work_id}'"))
        if config['EventType'] != 'product.changed':
            raise ValueError('Expected a synthetic product.changed record.')
        stage('Preparing the isolated CRC identity and dashboard fixture')
        run(oc + ['create', 'namespace', namespace]); created = True
        domain = 'apps-crc.testing'
        base = 'https://dashboard-' + namespace + '.' + domain
        authority_host = 'https://idp-' + namespace + '.' + domain
        authority = authority_host + '/realms/eventguard'
        # Public router certificate chain only. No router private key is accessed.
        encoded = run(oc + ['get', 'secret', 'router-certs-default', '-n', 'openshift-ingress', '-o', 'jsonpath={.data.tls\\.crt}'])
        ca = base64.b64decode(encoded).decode(); ca_path = output / 'router-ca.pem'; ca_path.write_text(ca)
        credentials = {'reader': secrets.token_hex(16), 'denied': secrets.token_hex(16)}
        client_secret = secrets.token_hex(24); db_password = secrets.token_hex(24)
        realm = {'realm': 'eventguard', 'enabled': True, 'registrationAllowed': False, 'sslRequired': 'all',
                 'users': [{'username': name, 'enabled': True, 'emailVerified': True,
                            'email': name + '@example.invalid', 'firstName': 'Fixture', 'lastName': name,
                            'attributes': {'eventguard_scope': ['crc-dashboard/products']} if name == 'reader' else {},
                            'credentials': [{'type': 'password', 'value': password, 'temporary': False}]} for name, password in credentials.items()],
                 'clients': [{'clientId': 'eventguard-dashboard', 'enabled': True, 'publicClient': False, 'secret': client_secret,
                              'standardFlowEnabled': True, 'directAccessGrantsEnabled': False,
                              'redirectUris': [base + '/signin-oidc'], 'webOrigins': [base],
                              'attributes': {'pkce.code.challenge.method': 'S256'},
                              'protocolMappers': [{'name': 'scope', 'protocol': 'openid-connect', 'protocolMapper': 'oidc-usermodel-attribute-mapper',
                                                  'config': {'user.attribute': 'eventguard_scope', 'claim.name': 'eventguard_scope', 'jsonType.label': 'String',
                                                             'multivalued': 'true', 'id.token.claim': 'true', 'access.token.claim': 'true'}}]}]}
        service_ca = resource('ConfigMap', 'service-ca', data={})
        service_ca['metadata']['annotations'] = {'service.beta.openshift.io/inject-cabundle': 'true'}
        svc = resource('Service', 'dashboard', spec={'selector': {'app': 'dashboard'}, 'ports': [{'name': 'https', 'port': 8443}]})
        svc['metadata']['annotations'] = {'service.beta.openshift.io/serving-cert-secret-name': 'dashboard-tls'}
        apply([service_ca, svc, resource('Secret', 'realm', stringData={'realm.json': json.dumps(realm)}),
               resource('ConfigMap', 'router-ca', data={'ca.pem': ca}),
               resource('Service', 'idp', spec={'selector': {'app': 'idp'}, 'ports': [{'name': 'http', 'port': 8080}]}),
               resource('PersistentVolumeClaim', 'keys', spec={'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '64Mi'}}})])
        service_pem = wait(lambda: get('configmap', 'service-ca').get('data', {}).get('service-ca.crt'))
        apply([{'apiVersion': 'route.openshift.io/v1', 'kind': 'Route', 'metadata': {'name': name, 'namespace': namespace},
                'spec': {'host': urlparse(host).hostname, 'to': {'kind': 'Service', 'name': name}, 'port': {'targetPort': port},
                         'tls': tls}} for name, host, port, tls in [
                             ('idp', authority_host, 'http', {'termination': 'edge', 'insecureEdgeTerminationPolicy': 'Redirect'}),
                             ('dashboard', base, 'https', {'termination': 'reencrypt', 'insecureEdgeTerminationPolicy': 'Redirect', 'destinationCACertificate': service_pem})]])
        apply([deployment('idp', 'quay.io/keycloak/keycloak:26.8.0@sha256:27b3230fbabb8c9ff3c66c864cf09dcd8a41aca82e5100679d109ef0eeb05741',
                          {'args': ['start-dev', '--import-realm'], 'ports': [{'containerPort': 8080}],
                           'env': [{'name': k, 'value': v} for k, v in {'KC_HOSTNAME': authority_host, 'KC_PROXY_HEADERS': 'xforwarded',
                                    'JAVA_OPTS_KC_HEAP': '-Xms128m -Xmx384m'}.items()],
                           'readinessProbe': {'httpGet': {'path': '/realms/eventguard/.well-known/openid-configuration', 'port': 8080,
                               'httpHeaders': [{'name': 'X-Forwarded-Proto', 'value': 'https'}, {'name': 'X-Forwarded-Port', 'value': '443'}]}, 'initialDelaySeconds': 20},
                           'volumeMounts': [{'name': 'data', 'mountPath': '/opt/keycloak/data'}, {'name': 'realm', 'mountPath': '/opt/keycloak/data/import', 'readOnly': True}, {'name': 'tmp', 'mountPath': '/tmp'}]},
                          [{'name': 'realm', 'secret': {'secretName': 'realm'}}, {'name': 'data', 'emptyDir': {}}, {'name': 'tmp', 'emptyDir': {}}])])
        # Protected cookie keys: ephemeral private certificate, persisted encrypted key ring.
        run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2', '-subj', '/CN=EventGuard qualification',
             '-keyout', str(output / 'key.pem'), '-out', str(output / 'certificate.pem')])
        run(['openssl', 'pkcs12', '-export', '-out', str(output / 'certificate.pfx'), '-inkey', str(output / 'key.pem'),
             '-in', str(output / 'certificate.pem'), '-passout', 'pass:'])
        sql(f"CREATE ROLE {role} LOGIN PASSWORD '{db_password}'; GRANT CONNECT ON DATABASE eventguard TO {role}; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};")
        role_created = True
        sql(f"CREATE INDEX IF NOT EXISTS ix_ops_created ON {schema}.works(scope_id,created_at DESC,id DESC); CREATE INDEX IF NOT EXISTS ix_ops_state_created ON {schema}.works(scope_id,state,created_at DESC,id DESC); CREATE INDEX IF NOT EXISTS ix_ops_replays ON {schema}.replay_requests(scope_id,work_id,created_at DESC,id DESC);")
        host = urlparse(base).hostname
        settings = {'Provider': 'PostgreSql', 'Schema': schema, 'StoreId': 'crc-dashboard', 'ApplicationLabel': 'Qualification', 'EnvironmentLabel': 'CRC test',
                    'AllowedHosts': [host, 'dashboard.' + namespace + '.svc'], 'MonitoringEnabled': True,
                    'Authority': authority, 'ClientId': 'eventguard-dashboard', 'ClientSecret': client_secret,
                    'ConnectionString': f'Host=eventguard-postgres.eventguard-local-test.svc;Database=eventguard;Username={role};Password={db_password}',
                    'KeyRingPath': '/var/lib/eventguard/keys', 'KeyProtectionCertificatePath': '/run/key-protection/certificate.pfx',
                    'Scopes': [{'Key': 'products', 'Label': 'Product projection', 'Tenant': config['EventGuard__Tenant'],
                                'Consumer': config['EventGuard__Consumer'], 'Source': config['EventGuard__Source'], 'EventType': 'product.changed'}]}
        apply([resource('Secret', 'dashboard-config', stringData={'appsettings.Production.json': json.dumps({'Dashboard': settings})}),
               resource('Secret', 'key-protection', data={'certificate.pfx': base64.b64encode((output / 'certificate.pfx').read_bytes()).decode()})])
        for private in ('key.pem', 'certificate.pfx'): (output / private).unlink()
        stage('Building the published dashboard in OpenShift')
        run(oc + ['new-build', '--binary', '--name=dashboard', '--strategy=docker', '-n', namespace])
        run(oc + ['start-build', 'dashboard', '--from-dir=' + str(published), '--follow', '--wait', '-n', namespace, '--request-timeout=0'], timeout=480)
        # The image has been pushed. Retaining the completed builder's writable
        # storage can trigger DiskPressure on CRC before application pods start.
        run(oc + ['delete', 'pod', 'dashboard-1-build', '-n', namespace, '--ignore-not-found=true', '--wait=false'])
        def node_ready():
            node = json.loads(run(oc + ['get', 'node', 'crc', '-o', 'json']))
            conditions = {c['type']: c['status'] for c in node['status']['conditions']}
            return conditions.get('Ready') == 'True' and conditions.get('DiskPressure') == 'False' and conditions.get('MemoryPressure') == 'False'
        # Preserve kubelet eviction safeguards, including the normal five-minute
        # recovery hysteresis. Do not remove its taints or relax its thresholds.
        stage('Waiting for CRC pressure checks after builder cleanup')
        wait(node_ready, 360)
        image = 'image-registry.openshift-image-registry.svc:5000/' + namespace + '/dashboard:latest'
        mounts = [('config', '/app/appsettings.Production.json', 'appsettings.Production.json'), ('tls', '/run/tls', None),
                  ('key-protection', '/run/key-protection', None), ('keys', '/var/lib/eventguard/keys', None), ('ca', '/run/trust', None), ('tmp', '/tmp', None)]
        container = {'ports': [{'name': 'https', 'containerPort': 8443}],
                     'env': [{'name': k, 'value': v} for k, v in {'ASPNETCORE_ENVIRONMENT': 'Production', 'SSL_CERT_FILE': '/run/trust/ca.pem',
                              'Kestrel__Certificates__Default__Path': '/run/tls/tls.crt', 'Kestrel__Certificates__Default__KeyPath': '/run/tls/tls.key'}.items()],
                     'volumeMounts': [{'name': n, 'mountPath': path, **({'subPath': sub} if sub else {})} for n, path, sub in mounts],
                     'readinessProbe': {'httpGet': {'path': '/health/ready', 'port': 8443, 'scheme': 'HTTPS', 'httpHeaders': [{'name': 'Host', 'value': host}]}, 'initialDelaySeconds': 5}}
        volumes = [{'name': 'config', 'secret': {'secretName': 'dashboard-config'}}, {'name': 'tls', 'secret': {'secretName': 'dashboard-tls'}},
                   {'name': 'key-protection', 'secret': {'secretName': 'key-protection'}}, {'name': 'keys', 'persistentVolumeClaim': {'claimName': 'keys'}},
                   {'name': 'ca', 'configMap': {'name': 'router-ca'}}, {'name': 'tmp', 'emptyDir': {}}]
        apply([deployment('dashboard', image, container, volumes, replicas=2)])
        stage('Waiting for the identity provider and two dashboard replicas')
        rollout('idp'); rollout('dashboard')
        session = requests.Session(); session.verify = str(ca_path); session.trust_env = False
        def discovery_ready():
            try:
                response = session.get(authority + '/.well-known/openid-configuration', timeout=10)
                return response.status_code == 200 and response.json().get('issuer') == authority
            except (requests.RequestException, ValueError):
                return False
        # Pod readiness can precede propagation of its Route backends to HAProxy.
        wait(discovery_ready)
        assert session.get(base + '/api/scopes', timeout=20).status_code == 401
        report['checks'].append('unauthenticated API denied')

        def login(name):
            s = requests.Session(); s.verify = str(ca_path); s.trust_env = False
            challenge = s.get(base + '/sign-in', timeout=30, allow_redirects=False)
            assert challenge.status_code == 302
            location = challenge.headers['Location']
            assert location.startswith(authority_host + '/')
            query = parse_qs(urlparse(location).query)
            assert query.get('client_id') == ['eventguard-dashboard']
            if 'request_uri' in query:
                # .NET 9+ uses PAR when discovery advertises it. Code/PKCE
                # parameters then travel over the backchannel, not the redirect.
                # This fixture's client permits only code flow and requires S256;
                # successful login/token redemption enforces both at the real IdP.
                assert len(query['request_uri']) == 1 and query['request_uri'][0].startswith('urn:ietf:params:oauth:request_uri:')
                report['authorizationTransport'] = 'pushed authorization request; provider requires code and S256'
            else:
                assert query.get('response_type') == ['code'] and query.get('code_challenge_method') == ['S256']
                report['authorizationTransport'] = 'redirect parameters with code and S256'
            # Keycloak may redirect again to a login-action URL without these
            # parameters. Validate the middleware's challenge before following it.
            r = s.get(location, timeout=30)
            assert urlparse(r.url).hostname == urlparse(authority_host).hostname
            form = next(f for f in Forms(r.text).forms if 'username' in f['fields'])
            action = urljoin(r.url, form['action'])
            assert urlparse(action).hostname == urlparse(authority_host).hostname and action.startswith('https://')
            r = s.post(action, data=form['fields'] | {'username': name, 'password': credentials[name]}, timeout=30)
            # ASP.NET Core requests response_mode=form_post. Submit only the exact callback.
            for form in Forms(r.text).forms:
                if 'code' in form['fields'] and 'state' in form['fields']:
                    action = urljoin(r.url, form['action']); assert action == base + '/signin-oidc'
                    r = s.post(action, data=form['fields'], timeout=30); break
            # A signed-in user without a registered scope receives a deliberate 404
            # on the home page. The cookie and API checks below distinguish this from
            # a failed OIDC callback.
            assert r.status_code == (200 if name == 'reader' else 404) and urlparse(r.url).hostname == host and '/auth-error' not in r.url
            cookie = next(c for c in s.cookies if c.name == '__Host-EventGuard')
            # Cookie attributes are case-insensitive; HAProxy normalizes HttpOnly.
            http_only = cookie.has_nonstandard_attr('HttpOnly') or cookie.has_nonstandard_attr('httponly')
            report['cookieFlags'] = {'secure': cookie.secure, 'path': cookie.path, 'httpOnly': http_only}
            assert cookie.secure and cookie.path == '/' and http_only
            return s

        reader = login('reader'); denied = login('denied')
        report['realOidc'] = True
        stage('Checking scope authorization and persistent sessions')
        assert reader.get(base + '/api/scopes', timeout=20).json() == [{'key': 'products', 'label': 'Product projection'}]
        assert denied.get(base + '/api/scopes', timeout=20).json() == []
        assert denied.get(base + '/api/works?scope=products', timeout=20).status_code == 404
        path = '/api/works/products/' + str(args.work_id)
        assert reader.get(base + path, timeout=20).status_code == 200
        report['checks'].append('real authorization-code/PKCE login and permitted/denied scope claims')
        pods = json.loads(run(oc + ['get', 'pods', '-n', namespace, '-l', 'app=dashboard', '-o', 'json']))['items']
        report['replicas'] = [{'scc': x['metadata']['annotations'].get('openshift.io/scc'),
                               'uid': x['spec']['containers'][0]['securityContext'].get('runAsUser', x['spec'].get('securityContext', {}).get('runAsUser')),
                               'imageId': x['status']['containerStatuses'][0]['imageID']} for x in pods]
        assert len(pods) == 2 and all(x['scc'] == 'restricted-v2' and isinstance(x['uid'], int) and x['uid'] > 0 for x in report['replicas'])
        run(oc + ['rollout', 'restart', 'deployment/dashboard', '-n', namespace]); rollout('dashboard')
        assert reader.get(base + path, timeout=20).status_code == 200
        report['checks'].append('existing login survives two-replica rolling restart with protected persistent keys')
        stage('Renewing the fixture service certificate and checking login again')
        before = get('secret', 'dashboard-tls')['data']['tls.crt']
        run(oc + ['delete', 'secret', 'dashboard-tls', '-n', namespace])
        def renewed():
            data = json.loads(run(oc + ['get', 'secrets', '-n', namespace, '-o', 'json']))['items']
            return next((s['data']['tls.crt'] for s in data if s['metadata']['name'] == 'dashboard-tls' and s['data']['tls.crt'] != before), None)
        after = wait(renewed, 180)
        run(oc + ['rollout', 'restart', 'deployment/dashboard', '-n', namespace]); rollout('dashboard')
        assert reader.get(base + path, timeout=20).status_code == 200
        assert login('reader').get(base + path, timeout=20).status_code == 200
        report['tls'] = {'beforeSha256': hashlib.sha256(base64.b64decode(before)).hexdigest(), 'afterSha256': hashlib.sha256(base64.b64decode(after)).hexdigest(),
                         'routerCertificateRotated': False, 'backendServingCertificateRotated': True, 'certificateValidationDisabled': False}
        report['checks'].append('renewed service certificate accepted by reencrypt route; old and fresh sessions work')
        report['identityImage'] = get('deployment', 'idp')['spec']['template']['spec']['containers'][0]['image']
        report['passed'] = True
    except Exception as e:
        report['errorType'] = type(e).__name__; report['error'] = str(e)
        report['errorLine'] = traceback.extract_tb(e.__traceback__)[-1].lineno
        if created:
            try:
                probe = 'printf "GET /realms/eventguard/.well-known/openid-configuration HTTP/1.1\\r\\nHost: %s\\r\\nConnection: close\\r\\n\\r\\n" "$1" | openssl s_client -connect "$1:443" -servername "$1" -CAfile /run/trust/ca.pem -verify_return_error -quiet'
                response = subprocess.run(oc + ['exec', 'deployment/dashboard', '-n', namespace, '--', 'sh', '-c', probe, '--', urlparse(authority_host).hostname],
                    capture_output=True, text=True, env=env, timeout=30)
                (output / 'discovery-probe.log').write_text(response.stdout + '\n' + response.stderr)
            except Exception: pass
            try:
                for app in ('dashboard', 'idp'):
                    logs = run(oc + ['logs', 'deployment/' + app, '-n', namespace, '--tail=80'])
                    (output / (app + '.log')).write_text(logs)
            except Exception: pass
    finally:
        if created:
            try:
                run(oc + ['delete', 'namespace', namespace, '--wait=true', '--timeout=150s', '--request-timeout=0'], timeout=170)
                report['cleanup']['namespaceRemoved'] = True
            except Exception: report['cleanup']['namespaceRemoved'] = False
        if role_created:
            try:
                sql(f'DROP OWNED BY {role}; DROP ROLE {role};'); report['cleanup']['readRoleRemoved'] = True
            except Exception: report['cleanup']['readRoleRemoved'] = False
        for name in ('key.pem', 'certificate.pfx'):
            (output / name).unlink(missing_ok=True)
        report['passed'] &= all(report['cleanup'].values())
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'errorType': report.get('errorType'), 'output': str(output)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
