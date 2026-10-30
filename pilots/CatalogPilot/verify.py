#!/usr/bin/env python3
"""Run the package-only local pilot against real PostgreSQL and RabbitMQ.
Owns only the processes it starts; retains isolated pilot data and evidence for inspection.
"""
import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
DOTNET = ROOT / '.tools/dotnet/dotnet'
APP = ROOT / 'pilots/CatalogPilot'

def run(framework):
    run_id = framework.replace('.', '').replace('net', '') + '_' + uuid.uuid4().hex[:10]
    output = ROOT / '.artifacts/catalog-pilot' / run_id
    output.mkdir(parents=True)
    schema = 'pilot_catalog_' + run_id
    port = 5194
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(('127.0.0.1', port))
    env = dict(os.environ, DOTNET_ROOT=str(DOTNET.parent), ASPNETCORE_ENVIRONMENT='Production',
               PILOT_SCHEMA=schema, PILOT_PORT=str(port), PILOT_TEST_CONTROLS='1')
    dll = APP / f'bin/Release/{framework}/CatalogPilot.dll'
    processes, handles, checks = [], [], []
    result = {'framework':framework, 'schema':schema, 'eventguardVersion':'0.1.0-dev.31',
              'database':'real local PostgreSQL 18.6', 'broker':'real local RabbitMQ 4.3.5', 'checks':checks}
    begin = time.monotonic()
    def record(name, evidence):
        checks.append({'scenario':name, 'passed':True, 'evidence':evidence})
        print(f'{framework}: PASS {name}', flush=True)
    def command(*args):
        p = subprocess.run([str(DOTNET), str(dll), *map(str,args)], cwd=APP, env=env,
                           capture_output=True, text=True, timeout=75)
        with (output/'commands.log').open('a') as log:
            log.write('COMMAND '+args[0]+'\n'+p.stdout+p.stderr)
        if p.returncode != 0:
            raise RuntimeError(f'Command {args[0]} exited {p.returncode}; see {output}/commands.log')
        if args[0] != 'init':
            return json.loads(p.stdout.strip().splitlines()[-1])
    def start(mode, **extra):
        log = (output/f'{mode}-{len(processes)}.log').open('w'); handles.append(log)
        process = subprocess.Popen([str(DOTNET),str(dll),mode], cwd=APP, env=dict(env,**extra),stdout=log,stderr=subprocess.STDOUT)
        processes.append(process); return process
    def terminate(process):
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(); raise RuntimeError('Graceful pilot shutdown exceeded 15 seconds.')
        if process.returncode != 0: raise RuntimeError(f'Pilot process {process.pid} exited {process.returncode}.')
    def http(path, body=None):
        data = None if body is None else json.dumps(body,separators=(',',':')).encode()
        request = urllib.request.Request(f'http://127.0.0.1:{port}'+path, data=data,
                    headers={'Content-Type':'application/json'} if data is not None else {})
        try:
            with urllib.request.urlopen(request,timeout=15) as response: return response.status,json.load(response)
        except urllib.error.HTTPError as error:
            return error.code,json.loads(error.read())
    def get(path):
        status,body = http(path); assert status==200,(path,status,body); return body
    def wait(fn, seconds=40):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            try:
                value=fn()
                if value: return value
            except (urllib.error.URLError,ConnectionError): pass
            time.sleep(.075)
        raise TimeoutError('Pilot condition timed out; see process logs in '+str(output))
    def healthy():
        health=get('/health')
        return health['status']=='ready' and health['schema']==schema
    def product(name, revision=1, category='general', event_id=None, title='Pilot product'):
        return {'eventId':event_id or str(uuid.uuid4()),'contractVersion':1,'productId':name,
                'revision':revision,'name':title,'category':category}
    def submit(value):
        status,body=http('/products',value); assert status==202,(status,body); return body
    def event(value): return get('/events/'+value['eventId'])
    def in_state(value, state):
        data=event(value)
        return data if data['work'] and data['work']['state']==state else None
    def completed(value):
        def read():
            data=event(value)
            return data if data['work'] and data['work']['state']=='Completed' and data['completionPublished']==1 else None
        return wait(read)
    def input_file(value, name=None):
        path=output/(name or (str(uuid.uuid4())+'.json'))
        path.write_text(json.dumps(value,separators=(',',':')),encoding='utf-8'); return path
    def ack_count():
        request=urllib.request.Request('http://127.0.0.1:15673/api/queues/eventguard_test/'+schema+'.changes',
            headers={'Authorization':'Basic '+base64.b64encode(b'eventguard:eventguard-local-only').decode()})
        with urllib.request.urlopen(request,timeout=5) as response: data=json.load(response)
        return data.get('message_stats',{}).get('ack',0)
    def inject(value, count=1):
        previous=ack_count(); path=input_file(value); command('inject',path,count)
        wait(lambda: ack_count()>=previous+count)
        return path
    def start_workers(): return [start('worker'),start('worker')]
    try:
        command('init')
        api=start('api'); wait(healthy)
        first=product('normal')
        assert submit(first)['status']=='Committed'
        assert event(first)['effects']==0 and get('/summary')['outbox']==1
        terminate(api); api=start('api'); wait(healthy)
        assert submit(first)['status']=='AlreadyCommitted'
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            repeats=list(pool.map(submit,[first]*20))
        assert all(x['status']=='AlreadyCommitted' for x in repeats)
        assert http('/products',dict(first,name='Conflicting replacement'))[0]==409
        assert event(first)['sourceWrites']==1
        record('Producer request survives API restart and 20 concurrent repeats',event(first))
        workers=start_workers(); data=completed(first)
        assert data['effects']==data['sourceWrites']==1
        record('Transactional HTTP to outbox to RabbitMQ to worker to completion outbox',data)

        inject(first,20); data=event(first)
        assert data['effects']==1 and data['conflicts']==0
        record('20 real broker duplicates have one business effect',data)
        inject(dict(first,name='Changed same identity')); data=event(first)
        assert data['conflicts']==1 and data['effects']==1 and get('/products/normal')['name']=='Pilot product'
        record('Same key with changed content preserves first input',data)

        newer=product('ordered',18,title='Current catalog entry'); submit(newer); completed(newer)
        older=product('ordered',17,title='Stale catalog entry'); submit(older); completed(older)
        assert get('/products/ordered')['revision']==18
        record('Revision 17 cannot regress revision 18',get('/products/ordered'))
        v2=product('version-two'); v2['contractVersion']=2; v2['displayName']=v2.pop('name')
        inject(v2); record('Explicit version 2 mapping',completed(v2))

        future=product('quarantine'); future['contractVersion']=99
        raw=inject(future); held=get('/quarantine')[0]; assert held['reason']=='unsupported-contract-version'
        proof=command('raw-proof',held['id']); assert proof['bodyHash']==hashlib.sha256(raw.read_bytes()).hexdigest().upper()
        following=product('following-poison'); submit(following); completed(following)
        record('Unsupported input is durable while following work continues',held)
        fixed=dict(future,contractVersion=1); fixed_file=input_file(fixed); request=str(uuid.uuid4())
        released=command('release',held['id'],request,fixed_file,held['version']); assert released['status']=='Accepted'
        repeated=command('release',held['id'],request,fixed_file,held['version']); assert repeated['repeatedRequest']
        assert completed(fixed)['effects']==1 and command('raw-proof',held['id'])==proof
        record('Local operator release is idempotent and keeps raw evidence',{'receipt':released,'repeat':repeated,'rawUnchanged':True})
        inject({'contractVersion':'invalid'})
        malformed=output/'malformed.json'; malformed.write_text('{')
        before=ack_count(); command('inject',malformed); wait(lambda:ack_count()>before)
        assert {q['reason'] for q in get('/quarantine') if not q['released']}=={'invalid-json','invalid-contract-version'}
        record('Malformed JSON and invalid version are held without killing ingress',get('/quarantine'))

        failed=product('needs-category',category='books'); submit(failed)
        failure=wait(lambda:in_state(failed,'DeadLetter'))
        assert failure['effects']==failure['completionIntents']==0
        command('category','books'); request=str(uuid.uuid4()); version=failure['work']['version']
        replay=command('replay',failed['eventId'],request,version); assert replay['status']=='Accepted'
        repeated=command('replay',failed['eventId'],request,version); assert repeated['repeatedRequest']
        recovered=completed(failed); assert recovered['effects']==1 and recovered['work']['replayGeneration']==1
        refusal=command('replay',failed['eventId'],str(uuid.uuid4()),recovered['work']['version']); assert refusal['status']=='NotFailed'
        record('Business configuration repair and durable replay; Completed stays closed',{'recovered':recovered,'replay':replay,'repeat':repeated,'completedRefusal':refusal})

        baseline=product('locked-product'); submit(baseline); completed(baseline)
        ready,release=output/'lock-ready',output/'lock-release'
        log=(output/'native-lock.log').open('w'); handles.append(log)
        locker=subprocess.Popen([str(DOTNET),str(dll),'lock','locked-product',str(ready),str(release)],cwd=APP,env=env,stdout=log,stderr=subprocess.STDOUT); processes.append(locker)
        wait(ready.exists)
        blocked=product('locked-product',2); submit(blocked)
        retry=wait(lambda:in_state(blocked,'RetryScheduled'))
        assert retry['effects']==retry['completionIntents']==0
        release.touch(); locker.wait(timeout=10); assert locker.returncode==0
        retried=completed(blocked); assert retried['effects']==1 and retried['work']['attemptCount']>=2
        record('Real PostgreSQL row-lock timeout rolls back then retries',{'whileLocked':retry,'afterRelease':retried})

        for worker in workers: terminate(worker)
        crash=product('crash-product'); marker=output/'before-commit'
        dying=start('worker',PILOT_HOLD_EVENT=crash['eventId'],PILOT_HOLD_FILE=str(marker))
        submit(crash); wait(marker.exists)
        uncommitted=event(crash); assert uncommitted['effects']==uncommitted['completionIntents']==0
        dying.kill(); dying.wait(timeout=10); assert dying.returncode==-signal.SIGKILL
        assert event(crash)['effects']==0
        workers=start_workers(); recovered=completed(crash)
        assert recovered['effects']==recovered['completionIntents']==1 and recovered['work']['attemptCount']>=2
        record('SIGKILL after business SQL and outbox insertion rolls back and recovers once',{'killedPid':dying.pid,'uncommitted':uncommitted,'recovered':recovered})

        for worker in workers: terminate(worker)
        before=get('/summary'); batch=[product('backlog-'+str(i)) for i in range(1000)]
        ids=input_file([v['eventId'] for v in batch],'backlog-event-ids.json')
        queued_at=time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(submit,batch))
        enqueue_seconds=time.monotonic()-queued_at
        queued=get('/summary'); assert queued['effects']==before['effects'] and queued['outbox']==before['outbox']+1000
        started=time.monotonic(); workers=start_workers()
        wait(lambda:(s:=get('/summary'))['completed']==before['completed']+1000 and s['outbox']==s['published'],120)
        drain_seconds=time.monotonic()-started
        batch_proof=command('verify-batch',ids)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            assert all(r['status']=='AlreadyCommitted' for r in pool.map(submit,batch[:20]))
        final=get('/summary'); assert final['duplicateEffects']==0 and final['works']==final['completed']==final['effects']
        record('1,000 queued records drain across two worker processes',{'batch':batch_proof,'enqueueSeconds':round(enqueue_seconds,3),'drainSeconds':round(drain_seconds,3),'notCapacityBenchmark':True,'final':final})
        for worker in workers: terminate(worker)
        terminate(api)
        result.update(passed=True,totalChecks=len(checks),elapsedSeconds=round(time.monotonic()-begin,3),final=final)
    except Exception as error:
        result.update(passed=False,error=type(error).__name__+': '+str(error),elapsedSeconds=round(time.monotonic()-begin,3))
        raise
    finally:
        for p in reversed(processes):
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=15)
                except subprocess.TimeoutExpired:p.kill();p.wait()
        for handle in handles:handle.close()
        (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print('Evidence: '+str(output/'result.json'),flush=True)
    return output/'result.json'

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--framework',choices=['net8.0','net10.0','both'],default='both'); args=parser.parse_args()
    for framework in (['net8.0','net10.0'] if args.framework=='both' else [args.framework]):run(framework)
