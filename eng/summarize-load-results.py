#!/usr/bin/env python3
"""Summarize all repetitions, keeping incomplete/failed evidence explicit."""
import hashlib
import json
from pathlib import Path
import statistics
import sys

if len(sys.argv) < 3:
    raise SystemExit('Usage: python3 eng/summarize-load-results.py NEW_REPORT.md RESULT_DIRECTORY ...')
output = Path(sys.argv[1])
if output.exists():
    raise SystemExit('Choose a new report path; previous evidence is preserved.')
lines = ['# Local consumer backlog measurements', '', 'These are local functional load baselines, not supported capacity or database product rankings.', '']
manifest = []
for argument in sys.argv[2:]:
    directory = Path(argument)
    result_file = directory / 'results.json'
    environment_file = directory / 'environment.json'
    result = json.loads(result_file.read_text())
    environment = json.loads(environment_file.read_text())
    options = result['Options']
    provider = options['Provider']
    rows = result['Cases']
    lines += [f"## {provider} — {result['Environment']['Framework']}", '',
              f"Status: {'complete and valid' if result['Complete'] and result['Success'] else 'FAILED OR INCOMPLETE'}. "
              f"{len(rows)}/{options['Repetitions'] * len(options['Concurrency'])} cases recorded.", '',
              f"{options['Count']:,} records/case; {options['PayloadBytes']:,} bytes/record; "
              f"admission concurrency {options['AdmissionConcurrency']}; pool {options['PoolSize']}; "
              f"warm-up {options['WarmupCount']}; repetitions {options['Repetitions']}.", '',
              '| Workers | Valid / recorded | Completion/s median [min–max] | Preload/s median | Drain seconds median | Claim p95 ms¹ | Execute p95 ms¹ | Client peak MiB² |',
              '|---:|---:|---:|---:|---:|---:|---:|---:|']
    for workers in options['Concurrency']:
        all_cases = [c for c in rows if c['Workers'] == workers]
        valid = [c['Measured'] for c in all_cases if c['Valid']]
        if not valid:
            lines.append(f'| {workers} | 0 / {len(all_cases)} | — | — | — | — | — | — |')
            continue
        rates = [v['VerifiedCompletionsPerSecond'] for v in valid]
        median = lambda key: statistics.median(v[key] for v in valid)
        claim = statistics.median(v['NonemptyClaims']['P95UpperMs'] for v in valid)
        execute = statistics.median(v['ExecuteCalls']['P95UpperMs'] for v in valid)
        peak = max(v['ClientResources']['ClientSampledPeakWorkingSetBytes'] for v in valid) / (1024**2)
        lines.append(f'| {workers} | {len(valid)} / {len(all_cases)} | {statistics.median(rates):.1f} [{min(rates):.1f}–{max(rates):.1f}] | '
                     f'{median("AcceptedPerSecond"):.1f} | {median("DrainSeconds"):.2f} | {claim:.2f} | {execute:.2f} | {peak:.1f} |')
    effects = sum(c['Measured']['Oracle']['Effects'] for c in rows if c['Valid'])
    lines += ['', f'Validated measured effects: **{effects:,}**. Every valid case verified one effect per Completed work, matching fingerprints/ordinals, '
              'and no unexpected attempt count. Warm-up is excluded from this total.', '']
    for c in rows:
        if not c['Valid']:
            lines.append(f"Failed case: workers={c['Workers']}, repetition={c['Repetition']}, stage={c['Stage']}, "
                         f"error={c['ErrorType']}, retained schema={c['Schema']}.")
    lines += [f"Host: {environment['host'].get('machdep.cpu.brand_string', environment['host']['machine'])}; "
              f"host logical CPUs {environment['host']['logicalCpus']}; Docker CPUs {environment['dockerEngine']['NCPU']}; "
              f"Docker memory {environment['dockerEngine']['MemTotal'] / 1024**3:.2f} GiB.", '',
              'Raw evidence: `' + str(directory) + '/results.json` and `environment.json`.', '']
    manifest.append({'results': str(result_file), 'sha256': hashlib.sha256(result_file.read_bytes()).hexdigest(),
                     'environment': str(environment_file), 'environmentSha256': hashlib.sha256(environment_file.read_bytes()).hexdigest(),
                     'complete': result['Complete'], 'success': result['Success']})
lines += ['¹ Median of each run’s p95 upper histogram bucket, not a pooled p95. Quantile rounding is below 2.2%.', '',
          '² Maximum sampled harness-process working set across these repetitions; not DB/container memory and not incremental payload memory.', '',
          'Admission and drain are separate phases. No new arrivals occur during drain. The handler hashes the payload and writes one SQL row with normal durable commit. '
          'No broker, outbox, remote business service or failure injection is included. Counters include outcomes; this summary shows rates only for valid cases and keeps failed/incomplete evidence explicit.', '',
          'The shared desktop/Docker VM and background demo processes introduce noise. PostgreSQL runs arm64; SQL Server runs amd64 under ARM emulation with a different memory limit. '
          'Cross-database comparisons are not equivalent hardware results. CPU/GC/allocation and latency detail is available in the raw JSON.', '',
          'See the repository README, Load experiments section, for methodology and remaining sustained-load, million-record, hot-key, DB profiling, multi-process and broker matrix work.', '']
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text('\n'.join(lines))
output.with_suffix('.evidence.json').write_text(json.dumps(manifest, indent=2))
print(output)
