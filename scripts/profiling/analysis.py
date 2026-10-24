"""Keep sampled CPU cost and observed syscall latency as separate measurements."""
import csv
import gzip
import json
from pathlib import Path
import re


def cpu_summary(decoded):
    methods = {}
    root = {'name': 'all sampled stacks', 'cpu_ns': 0, 'samples': 0, 'children': {}}
    for sample in decoded['stacks']:
        ns, count, frames = sample['cpu_ns'], sample['samples'], sample['frames']
        root['cpu_ns'] += ns
        root['samples'] += count
        seen = set()
        for index, frame in enumerate(frames):
            key = (frame['name'], frame['file'])
            row = methods.setdefault(key, dict(name=key[0], file=key[1], line=frame['line'],
                self_cpu_ns=0, inclusive_cpu_ns=0, self_samples=0, inclusive_samples=0,
                invocation_count=None, mean_call_ns=None))
            if index == 0:
                row['self_cpu_ns'] += ns
                row['self_samples'] += count
            if key not in seen:
                row['inclusive_cpu_ns'] += ns
                row['inclusive_samples'] += count
                seen.add(key)
        current = root
        for frame in reversed(frames):
            key = (frame['name'], frame['file'], frame['line'])
            current = current['children'].setdefault(key, dict(frame, cpu_ns=0, samples=0, children={}))
            current['cpu_ns'] += ns
            current['samples'] += count
    def pack(node):
        node['children'] = [pack(child) for child in sorted(node['children'].values(), key=lambda x: (-x['cpu_ns'], x['name']))]
        return node
    return {'total_cpu_ns': root['cpu_ns'], 'total_samples': root['samples'],
            'duration_ns': decoded['duration_ns'], 'profile_time_ns': decoded['time_ns'],
            'sample_period': decoded['period'], 'tree': pack(root),
            'methods': sorted(methods.values(), key=lambda x: (-x['inclusive_cpu_ns'], x['name']))}


LINE = re.compile(r'^\s*(?:(?:\[pid\s+(\d+)\]|(\d+))\s+)?(\d+\.\d+)\s+(.*)$')
ENTRY = re.compile(r'(\w+)\(')
RESUMED = re.compile(r'<\.\.\. (\w+) resumed>')
DURATION = re.compile(r'<(\d+(?:\.\d+)?)>\s*$')
RETURN = re.compile(r'.*\)\s+=\s+(\S+)(.*)$')


def syscall_summary(path, events_path):
    """Join interleaved threads and retain every completed event in gzip JSONL."""
    pending, rows = {}, {}
    orphan_resumes = 0
    first, last = None, None
    path, events_path = Path(path), Path(events_path)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', errors='replace') as source, gzip.open(events_path, 'wt') as events:
        for raw in source:
            match = LINE.match(raw.rstrip())
            if not match:
                continue
            pid = match[1] or match[2] or 'main'
            timestamp, body = float(match[3]), match[4]
            entry, resumed = ENTRY.match(body), RESUMED.match(body)
            if entry:
                name, start = entry[1], timestamp
                row = rows.setdefault(name, {'name': name, 'calls': 0, 'completed': 0, 'timed_calls': 0,
                    'errors': 0, 'errnos': {}, 'total_elapsed_ns': 0, 'min_elapsed_ns': None,
                    'max_elapsed_ns': None, 'examples': []})
                row['calls'] += 1
                first = start if first is None else min(first, start)
                last = timestamp if last is None else max(last, timestamp)
                if body.endswith('<unfinished ...>'):
                    pending[pid] = (name, start, body.replace('<unfinished ...>', ''))
                    continue
            elif resumed:
                previous = pending.pop(pid, None)
                if previous is None or previous[0] != resumed[1]:
                    orphan_resumes += 1
                    continue
                name, start, original = previous
                body = original + body[resumed.end():]
                row = rows[name]
            else:
                continue
            first = start if first is None else min(first, start)
            last = timestamp if last is None else max(last, timestamp)
            # Truncated records and threads killed while blocked have no known return.
            returned = RETURN.fullmatch(body)
            if returned is None or returned[1] == '?':
                continue
            row['completed'] += 1
            duration = DURATION.search(body)
            ns = round(float(duration[1]) * 1e9) if duration else None
            if ns is not None:
                row['timed_calls'] += 1
                row['total_elapsed_ns'] += ns
                row['min_elapsed_ns'] = ns if row['min_elapsed_ns'] is None else min(row['min_elapsed_ns'], ns)
                row['max_elapsed_ns'] = ns if row['max_elapsed_ns'] is None else max(row['max_elapsed_ns'], ns)
            error = re.match(r'\s+(\w+)', returned[2]) if returned[1] == '-1' else None
            errno = error[1] if error else None
            if errno:
                row['errors'] += 1
                row['errnos'][errno] = row['errnos'].get(errno, 0) + 1
            text = body[:4096]
            if len(row['examples']) < 3 and text not in row['examples']:
                row['examples'].append(text)
            events.write(json.dumps({'pid': pid, 'syscall': name, 'start_unix': start,
                                     'elapsed_ns': ns, 'errno': errno, 'call': text}) + '\n')
    for row in rows.values():
        row['incomplete'] = row['calls'] - row['completed']
        row['mean_elapsed_ns'] = row['total_elapsed_ns'] / row['timed_calls'] if row['timed_calls'] else None
    return {'syscalls': sorted(rows.values(), key=lambda x: (-x['total_elapsed_ns'], x['name'])),
            'orphan_resumes': orphan_resumes, 'first_unix': first, 'last_unix': last,
            'observed_calls': sum(r['calls'] for r in rows.values()),
            'total_elapsed_ns': sum(r['total_elapsed_ns'] for r in rows.values())}


def write_csv(path, rows, fields):
    with Path(path).open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def write_results(directory, manifest, targets):
    directory = Path(directory)
    data = {'schema_version': 1, 'manifest': manifest, 'targets': targets,
            'semantics': {'method_time': 'sampled CPU nanoseconds, not invocation latency',
                          'method_calls': 'unavailable: samples are not invocation counts',
                          'syscall_time': 'traced elapsed nanoseconds including waits and tracing overhead',
                          'syscall_calls': 'observed entries within the trace window'}}
    (directory / 'summary.json').write_text(json.dumps(data, indent=2) + '\n')
    methods, syscalls = [], []
    for name, target in targets.items():
        methods.extend(dict(row, target=name) for row in target.get('cpu', {}).get('methods', []))
        syscalls.extend(dict(row, target=name) for row in target.get('trace', {}).get('syscalls', []))
    write_csv(directory / 'methods.csv', methods, ['target', 'name', 'file', 'line', 'self_cpu_ns',
        'inclusive_cpu_ns', 'self_samples', 'inclusive_samples', 'invocation_count', 'mean_call_ns'])
    write_csv(directory / 'syscalls.csv', syscalls, ['target', 'name', 'calls', 'completed', 'incomplete',
        'timed_calls', 'errors', 'total_elapsed_ns', 'mean_elapsed_ns', 'min_elapsed_ns', 'max_elapsed_ns'])
    lines = ['# Performance capture summary', '',
        'Method time is sampled CPU cost. Method invocation counts/per-call latency are unavailable.',
        'Syscall durations are traced elapsed time, including blocking and tracer overhead. They are not CPU time.',
        'Inclusive method totals overlap. Syscall durations may overlap across threads. Do not sum these two domains.', '',
        '| Target | CPU seconds | CPU samples | Observed syscall entries | Summed syscall elapsed seconds |',
        '| --- | ---: | ---: | ---: | ---: |']
    for name, target in targets.items():
        cpu, trace = target.get('cpu', {}), target.get('trace', {})
        cpu_seconds = f"{cpu['total_cpu_ns']/1e9:.6f}" if cpu else 'unavailable'
        trace_seconds = f"{trace['total_elapsed_ns']/1e9:.6f}" if trace else 'unavailable'
        lines.append(f"| {name} | {cpu_seconds} | {cpu.get('total_samples', 'unavailable')} | {trace.get('observed_calls', 'unavailable')} | {trace_seconds} |")
    lines.extend(['', 'Full per-method statistics: [methods.csv](methods.csv).',
                  'Full per-syscall statistics: [syscalls.csv](syscalls.csv).',
                  'Individual timed syscall events: `raw/*.events.jsonl.gz`.',
                  'Interactive heatmaps and CPU flame graph: [report.html](report.html).',
                  'Raw captures, binary hashes, build metadata and phase timing are retained in this directory.'])
    (directory / 'summary.md').write_text('\n'.join(lines) + '\n')
    template = (Path(__file__).parent / 'report.html').read_text()
    # Prevent profile names/source names from closing the embedded script element.
    payload = json.dumps(data, separators=(',', ':')).replace('<', '\\u003c').replace('&', '\\u0026')
    (directory / 'report.html').write_text(template.replace('/*PROFILE_DATA*/null', payload))
    return data
