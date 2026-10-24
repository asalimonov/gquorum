"""Capture profiles with explicit resource ownership and reproducible artifacts."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request
import uuid

from .analysis import cpu_summary, syscall_summary, write_results

ROOT = Path(__file__).resolve().parents[2]


def utc():
    return datetime.now(timezone.utc).isoformat()


def execute(args, *, log=None, timeout=180, env=None, cwd=ROOT):
    try:
        result = subprocess.run([str(a) for a in args], cwd=cwd, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    except subprocess.TimeoutExpired as err:
        if log:
            def output(value):
                return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
            with Path(log).open('a') as f:
                f.write(json.dumps({'time': utc(), 'argv': [str(a) for a in args],
                    'exit_code': None, 'error': str(err), 'stdout': output(err.stdout),
                    'stderr': output(err.stderr)}) + '\n')
        raise
    if log:
        with Path(log).open('a') as f:
            f.write(json.dumps({'time': utc(), 'argv': [str(a) for a in args],
                                'exit_code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}) + '\n')
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(map(str, args))}\n{result.stderr[-4000:]}")
    return result.stdout + result.stderr if len(args) > 1 and str(args[0]) == 'docker' and str(args[1]) == 'logs' else result.stdout


def new_run(output, command):
    if output is None:
        output = ROOT / 'test-reports' / 'performance' / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])
    directory = Path(output).resolve()
    if directory.exists() and any(directory.iterdir()):
        raise ValueError(f'output directory must be empty: {directory}')
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'raw').mkdir()
    manifest = {'schema_version': 1, 'started_utc': utc(), 'command': command, 'status': 'capturing',
                'phases': {}, 'errors': [], 'files': {}}
    for key, args in [('commit', ['git', 'rev-parse', 'HEAD']), ('working_tree', ['git', 'status', '--short']),
                      ('go_version', ['go', 'version'])]:
        try:
            manifest[key] = execute(args).strip()
        except (OSError, RuntimeError) as err:
            manifest[key] = str(err)
    try:
        (directory / 'source.patch').write_text(execute(['git', 'diff', 'HEAD', '--binary']))
    except (OSError, RuntimeError):
        pass
    save_manifest(directory, manifest)
    return directory, manifest


def save_manifest(directory, manifest):
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


def finish(directory, manifest, targets, error=None):
    manifest['finished_utc'] = utc()
    if error:
        manifest['errors'].append(str(error))
    manifest['status'] = 'failed' if manifest['errors'] else 'complete'
    if targets:
        write_results(directory, manifest, targets)
    for path in sorted(directory.rglob('*')):
        if not path.is_file() or path.name in {'manifest.json', 'summary.json', 'report.html'}:
            continue
        h = hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                h.update(chunk)
        manifest['files'][str(path.relative_to(directory))] = {'bytes': path.stat().st_size, 'sha256': h.hexdigest()}
    save_manifest(directory, manifest)
    if targets:
        write_results(directory, manifest, targets)


def decoder(directory):
    tool = directory / 'profiledecode'
    execute(['go', 'build', '-o', tool, '.'], cwd=ROOT / 'scripts/profiledecode', log=directory / 'commands.jsonl')
    return tool


def analyze_cpu(path, tool):
    return cpu_summary(json.loads(execute([tool, path])))


def fetch(url, path, timeout):
    temporary = path.with_suffix(path.suffix + '.part')
    with urllib.request.urlopen(url, timeout=timeout) as response, temporary.open('wb') as f:
        shutil.copyfileobj(response, f)
    temporary.replace(path)


def capture_endpoints(directory, manifest, endpoints, seconds, tool, targets):
    manifest['phases']['cpu'] = {'start_utc': utc(), 'seconds_requested': seconds, 'endpoints': endpoints}
    save_manifest(directory, manifest)
    def collect(name, base):
        path = directory / 'raw' / (name + '.cpu.pprof')
        fetch(base.rstrip('/') + f'/debug/pprof/profile?seconds={seconds}', path, seconds + 20)
        return name, path
    # Requests overlap across processes; each process has one CPU profiler.
    with ThreadPoolExecutor(max_workers=min(16, len(endpoints))) as pool:
        futures = [pool.submit(collect, name, base) for name, base in endpoints.items()]
        errors = []
        for future in as_completed(futures):
            try:
                name, path = future.result()
                targets.setdefault(name, {})['cpu'] = analyze_cpu(path, tool)
            except Exception as err:
                errors.append(str(err))
        if errors:
            raise RuntimeError('; '.join(errors))
    manifest['phases']['cpu']['end_utc'] = utc()
    # Heap and goroutine profiles are retained for pprof investigations, not mislabeled as CPU.
    for name, base in endpoints.items():
        for kind in ('heap', 'goroutine'):
            try:
                fetch(base.rstrip('/') + '/debug/pprof/' + kind,
                      directory / 'raw' / f'{name}.{kind}.pprof', 15)
            except Exception as err:
                manifest.setdefault('warnings', []).append(f'{name} {kind}: {err}')


def import_trace(name, path, directory, targets):
    destination = directory / 'raw' / (name + '.strace.gz')
    source = Path(path).resolve()
    if source != destination.resolve():
        if source.suffix == '.gz':
            shutil.copyfile(source, destination)
        else:
            with source.open('rb') as f, gzip.open(destination, 'wb') as out:
                shutil.copyfileobj(f, out)
    result = syscall_summary(destination, directory / 'raw' / (name + '.events.jsonl.gz'))
    if not result['observed_calls']:
        raise ValueError(f'{name}: no syscall entries found; use strace -f -ttt -T')
    targets.setdefault(name, {})['trace'] = result


def copy_binaries(binaries, directory):
    if not binaries:
        return
    dest = directory / 'binaries'
    dest.mkdir(exist_ok=True)
    for name, source in binaries.items():
        shutil.copyfile(source, dest / name)
        (dest / (name + '.build.txt')).write_text(execute(['go', 'version', '-m', dest / name]))


def cache_config(original, image, args):
    """Copy resolved Compose settings while isolating resources and retaining node env."""
    config = copy.deepcopy(original)
    for name in ('node1', 'node2', 'node3'):
        service = config['services'][name]
        service.update({'image': image, 'ports': ['127.0.0.1::6060']})
        service.pop('build', None)
        service.pop('secrets', None)
        service['environment'].update({'CACHE_PPROF_LISTEN': ':6060',
            'GQUORUM_CREDENTIAL_FILE': '/cache-profile-credential',
            'GOMAXPROCS': str(args.gomaxprocs), 'STORAGE_DISABLE_FSYNC': 'false' if args.fsync else 'true'})
    config['services']['loadgen'].update({'image': image, 'scale': 1})
    config['services']['loadgen']['environment'].update({'CACHE_WORKERS': str(args.workers),
        'CACHE_KEYS': str(args.keys), 'CACHE_DURATION': '0s', 'GOMAXPROCS': str(args.gomaxprocs)})
    for category in ('networks', 'volumes', 'secrets'):
        for definition in config.get(category, {}).values():
            definition.pop('name', None)
    config.pop('name', None)
    config.pop('secrets', None)
    for key in list(config):
        if key.startswith('x-'):
            del config[key]
    return config


def run_cache(args, directory, manifest, tool, targets):
    """Own an isolated Compose project; keep the user's default project untouched."""
    token = uuid.uuid4().hex[:10]
    project, image = 'gq-profile-' + token, 'gquorum-cache:profile-' + token
    tracer = 'gquorum-strace:profile-' + token
    log = directory / 'commands.jsonl'
    nodes = ['node1', 'node2', 'node3']
    overlay = directory / 'compose.json'
    original = json.loads(execute(['docker', 'compose', '-f', 'examples/inmemory-cache/compose.yaml', 'config', '--format', 'json']))
    # Retain and transfer the public demo credential through the Docker API;
    # this does not depend on host file sharing or a Compose secrets definition.
    credential = directory / 'demo-credential.txt'
    shutil.copyfile(ROOT / 'examples/inmemory-cache/demo-credential.txt', credential)
    credential.chmod(0o444)
    original = cache_config(original, image, args)
    overlay.write_text(json.dumps(original, indent=2))
    base = ['docker', 'compose', '-p', project, '-f', overlay]
    env = dict(os.environ)
    env.pop('COMPOSE_PROJECT_NAME', None)
    containers = {}
    manifest['configuration'] = {'project': project, 'workers': args.workers, 'keys': args.keys,
        'gomaxprocs': args.gomaxprocs, 'fsync': args.fsync, 'seconds': args.seconds,
        'warmup_seconds': args.warmup, 'syscall_seconds': args.syscall_seconds if args.syscalls else None}
    try:
        execute(['docker', 'build', '-f', 'examples/inmemory-cache/Dockerfile', '-t', image, '.'], log=log, timeout=600)
        manifest['image'] = json.loads(execute(['docker', 'image', 'inspect', image]))[0]['Id']
        manifest['docker'] = json.loads(execute(['docker', 'version', '--format', '{{json .Server}}']))
        execute(base + ['create'], env=env, log=log, timeout=150)
        for node in nodes:
            node_id = execute(base + ['ps', '--all', '-q', node], env=env).strip()
            if not node_id:
                raise RuntimeError(f'{node}: no created container to receive the demo credential')
            execute(['docker', 'cp', credential, node_id + ':/cache-profile-credential'], log=log)
        execute(base + ['up', '-d', '--wait', '--wait-timeout', '120'], env=env, log=log, timeout=150)
        endpoints = {}
        for node in nodes:
            address = execute(base + ['port', node, '6060'], env=env).strip()
            endpoints[node] = 'http://' + address
        (directory / 'binaries').mkdir()
        node_id = execute(base + ['ps', '-q', 'node1'], env=env).strip()
        for binary in ('cache-server', 'cache-loadgen'):
            dest = directory / 'binaries' / binary
            execute(['docker', 'cp', node_id + ':/' + binary, dest], log=log)
            (directory / 'binaries' / (binary + '.build.txt')).write_text(execute(['go', 'version', '-m', dest]))
        manifest['phases']['warmup'] = {'start_utc': utc(), 'seconds': args.warmup}
        save_manifest(directory, manifest)
        time.sleep(args.warmup)
        capture_endpoints(directory, manifest, endpoints, args.seconds, tool, targets)
        if args.syscalls:
            # Separate pass: strace must never run during the CPU capture above.
            execute(['docker', 'build', '-f', 'scripts/profiling/Dockerfile.strace', '-t', tracer, '.'], log=log, timeout=180)
            manifest['tracer_version'] = execute(['docker', 'run', '--rm', tracer, '--version'])
            manifest['phases']['syscalls'] = {'start_utc': utc(), 'seconds_requested': args.syscall_seconds}
            save_manifest(directory, manifest)
            for node in nodes:
                target = execute(base + ['ps', '-q', node], env=env).strip()
                name = project + '-trace-' + node
                containers[name] = node
                execute(['docker', 'run', '-d', '--name', name, '--pid=container:' + target,
                    '--cap-add=SYS_PTRACE', '--security-opt=seccomp:unconfined',
                    '--entrypoint', '/bin/sh', tracer, '-c',
                    f'timeout -s INT {args.syscall_seconds} strace -I 2 -f -ttt -T -yy -s 80 -o /tmp/{node}.strace -p 1'], log=log)
            with ThreadPoolExecutor(max_workers=3) as pool:
                waits = {pool.submit(execute, ['docker', 'wait', name], timeout=args.syscall_seconds + 30): name for name in containers}
                for future in as_completed(waits):
                    name = waits[future]
                    status = future.result().strip()
                    (directory / (name + '.log')).write_text(execute(['docker', 'logs', name]))
                    if status not in {'0', '124', '130'}:
                        raise RuntimeError(f'{name}: tracer exit {status}; see saved log')
            manifest['phases']['syscalls']['end_utc'] = utc()
            for node in nodes:
                trace = directory / 'raw' / (node + '.strace')
                execute(['docker', 'cp', project + '-trace-' + node + ':/tmp/' + node + '.strace', trace], log=log)
                import_trace(node, trace, directory, targets)
                trace.unlink()
        # Capture logs while containers still exist; loader timing is independent of profile timing.
        (directory / 'workload.log').write_text(execute(base + ['logs', '--no-color', 'loadgen'], env=env))
    finally:
        for name, node in containers.items():
            trace = directory / 'raw' / (node + '.strace')
            if not trace.exists() and not trace.with_suffix('.strace.gz').exists():
                try:
                    execute(['docker', 'cp', name + ':/tmp/' + node + '.strace', trace], log=log)
                except Exception as err:
                    manifest.setdefault('warnings', []).append(f'collect partial trace {name}: {err}')
            try:
                execute(['docker', 'rm', '-f', name], log=log)
            except Exception as err:
                manifest['errors'].append(f'tracer cleanup {name}: {err}')
        try:
            (directory / 'nodes.log').write_text(execute(base + ['logs', '--no-color'], env=env))
        except Exception as err:
            manifest.setdefault('warnings', []).append(f'collect node logs: {err}')
        try:
            execute(base + ['down', '--volumes', '--remove-orphans'], env=env, log=log, timeout=90)
        except Exception as err:
            manifest['errors'].append(f'Compose cleanup {project}: {err}')
        for tag in (image, tracer) if args.syscalls else (image,):
            result = subprocess.run(['docker', 'image', 'rm', tag], capture_output=True, text=True)
            if result.returncode and 'No such image' not in result.stderr:
                manifest['errors'].append(f'image cleanup {tag}: {result.stderr}')
