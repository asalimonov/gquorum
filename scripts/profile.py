#!/usr/bin/env python3
"""Capture a gquorum build or analyze saved pprof/strace evidence."""
import argparse
from pathlib import Path
import re
import shutil
import sys
from urllib.parse import urlparse

from profiling.capture import (new_run, finish, decoder, analyze_cpu, capture_endpoints,
                               copy_binaries, import_trace, run_cache)


def assignments(values, *, urls=False):
    result = {}
    for value in values:
        name, separator, target = value.partition('=')
        if not separator or not target or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', name):
            raise ValueError('use NAME=VALUE with a safe alphanumeric name')
        if name in result:
            raise ValueError(f'duplicate target: {name}')
        if urls:
            parsed = urlparse(target)
            if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
                raise ValueError('pprof base URLs must be http(s), without credentials/query/fragment')
        result[name] = target
    return result


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    for name, help_text in [('capture', 'capture existing pprof HTTP endpoints'),
                            ('analyze', 'import saved CPU profiles and/or strace logs'),
                            ('cache', 'build and profile an isolated three-node cache example')]:
        command = sub.add_parser(name, help=help_text)
        command.add_argument('--output', type=Path, help='empty run directory; default test-reports/performance/<timestamp-id>')
        if name == 'capture':
            command.add_argument('--target', action='append', default=[], required=True, metavar='NAME=URL')
        if name != 'cache':
            command.add_argument('--binary', action='append', default=[], metavar='NAME=PATH', help='retain the exact application binary')
        if name in ('cache', 'capture'):
            command.add_argument('--seconds', type=int, default=30)
        if name == 'analyze':
            command.add_argument('--profile', action='append', default=[], metavar='NAME=PATH')
            command.add_argument('--strace', action='append', default=[], metavar='NAME=PATH')
        if name == 'cache':
            command.add_argument('--workers', type=int, default=1)
            command.add_argument('--keys', type=int, default=615)
            command.add_argument('--gomaxprocs', type=int, default=1)
            command.add_argument('--warmup', type=int, default=5)
            command.add_argument('--fsync', action='store_true', help='enable load-group disk synchronization')
            command.add_argument('--syscalls', action=argparse.BooleanOptionalAction, default=True, help='trace syscalls in a separate pass (default enabled; Linux Docker)')
            command.add_argument('--syscall-seconds', type=int, default=10)
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    directory, manifest, targets = None, None, {}
    try:
        if hasattr(args, 'seconds') and not 1 <= args.seconds <= 3600:
            raise ValueError('seconds must be 1..3600')
        if args.command == 'cache' and (not 1 <= args.workers <= 256 or args.keys < args.workers or
                args.gomaxprocs < 1 or not 0 <= args.warmup <= 300 or not 1 <= args.syscall_seconds <= 300):
            raise ValueError('need workers=1..256, keys>=workers, gomaxprocs>=1, warmup=0..300, syscall-seconds=1..300')
        profiles = assignments(getattr(args, 'profile', []))
        traces = assignments(getattr(args, 'strace', []))
        endpoints = assignments(getattr(args, 'target', []), urls=True)
        binaries = assignments(getattr(args, 'binary', []))
        if args.command == 'analyze' and not profiles and not traces:
            raise ValueError('analyze needs at least one --profile or --strace')
        for value in list(profiles.values()) + list(traces.values()) + list(binaries.values()):
            if not Path(value).is_file():
                raise ValueError(f'input file not found: {value}')
        directory, manifest = new_run(args.output, vars(args)['command'])
        print('Capture directory:', directory, flush=True)
        tool = decoder(directory) if profiles or args.command != 'analyze' else None
        copy_binaries(binaries, directory)
        if args.command == 'cache':
            run_cache(args, directory, manifest, tool, targets)
        elif args.command == 'capture':
            capture_endpoints(directory, manifest, endpoints, args.seconds, tool, targets)
        else:
            for name, source in profiles.items():
                path = directory / 'raw' / (name + '.cpu.pprof')
                shutil.copyfile(source, path)
                targets.setdefault(name, {})['cpu'] = analyze_cpu(path, tool)
            for name, source in traces.items():
                import_trace(name, source, directory, targets)
        finish(directory, manifest, targets)
        if manifest['errors']:
            raise RuntimeError('; '.join(manifest['errors']))
        print('Report:', directory / 'report.html', flush=True)
        return 0
    except (Exception, KeyboardInterrupt) as err:
        if directory is not None:
            finish(directory, manifest, targets, error=err or 'interrupted')
            print('Partial evidence retained:', directory, file=sys.stderr)
        print(f'profile: {err or "interrupted"}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
