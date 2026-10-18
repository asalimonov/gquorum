"""Regression tests for measurements, partial records and offline report escaping."""
import gzip
import json
from pathlib import Path
import tempfile
import unittest
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

from profiling.analysis import cpu_summary, syscall_summary, write_results
from profile import assignments
from profiling.capture import cache_config, capture_endpoints, execute, finish, new_run, run_cache


class AnalysisTests(unittest.TestCase):
    def test_recursive_cpu_inclusive_counted_once(self):
        frame = lambda name: {'name': name, 'file': 'x.go', 'line': 7}
        result = cpu_summary({'duration_ns': 100, 'time_ns': 1, 'period': 10,
            'stacks': [{'cpu_ns': 30, 'samples': 3, 'frames': [frame('recur'), frame('recur'), frame('root')]},
                       {'cpu_ns': 20, 'samples': 2, 'frames': [frame('leaf'), frame('root')]}]})
        rows = {r['name']: r for r in result['methods']}
        self.assertEqual(rows['recur']['inclusive_cpu_ns'], 30)
        self.assertEqual(rows['root']['inclusive_cpu_ns'], 50)
        self.assertEqual(sum(r['self_cpu_ns'] for r in rows.values()), 50)
        self.assertIsNone(rows['recur']['invocation_count'])
        self.assertEqual(result['tree']['cpu_ns'], 50)
        self.assertEqual(result['total_samples'], 5)

    def test_interleaved_syscalls_missing_duration_and_unfinished(self):
        trace = '''10 100.000001 read(4, <unfinished ...>
11 100.000002 futex(0x1, FUTEX_WAIT_PRIVATE, 0, NULL <unfinished ...>
10 100.000003 <... read resumed> "abc", 3) = 3 <0.000002>
11 100.000005 <... futex resumed>) = -1 EAGAIN (changed) <0.000003>
10 100.000006 write(4, "abc", 3) = 3
10 100.000007 read(4, <unfinished ...>
11 100.000008 <... read resumed> "", 1) = 0 <0.000001>
10 100.000009 <... read resumed>) = ?
'''
        with tempfile.TemporaryDirectory() as tmp:
            path, events = Path(tmp)/'trace', Path(tmp)/'events.gz'
            path.write_text(trace)
            result = syscall_summary(path, events)
            rows = {r['name']: r for r in result['syscalls']}
            self.assertEqual(rows['read']['calls'], 2)
            self.assertEqual(rows['read']['completed'], 1)
            self.assertEqual(rows['read']['total_elapsed_ns'], 2000)
            self.assertEqual(rows['read']['incomplete'], 1)
            self.assertEqual(rows['futex']['mean_elapsed_ns'], 3000)
            self.assertEqual(rows['futex']['errors'], 1)
            self.assertEqual(rows['write']['timed_calls'], 0)
            self.assertIsNone(rows['write']['mean_elapsed_ns'])
            self.assertEqual(result['orphan_resumes'], 1)
            with gzip.open(events, 'rt') as f:
                records = [json.loads(x) for x in f]
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0]['pid'], '10')

    def test_gzip_and_bracketed_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'trace.gz'
            with gzip.open(path, 'wt') as f:
                f.write('[pid 12] 100.123456 close(4) = 0 <0.000010>\n')
            result = syscall_summary(path, Path(tmp)/'events.gz')
            self.assertEqual(result['observed_calls'], 1)
            self.assertEqual(result['syscalls'][0]['max_elapsed_ns'], 10000)

    def test_report_escape_and_no_fake_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            cpu = cpu_summary({'duration_ns': 1, 'time_ns': 1, 'period': 1,
                'stacks': [{'cpu_ns': 10, 'samples': 1, 'frames': [{'name': '</script><script>alert(1)', 'file': 'x', 'line': 1}]}]})
            write_results(tmp, {'status': 'complete'}, {'node1': {'cpu': cpu}})
            html = (Path(tmp)/'report.html').read_text()
            self.assertNotIn('</script><script>alert(1)', html)
            self.assertIn('\\u003c/script>', html)
            self.assertIn('invocation_count', (Path(tmp)/'methods.csv').read_text())

    def test_cache_config_preserves_node_env_and_isolates_resources(self):
        config = {'name': 'user-project', 'services': {}, 'networks': {'cache': {'name': 'user-network'}},
                  'secrets': {'credential': {'file': '/unshared/worktree/demo-credential.txt'}},
                  'x-node': {'secrets': ['credential']},
                  'volumes': {'data': {'name': 'user-data'}}}
        for name in ('node1', 'node2', 'node3', 'loadgen'):
            config['services'][name] = {'environment': {'GQUORUM_ALLOW_PLAINTEXT': 'true', 'GQUORUM_NODE': name},
                                        'ports': ['16381:6379'], 'build': {'context': '.'}, 'secrets': ['credential']}
        options = SimpleNamespace(workers=10, keys=615, gomaxprocs=1, fsync=True)
        result = cache_config(config, 'owned:image', options)
        self.assertEqual(result['services']['node1']['environment']['GQUORUM_ALLOW_PLAINTEXT'], 'true')
        self.assertEqual(result['services']['node1']['environment']['STORAGE_DISABLE_FSYNC'], 'false')
        self.assertEqual(result['services']['node1']['ports'], ['127.0.0.1::6060'])
        self.assertNotIn('build', result['services']['node1'])
        self.assertNotIn('secrets', result['services']['node1'])
        self.assertNotIn('secrets', result)
        self.assertNotIn('x-node', result)
        self.assertEqual(result['services']['node1']['environment']['GQUORUM_CREDENTIAL_FILE'], '/cache-profile-credential')
        self.assertNotIn('name', result['volumes']['data'])
        self.assertNotIn('name', result)
        self.assertEqual(config['volumes']['data']['name'], 'user-data')

    def test_assignment_validation(self):
        self.assertEqual(assignments(['node1=http://localhost:6060'], urls=True), {'node1': 'http://localhost:6060'})
        for values in [['../escape=x'], ['a=x', 'a=y'], ['missing']]:
            with self.assertRaises(ValueError): assignments(values)
        for url in ['http://u:p@localhost', 'http://@localhost', 'http://localhost?q=1', 'http://localhost/#fragment']:
            with self.assertRaises(ValueError): assignments(['x='+url], urls=True)

    def test_truncated_returns_and_argument_text_are_not_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, events = Path(tmp)/'trace', Path(tmp)/'events.gz'
            path.write_text('10 100.000001 read(4, <unfinished ...>\n'
                            '11 100.000002 write(4, "partial"\n'
                            '12 100.000003 write(4, "close(1) = -1 EIO", 17) = 17 <0.000001>\n')
            result = syscall_summary(path, events)
            rows = {r['name']: r for r in result['syscalls']}
            self.assertEqual(result['first_unix'], 100.000001)
            self.assertEqual(rows['read']['incomplete'], 1)
            self.assertEqual(rows['write']['incomplete'], 1)
            self.assertEqual(rows['write']['completed'], 1)
            self.assertEqual(rows['write']['errors'], 0)

    def test_missing_domain_is_unavailable_in_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_results(tmp, {'status': 'complete'}, {'node1': {'trace': {
                'observed_calls': 1, 'total_elapsed_ns': 10, 'syscalls': []}}})
            self.assertIn('| node1 | unavailable | unavailable | 1 |',
                          (Path(tmp)/'summary.md').read_text())


class CaptureTests(unittest.TestCase):
    def test_failed_tracer_copies_partial_evidence_before_removal_without_bind_mounts(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory/'raw').mkdir()
            credential = directory/'source-credential'
            credential.write_text('public-demo-credential')
            config = {'secrets': {'credential': {'file': str(credential)}},
                      'services': {n: {'environment': {}} for n in ('node1', 'node2', 'node3', 'loadgen')}}
            options = SimpleNamespace(workers=1, keys=615, gomaxprocs=1, fsync=False,
                                      seconds=1, warmup=0, syscalls=True, syscall_seconds=1)
            commands = []
            def command(args, **kwargs):
                args = list(map(str, args))
                commands.append(args)
                if 'config' in args: return json.dumps(config)
                if 'inspect' in args: return '[{"Id":"image-id"}]'
                if 'version' in args: return '{}'
                if 'ps' in args: return 'created-' + args[-1]
                if 'port' in args: return '127.0.0.1:12345'
                if args[:2] == ['docker', 'wait']: return '1'
                if args[:2] == ['docker', 'cp'] and ':' in args[2]:
                    Path(args[3]).write_bytes(b'partial capture')
                return ''
            manifest = {'errors': [], 'phases': {}}
            with patch('profiling.capture.execute', side_effect=command), \
                 patch('profiling.capture.capture_endpoints'), \
                 patch('profiling.capture.subprocess.run', return_value=SimpleNamespace(returncode=0)):
                with self.assertRaisesRegex(RuntimeError, 'tracer exit 1'):
                    run_cache(options, directory, manifest, None, {})
            tracers = [c for c in commands if c[:3] == ['docker', 'run', '-d']]
            self.assertEqual(len(tracers), 3)
            for command in tracers:
                self.assertNotIn('--mount', command)
                name = command[command.index('--name')+1]
                node = name.rsplit('-', 1)[-1]
                copied = next(i for i,c in enumerate(commands) if c[:3] == ['docker', 'cp', name+':/tmp/'+node+'.strace'])
                removed = commands.index(['docker', 'rm', '-f', name])
                self.assertLess(copied, removed)
                self.assertEqual((directory/'raw'/f'{node}.strace').read_bytes(), b'partial capture')

    def test_partial_endpoint_failure_preserves_successful_peer(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory/'raw').mkdir()
            manifest, targets = {'phases': {}}, {}
            def fetch(url, path, timeout):
                if 'broken' in url:
                    raise OSError('endpoint down')
                path.write_bytes(b'raw profile')
            with patch('profiling.capture.fetch', side_effect=fetch), \
                 patch('profiling.capture.analyze_cpu', return_value={'total_cpu_ns': 10}):
                with self.assertRaisesRegex(RuntimeError, 'endpoint down'):
                    capture_endpoints(directory, manifest, {'ok': 'http://ok', 'bad': 'http://broken'}, 1, None, targets)
            self.assertEqual(targets['ok']['cpu']['total_cpu_ns'], 10)
            self.assertEqual((directory/'raw/ok.cpu.pprof').read_bytes(), b'raw profile')

    def test_optional_snapshot_failure_is_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory/'raw').mkdir()
            manifest, targets = {'phases': {}}, {}
            def fetch(url, path, timeout):
                if '/profile?' not in url:
                    raise OSError('snapshot unavailable')
                path.write_bytes(b'profile')
            with patch('profiling.capture.fetch', side_effect=fetch), \
                 patch('profiling.capture.analyze_cpu', return_value={'total_cpu_ns': 10}):
                capture_endpoints(directory, manifest, {'ok': 'http://ok'}, 1, None, targets)
            self.assertEqual(len(manifest['warnings']), 2)
            self.assertIn('end_utc', manifest['phases']['cpu'])
            self.assertIn('cpu', targets['ok'])

    def test_timeout_retains_command_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp)/'commands.jsonl'
            with patch('profiling.capture.subprocess.run', side_effect=subprocess.TimeoutExpired(
                    ['build'], 1, output=b'partial build', stderr=b'timed out')):
                with self.assertRaises(subprocess.TimeoutExpired):
                    execute(['build'], log=log, timeout=1)
            record = json.loads(log.read_text())
            self.assertIsNone(record['exit_code'])
            self.assertEqual(record['stdout'], 'partial build')

    def test_failed_run_retains_manifest_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp, patch('profiling.capture.execute', return_value='fixture'):
            directory, manifest = new_run(Path(tmp)/'run', 'capture')
            (directory/'raw/partial').write_bytes(b'partial')
            finish(directory, manifest, {}, error=RuntimeError('capture failed'))
            saved = json.loads((directory/'manifest.json').read_text())
            self.assertEqual(saved['status'], 'failed')
            self.assertEqual(saved['errors'], ['capture failed'])
            self.assertEqual(saved['files']['raw/partial']['bytes'], 7)
            with self.assertRaisesRegex(ValueError, 'must be empty'):
                new_run(directory, 'capture')
            self.assertEqual((directory/'raw/partial').read_bytes(), b'partial')

    def test_startup_failure_or_interrupt_cleans_only_owned_resources(self):
        config = {'services': {n: {'environment': {}} for n in ('node1', 'node2', 'node3', 'loadgen')}}
        options = SimpleNamespace(workers=1, keys=615, gomaxprocs=1, fsync=False,
                                  seconds=1, warmup=0, syscalls=False)
        for failure in (RuntimeError('startup failed'), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as tmp:
                credential = Path(tmp)/'source-credential'
                credential.write_text('public-demo-credential')
                config['secrets'] = {'credential': {'file': str(credential)}}
                commands = []
                def command(args, **kwargs):
                    args = list(map(str, args))
                    commands.append(args)
                    if 'config' in args: return json.dumps(config)
                    if 'inspect' in args: return '[{"Id":"image-id"}]'
                    if 'version' in args: return '{}'
                    if 'ps' in args: return 'created-' + args[-1]
                    if 'up' in args: raise failure
                    if 'logs' in args: raise OSError('logs unavailable')
                    return ''
                manifest = {'errors': []}
                with patch('profiling.capture.execute', side_effect=command), \
                     patch('profiling.capture.subprocess.run', return_value=SimpleNamespace(returncode=0)) as remove:
                    with self.assertRaises(type(failure)):
                        run_cache(options, Path(tmp), manifest, None, {})
                down = next(c for c in commands if 'down' in c)
                self.assertTrue(down[down.index('-p')+1].startswith('gq-profile-'))
                self.assertIn('--volumes', down)
                self.assertTrue(remove.call_args.args[0][-1].startswith('gquorum-cache:profile-'))
                self.assertEqual(len(manifest['warnings']), 1)
                copies = [c for c in commands if c[:2] == ['docker', 'cp']]
                self.assertEqual([c[-1] for c in copies],
                                 ['created-'+n+':/cache-profile-credential' for n in ('node1', 'node2', 'node3')])
                up_index = next(i for i,c in enumerate(commands) if 'up' in c)
                self.assertTrue(all(commands.index(c) < up_index for c in copies))
                self.assertEqual((Path(tmp)/'demo-credential.txt').stat().st_mode & 0o777, 0o444)


if __name__ == '__main__':
    unittest.main()
