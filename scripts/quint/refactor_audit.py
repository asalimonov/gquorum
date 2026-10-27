#!/usr/bin/env python3
"""Audit the mechanical Quint extraction against its recorded baseline.

Defaults to the mechanical extraction commit c5ece71; --revision overrides it.
This checks token-preserving transforms and adapters, not semantic equivalence.
"""
from pathlib import Path
import argparse
import json
import re
import subprocess
REPO = Path(__file__).resolve().parents[2]
ROOT = REPO / 'scripts/quint'

def blocks(s):
    ms = list(re.finditer('^  (action|pure def|pure val|const|var|import) (\\w+)', s, re.M))
    end = s.rfind('}')
    return [(m.group(1), m.group(2), s[m.start():ms[i + 1].start() if i + 1 < len(ms) else end].rstrip(), m.start(), ms[i + 1].start() if i + 1 < len(ms) else end) for i, m in enumerate(ms)]
consttypes = {'NODE_POOL': 'Set[NodeName]', 'MAX_REQUESTS': 'int', 'FOUNDERS': 'NodeName -> int', 'LOAD_IDS': 'Set[GroupId]', 'INITIAL_LEADER': 'NodeName', 'MAX_POSITION': 'int', 'MAX_INCARNATION': 'int', 'MAX_DELIVERIES': 'int', 'rules': 'Rules'}
rx = re.compile('//[^\\n]*|/\\*[\\s\\S]*?\\*/|"(?:\\\\.|[^"\\\\])*"|[A-Za-z_]\\w*|\\d+|::|=>|->|[^\\w\\s]')

def tokens(s):
    return [(m.group(), m.start(), m.end()) for m in rx.finditer(s) if not m.group().startswith(('//', '/*'))]

def replace_id(s, old, new):
    for v, a, b in reversed(tokens(s)):
        if v == old:
            s = s[:a] + new + s[b:]
    return s

def splitparts(s):
    out = []
    level = 0
    start = 0
    for v, a, b in tokens(s):
        if v in ['{', '(', '[']:
            level += 1
        elif v in ['}', ')', ']']:
            level -= 1
        elif v == ',' and level == 0:
            out.append(s[start:a])
            start = b
    if s[start:].strip():
        out.append(s[start:])
    return out

def convert(body, mode):
    while re.search("\\bworld\\s*'", body):
        ts = tokens(body)
        stack = []
        choices = []
        for i, (v, a, b) in enumerate(ts):
            if v == '{':
                stack.append((i, i > 0 and ts[i - 1][0] == 'all'))
            elif v == '}':
                j, isall = stack.pop()
                if isall:
                    parts = splitparts(body[ts[j][2]:a])
                    assign = []
                    guards = []
                    for part in parts:
                        pts = tokens(part)
                        if len(pts) >= 3 and [q[0] for q in pts[:3]] == ['world', "'", '=']:
                            assign.append(part[pts[2][2]:].strip())
                        elif pts:
                            guards.append(part.strip())
                    if assign:
                        choices.append((ts[j - 1][1], b, assign, guards))
        assert choices, body[:300]
        a, b, assign, guards = choices[0]
        assert len(assign) == 1
        value = ('and {\n' + ',\n'.join(guards) + '\n}' if guards else 'true') if mode == 'guard' else '(' + assign[0] + ')'
        body = body[:a] + value + body[b:]
    return replace_id(body, 'world', 'w')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', default='c5ece71', help='Mechanical snapshot to audit (default: c5ece71; later helper revisions are not supported)')
    args = parser.parse_args()
    ledger = json.loads((REPO / 'docs/progress/20261028-quint-action-map.json').read_text())

    def read(path, rev=None):
        if rev:
            return subprocess.check_output(['git', 'show', f'{rev}:scripts/quint/{path}'], cwd=REPO, text=True)
        return (ROOT / path).read_text()
    original = {name: decl for kind, name, decl, *_ in blocks(read('cluster.qnt', ledger['baseline'])) if kind == 'action'}
    facade = read('cluster.qnt', args.revision)
    actual = {name: decl for kind, name, decl, *_ in blocks(facade) if kind == 'action'}
    aliases = {m.group(1): m.group(2) for m in re.finditer('import (\\w+) as (\\w+) from', facade)}
    normalize = lambda s: [t[0] for t in tokens(s)]
    failures = []
    if set(original) != set(actual):
        parser.error('Action inventory differs from the mechanical snapshot. Use --revision c5ece71; this audit does not validate subsequent helper rewrites.')
    candidate_types = {'handle': ('tr', 'Transition'), 'applyMeta': ('replica', 'ReplicaKey'), 'dispatchGrantFrom': ('replica', 'ReplicaKey')}
    for row in ledger['actions']:
        name = row['action']
        decl = original[name]
        if row['location'] == 'cluster.qnt':
            if normalize(actual[name]) != normalize(decl):
                failures.append(name + ': retained body changed')
            continue
        m = re.match('  action \\w+(?:\\((.*?)\\))?(?:: bool)?\\s*=\\s*([\\s\\S]*)', decl)
        params = m.group(1) or ''
        body = m.group(2)
        definitions = {n: d for k, n, d, *_ in blocks(read(row['location'], args.revision)) if k == 'pure def'}
        choice = None
        if name in candidate_types:
            choice, typ = candidate_types[name]
            nd = re.search('^\\s*nondet ' + choice + ' = (.*?)\\.oneOf\\(\\)\\s*$', body, re.M)
            candidate = replace_id(body[:nd.start()] + '\n    ' + nd.group(1) + '\n  }', 'world', 'w')
            given = definitions[name + 'Candidates'].split(' = ', 1)[1]
            if normalize(candidate) != normalize(given):
                failures.append(name + ': candidates differ')
            body = body[:nd.start()] + '\n' + body[nd.end():]
        for mode, suffix in [('guard', 'Enabled'), ('post', 'Next')]:
            expected = convert(body, mode)
            for old, new in [('freshMetaAvailable', 'M::freshMetaAvailable'), ('locallyLeader', 'Sessions::locallyLeader')]:
                expected = re.sub('(?<![\\w:])' + old + '\\(', new + '(', expected)
            given = definitions[name + suffix].split(' = ', 1)[1]
            if normalize(expected) != normalize(given):
                failures.append(name + ': ' + mode + ' differs')
        alias = aliases[row['module']]
        names = [p.split(':')[0].strip() for p in splitparts(params)]
        inputs = ', '.join(['world'] + names + ([choice] if choice else []) + row['constants'])
        expected = 'action ' + name + ('(' + params + ')' if params else '') + ': bool = { '
        if choice:
            inputs0 = ', '.join(['world'] + names + row['constants'])
            expected += 'nondet ' + choice + ' = ' + alias + '::' + name + 'Candidates(' + inputs0 + ').oneOf() '
        expected += 'if (' + alias + '::' + name + 'Enabled(' + inputs + ")) world' = " + alias + '::' + name + 'Next(' + inputs + ") else all { false, world' = world } }"
        if normalize(expected) != normalize(actual[name]):
            failures.append(name + ': adapter differs')
    for fail in failures:
        print(fail)
    print(f'{args.revision}: {len(original)} action mappings checked; {len(failures)} differences')
    return bool(failures)
if __name__ == '__main__':
    raise SystemExit(main())
