// Browser-side equivalent of sim_visualize.parse_log. No network access.
namespace SimLog {
  export interface Event {
    at: number; message: string; node: string; level: string; step: number; line: number;
    kind: string; role?: string; term?: number; health?: string; group?: string;
    target?: string; outage?: number; source: {file?: string; line?: number; function?: string};
    details?: unknown;
  }
  export interface Capture {
    name: string; seed: number | null; status: string; duration: number; nodes: string[];
    events: Event[]; build?: string; started: string; source?: string;
    summary: null | {checks: number; failures: number; cadence: string; deadline: string; slowest: string};
    diagnostics?: {line: number; text: string}[];
    clock?: string; capture_id?: string; scope?: string;
  }
  export interface Report {tests: Capture[]; source: string; warnings: string[]}
  function duration(value: string): number {
    const units: Record<string, number> = {ns: 1e-9, "µs": 1e-6, us: 1e-6, ms: .001, s: 1, m: 60, h: 3600};
    const parts = [...value.matchAll(/([\d.]+)(ns|µs|us|ms|s|m|h)/g)];
    if (!parts.length || parts.map(p => p[0]).join("") !== value) throw new Error(`Invalid duration: ${value}`);
    const result = parts.reduce((sum, part) => sum + Number(part[1]) * units[part[2]], 0);
    if (!Number.isFinite(result)) throw new Error(`Invalid duration: ${value}`);
    return result;
  }
  function object(value: unknown): value is Record<string, unknown> {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }
  function json(line: string): unknown {
    try { return JSON.parse(line); } catch { return null; }
  }
  function timestamp(value: unknown): number {
    if (typeof value !== "string") return NaN;
    // Keep sub-millisecond ordering in virtual time; Date.parse truncates it.
    const fractional = value.match(/\.(\d+)(?:Z|[+-]\d\d:\d\d)$/);
    const extra = fractional ? Number(`0.${fractional[1]}`) - Number(`0.${fractional[1].slice(0, 3)}`) : 0;
    return Date.parse(value) / 1000 + extra;
  }
  export function parse(contents: string, source: string): Report {
    const tests: Capture[] = [], warnings: string[] = [];
    const latest = new Map<string, Capture>();
    const captures = new Map<string, {capture: Capture; origin: number}>();
    let active: Capture | undefined;
    function create(name: string, seed: number | null) {
      const capture: Capture = {name, seed, status: "INCOMPLETE", events: [], duration: 0,
        nodes: [], summary: null, started: "", source};
      tests.push(capture); latest.set(`${name}/seed=${seed}`, capture);
      return capture;
    }
    function consume(line: string, number: number, label?: string) {
      const prefix = line.match(/^\[(.+\/seed=-?\d+)\] (.*)$/);
      if (prefix) { label = prefix[1]; line = prefix[2]; }
      const start = line.match(/^\s*=== RUN\s+(.+) \(seed (-?\d+)\)\s*$/);
      if (start) { active = create(start[1], Number(start[2])); return; }
      let target = label ? latest.get(label) : active;
      const result = line.match(/^\s*--- (PASS|FAIL|SKIP): (.+) \(([0-9.]+)s simulated\)\s*$/);
      if (result) {
        if (!target || target.name !== result[2]) warnings.push(`Line ${number}: unmatched test result`);
        else { target.status = result[1]; target.duration = Number(result[3]); }
        return;
      }
      const record = json(line);
      if (!object(record)) {
        if (target && line.trim() && !line.startsWith("Command:")) {
          (target.diagnostics ||= []).push({line: number, text: line});
        }
        return;
      }
      if (!("time" in record) || !("msg" in record)) return;
      if (!target) {
        if (label) {
          // Never attach an unknown concurrent worker's output to another test.
          warnings.push(`Line ${number}: output has no matching test start (${label})`);
          return;
        }
        target = active = create("Unlabelled capture", null);
      }
      const at = timestamp(record.time);
      if (!Number.isFinite(at)) { warnings.push(`Line ${number}: invalid timestamp`); return; }
      const event: Event = {at, message: String(record.msg), node: String(record.machine ?? "main"),
        level: String(record.level ?? "INFO"), step: typeof record.step === "number" ? record.step : number,
        line: number, kind: "log", source: object(record.source) ? record.source : {}};
      const role = event.message.match(/^\w+ became (leader|follower|candidate|pre-candidate) at term (\d+)/);
      if (role) { event.kind = "role"; event.role = role[1]; event.term = Number(role[2]); }
      if (event.message === "group health changed") {
        event.kind = "health"; event.health = String(record.state ?? "unknown"); event.group = String(record.group ?? "");
      }
      const outage = event.message.match(/cycle=(\d+) isolate=(\S+) interval=(\S+) outage=(\S+)/);
      if (outage) {
        try { event.outage = duration(outage[4]); event.kind = "partition"; event.target = outage[2]; }
        catch { warnings.push(`Line ${number}: invalid isolation duration`); }
      }
      const summary = event.message.match(/quorum checks=(\d+) failures=(\d+) cadence=(\S+) deadline=(\S+) slowest=(\S+)/);
      if (summary) target.summary = {checks: Number(summary[1]), failures: Number(summary[2]),
        cadence: summary[3], deadline: summary[4], slowest: summary[5]};
      const build = event.message.match(/BuildDigest:([^ ]+)/);
      if (build) target.build = build[1];
      target.events.push(event);
    }
    contents.split(/\r?\n/).forEach((line, index) => {
      const outer = json(line);
      if (object(outer) && outer.replay === "gquorum/v1") {
        const id = String(outer.id);
        if (outer.event === "start") {
          const capture = create(String(outer.test), null);
          capture.clock = "wall"; capture.capture_id = id; capture.scope = String(outer.scope || "");
          captures.set(id, {capture, origin: timestamp(outer.time)});
          return;
        }
        const current = captures.get(id);
        if (!current) { warnings.push(`Line ${index + 1}: replay record without matching start`); return; }
        if (outer.event === "end") { current.capture.duration = Number(outer.duration); return; }
        if (outer.event === "result") { current.capture.status = String(outer.status); return; }
        if (outer.event === "observation") {
          const at = timestamp(outer.time);
          if (!Number.isFinite(at)) { warnings.push(`Line ${index + 1}: invalid timestamp`); return; }
          current.capture.events.push({at, message: String(outer.msg), node: String(outer.machine),
            line: index + 1, level: "INFO", step: index + 1, kind: "status", source: {},
            role: String(outer.role), term: typeof outer.term === "number" ? outer.term : undefined,
            health: String(outer.health), details: outer.details});
        }
        return;
      }
      if (object(outer) && "Action" in outer) {
        if (typeof outer.Output === "string") {
          const label = typeof outer.Test === "string" ? outer.Test : undefined;
          for (const output of outer.Output.trimEnd().split("\n")) if (output) consume(output, index + 1, label);
        }
      } else consume(line, index + 1);
    });
    if (!tests.length) throw new Error("No simulation tests or timestamped Gosim records found.");
    for (const capture of tests) {
      const origin = (capture.capture_id ? captures.get(capture.capture_id)?.origin : undefined)
        ?? capture.events.reduce((min, e) => Math.min(min, e.at), Infinity);
      capture.started = Number.isFinite(origin) ? new Date(origin * 1000).toISOString() : "";
      const nodes = new Set<string>();
      for (const event of capture.events) {
        event.at -= origin;
        if (event.node !== "main") nodes.add(event.node);
        if (event.target) nodes.add(event.target);
        capture.duration = Math.max(capture.duration, event.at);
      }
      capture.nodes = [...nodes].sort();
      capture.events.sort((a, b) => a.at - b.at || a.step - b.step || a.line - b.line);
      capture.duration = Math.max(capture.duration, .001);
    }
    return {source, tests, warnings};
  }
}
