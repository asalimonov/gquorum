"use strict";
// Browser-side equivalent of sim_visualize.parse_log. No network access.
var SimLog;
(function (SimLog) {
    function duration(value) {
        const units = { ns: 1e-9, "µs": 1e-6, us: 1e-6, ms: .001, s: 1, m: 60, h: 3600 };
        const parts = [...value.matchAll(/([\d.]+)(ns|µs|us|ms|s|m|h)/g)];
        if (!parts.length || parts.map(p => p[0]).join("") !== value)
            throw new Error(`Invalid duration: ${value}`);
        const result = parts.reduce((sum, part) => sum + Number(part[1]) * units[part[2]], 0);
        if (!Number.isFinite(result))
            throw new Error(`Invalid duration: ${value}`);
        return result;
    }
    function object(value) {
        return value !== null && typeof value === "object" && !Array.isArray(value);
    }
    function json(line) {
        try {
            return JSON.parse(line);
        }
        catch {
            return null;
        }
    }
    function timestamp(value) {
        if (typeof value !== "string")
            return NaN;
        // Keep sub-millisecond ordering in virtual time; Date.parse truncates it.
        const fractional = value.match(/\.(\d+)(?:Z|[+-]\d\d:\d\d)$/);
        const extra = fractional ? Number(`0.${fractional[1]}`) - Number(`0.${fractional[1].slice(0, 3)}`) : 0;
        return Date.parse(value) / 1000 + extra;
    }
    function parse(contents, source) {
        const tests = [], warnings = [];
        const latest = new Map();
        const captures = new Map();
        let active;
        function create(name, seed) {
            const capture = { name, seed, status: "INCOMPLETE", events: [], duration: 0,
                nodes: [], summary: null, started: "", source };
            tests.push(capture);
            latest.set(`${name}/seed=${seed}`, capture);
            return capture;
        }
        function consume(line, number, label) {
            const prefix = line.match(/^\[(.+\/seed=-?\d+)\] (.*)$/);
            if (prefix) {
                label = prefix[1];
                line = prefix[2];
            }
            const start = line.match(/^\s*=== RUN\s+(.+) \(seed (-?\d+)\)\s*$/);
            if (start) {
                active = create(start[1], Number(start[2]));
                return;
            }
            let target = label ? latest.get(label) : active;
            const result = line.match(/^\s*--- (PASS|FAIL|SKIP): (.+) \(([0-9.]+)s simulated\)\s*$/);
            if (result) {
                if (!target || target.name !== result[2])
                    warnings.push(`Line ${number}: unmatched test result`);
                else {
                    target.status = result[1];
                    target.duration = Number(result[3]);
                }
                return;
            }
            const record = json(line);
            if (!object(record)) {
                if (target && line.trim() && !line.startsWith("Command:")) {
                    (target.diagnostics || (target.diagnostics = [])).push({ line: number, text: line });
                }
                return;
            }
            if (!("time" in record) || !("msg" in record))
                return;
            if (!target) {
                if (label) {
                    // Never attach an unknown concurrent worker's output to another test.
                    warnings.push(`Line ${number}: output has no matching test start (${label})`);
                    return;
                }
                target = active = create("Unlabelled capture", null);
            }
            const at = timestamp(record.time);
            if (!Number.isFinite(at)) {
                warnings.push(`Line ${number}: invalid timestamp`);
                return;
            }
            const event = { at, message: String(record.msg), node: String(record.machine ?? "main"),
                level: String(record.level ?? "INFO"), step: typeof record.step === "number" ? record.step : number,
                line: number, kind: "log", source: object(record.source) ? record.source : {} };
            const role = event.message.match(/^\w+ became (leader|follower|candidate|pre-candidate) at term (\d+)/);
            if (role) {
                event.kind = "role";
                event.role = role[1];
                event.term = Number(role[2]);
            }
            if (event.message === "group health changed") {
                event.kind = "health";
                event.health = String(record.state ?? "unknown");
                event.group = String(record.group ?? "");
            }
            const outage = event.message.match(/cycle=(\d+) isolate=(\S+) interval=(\S+) outage=(\S+)/);
            if (outage) {
                try {
                    event.outage = duration(outage[4]);
                    event.kind = "partition";
                    event.target = outage[2];
                }
                catch {
                    warnings.push(`Line ${number}: invalid isolation duration`);
                }
            }
            const summary = event.message.match(/quorum checks=(\d+) failures=(\d+) cadence=(\S+) deadline=(\S+) slowest=(\S+)/);
            if (summary)
                target.summary = { checks: Number(summary[1]), failures: Number(summary[2]),
                    cadence: summary[3], deadline: summary[4], slowest: summary[5] };
            const build = event.message.match(/BuildDigest:([^ ]+)/);
            if (build)
                target.build = build[1];
            target.events.push(event);
        }
        contents.split(/\r?\n/).forEach((line, index) => {
            const outer = json(line);
            if (object(outer) && outer.replay === "gquorum/v1") {
                const id = String(outer.id);
                if (outer.event === "start") {
                    const capture = create(String(outer.test), null);
                    capture.clock = "wall";
                    capture.capture_id = id;
                    capture.scope = String(outer.scope || "");
                    captures.set(id, { capture, origin: timestamp(outer.time) });
                    return;
                }
                const current = captures.get(id);
                if (!current) {
                    warnings.push(`Line ${index + 1}: replay record without matching start`);
                    return;
                }
                if (outer.event === "end") {
                    current.capture.duration = Number(outer.duration);
                    return;
                }
                if (outer.event === "result") {
                    current.capture.status = String(outer.status);
                    return;
                }
                if (outer.event === "observation") {
                    const at = timestamp(outer.time);
                    if (!Number.isFinite(at)) {
                        warnings.push(`Line ${index + 1}: invalid timestamp`);
                        return;
                    }
                    current.capture.events.push({ at, message: String(outer.msg), node: String(outer.machine),
                        line: index + 1, level: "INFO", step: index + 1, kind: "status", source: {},
                        role: String(outer.role), term: typeof outer.term === "number" ? outer.term : undefined,
                        health: String(outer.health), details: outer.details });
                }
                return;
            }
            if (object(outer) && "Action" in outer) {
                if (typeof outer.Output === "string") {
                    const label = typeof outer.Test === "string" ? outer.Test : undefined;
                    for (const output of outer.Output.trimEnd().split("\n"))
                        if (output)
                            consume(output, index + 1, label);
                }
            }
            else
                consume(line, index + 1);
        });
        if (!tests.length)
            throw new Error("No simulation tests or timestamped Gosim records found.");
        for (const capture of tests) {
            const origin = (capture.capture_id ? captures.get(capture.capture_id)?.origin : undefined)
                ?? capture.events.reduce((min, e) => Math.min(min, e.at), Infinity);
            capture.started = Number.isFinite(origin) ? new Date(origin * 1000).toISOString() : "";
            const nodes = new Set();
            for (const event of capture.events) {
                event.at -= origin;
                if (event.node !== "main")
                    nodes.add(event.node);
                if (event.target)
                    nodes.add(event.target);
                capture.duration = Math.max(capture.duration, event.at);
            }
            capture.nodes = [...nodes].sort();
            capture.events.sort((a, b) => a.at - b.at || a.step - b.step || a.line - b.line);
            capture.duration = Math.max(capture.duration, .001);
        }
        return { source, tests, warnings };
    }
    SimLog.parse = parse;
})(SimLog || (SimLog = {}));
// Compiled with the pinned TypeScript compiler; embedded by sim_visualize.py.
(() => {
    const $ = (id) => document.getElementById(id);
    let data = JSON.parse($("capture-data").textContent || "{}");
    const colors = {
        leader: "#c4e77c", follower: "#8db6cb", candidate: "#edb15e", "pre-candidate": "#cfa578",
        unknown: "#a1aaa0", healthy: "#9fbd7d", degraded: "#e6b271", unhealthy: "#e08e76", unavailable: "#e08e76",
        nonleader: "#8db6cb", "startup-ready": "#9fbd7d", "not-ready": "#e6b271", unreachable: "#e08e76",
    };
    let current = 0, time = 0, playing = false, speed = 5, frameAt = 0;
    const saved = new Map();
    const svg = document.getElementById("cluster");
    const namespace = "http://www.w3.org/2000/svg";
    const nodeViews = new Map();
    const links = [];
    let eventViews = [];
    let cursorViews = [];
    let lastEventIndex = -2;
    const test = () => data.tests[current];
    const text = (id, value) => { $(id).textContent = value; };
    const seconds = (value) => `${value.toFixed(2)}s`;
    const clock = (value) => {
        const ms = Math.round(value * 1000);
        return `${Math.floor(ms / 60000).toString().padStart(2, "0")}:${(Math.floor(ms / 1000) % 60).toString().padStart(2, "0")}.${(ms % 1000).toString().padStart(3, "0")}`;
    };
    function element(tag, attrs, content) {
        const el = document.createElementNS(namespace, tag);
        for (const [key, value] of Object.entries(attrs))
            el.setAttribute(key, String(value));
        if (content !== undefined)
            el.textContent = content;
        return el;
    }
    function stateEvents() { return test().events.filter(e => ["role", "health", "partition", "status"].includes(e.kind)); }
    function scheduled(at) {
        return test().events.filter(e => e.kind === "partition" && e.at <= at && at < e.at + (e.outage || 0));
    }
    function nodeStates(at) {
        const states = new Map(test().nodes.map(name => [name, { role: "unknown", health: "unknown" }]));
        for (const event of test().events) {
            if (event.at > at)
                break;
            const state = states.get(event.node);
            if (!state)
                continue;
            if (event.kind === "role" || event.kind === "status") {
                state.role = event.role || "unknown";
                state.term = event.term;
                state.roleAt = event.at;
            }
            if (event.kind === "health" || event.kind === "status") {
                state.health = event.health || "unknown";
                state.healthAt = event.at;
            }
        }
        return states;
    }
    function buildScene() {
        svg.replaceChildren();
        nodeViews.clear();
        links.length = 0;
        const nodes = test().nodes;
        const positions = nodes.map((_, i) => {
            if (nodes.length === 1)
                return { x: 340, y: 170 };
            const angle = -Math.PI / 2 + i * Math.PI * 2 / nodes.length;
            return { x: 340 + Math.cos(angle) * 210, y: 175 + Math.sin(angle) * 105 };
        });
        for (let i = 0; i < nodes.length; i++) {
            for (let j = i + 1; j < nodes.length; j++) {
                const line = element("line", { x1: positions[i].x, y1: positions[i].y, x2: positions[j].x, y2: positions[j].y, class: "link" });
                svg.append(line);
                links.push({ from: nodes[i], to: nodes[j], line });
            }
        }
        nodes.forEach((name, i) => {
            const { x, y } = positions[i];
            const group = element("g", { transform: `translate(${x},${y})` });
            const ring = element("circle", { r: 53, fill: "#24302b", stroke: colors.unknown, "stroke-width": 2 });
            const title = element("title", {}, name);
            const role = element("text", { y: 8, class: "node-role" }, "unknown");
            const meta = element("text", { y: 27, class: "node-meta" }, "term —");
            const health = element("text", { y: 75, class: "node-meta" }, "health: unknown");
            group.append(title, ring, element("text", { y: -15, class: "node-name" }, name), role, meta, health);
            svg.append(group);
            nodeViews.set(name, { ring, role, meta, health, title });
        });
        if (!nodes.length)
            svg.append(element("text", { x: 340, y: 180, class: "node-name" }, "No node observations in this capture"));
    }
    function buildTimeline() {
        const timeline = $("timeline");
        timeline.replaceChildren();
        cursorViews = [];
        for (const node of test().nodes) {
            const row = document.createElement("div");
            row.className = "timeline-row";
            const label = document.createElement("span");
            label.className = "timeline-label";
            label.textContent = node;
            const track = document.createElement("div");
            track.className = "track";
            track.setAttribute("role", "slider");
            track.tabIndex = 0;
            track.setAttribute("aria-label", `${node} timeline`);
            track.setAttribute("aria-valuemin", "0");
            track.setAttribute("aria-valuemax", String(test().duration));
            const segment = (start, end, value, kind) => {
                const bar = document.createElement("span");
                bar.className = `segment ${kind}`;
                bar.style.left = `${start / test().duration * 100}%`;
                bar.style.width = `${(end - start) / test().duration * 100}%`;
                bar.style.background = colors[value] || colors.unknown;
                track.append(bar);
            };
            for (const kind of ["role", "health"]) {
                let previous = 0, value = "unknown";
                for (const event of test().events.filter(e => e.node === node && (e.kind === kind || e.kind === "status"))) {
                    segment(previous, event.at, value, kind);
                    previous = event.at;
                    value = (kind === "role" ? event.role : event.health) || "unknown";
                }
                segment(previous, test().duration, value, kind);
            }
            for (const event of test().events.filter(e => e.kind === "partition" && e.target === node)) {
                const bar = document.createElement("span");
                bar.className = "outage";
                bar.style.left = `${event.at / test().duration * 100}%`;
                bar.style.width = `${Math.min(event.outage || 0, test().duration - event.at) / test().duration * 100}%`;
                track.append(bar);
            }
            const cursor = document.createElement("span");
            cursor.className = "cursor";
            track.append(cursor);
            cursorViews.push(cursor);
            track.onclick = event => { const rect = track.getBoundingClientRect(); seek((event.clientX - rect.left) / rect.width * test().duration); };
            track.onkeydown = event => {
                if (event.key === "ArrowRight" || event.key === "ArrowLeft") {
                    event.preventDefault();
                    event.stopPropagation();
                    seek(time + (event.key === "ArrowRight" ? 1 : -1));
                }
            };
            track.onmousemove = event => {
                const rect = track.getBoundingClientRect();
                const at = (event.clientX - rect.left) / rect.width * test().duration;
                const state = nodeStates(at).get(node);
                track.title = `${seconds(at)} · ${state.role} · health ${state.health}`;
            };
            row.append(label, track);
            timeline.append(row);
        }
        const axis = $("time-axis");
        axis.replaceChildren();
        for (let i = 0; i <= 5; i++) {
            const span = document.createElement("span");
            span.textContent = seconds(test().duration * i / 5);
            axis.append(span);
        }
    }
    function buildEvents() {
        const all = $("event-filter").value === "all";
        const query = $("search").value.toLowerCase();
        const events = (all ? test().events : stateEvents()).filter(e => `${e.node} ${e.message}`.toLowerCase().includes(query));
        const list = $("events");
        list.replaceChildren();
        eventViews = [];
        lastEventIndex = -2;
        text("event-count", `${events.length} EVENTS`);
        for (const event of events) {
            const button = document.createElement("button");
            button.className = "event";
            const caption = document.createElement("span");
            caption.className = "event-caption";
            const when = document.createElement("span");
            when.textContent = `${seconds(event.at)} · ${event.node}`;
            const line = document.createElement("span");
            line.textContent = `LINE ${event.line}`;
            const message = document.createElement("span");
            message.className = "event-message";
            message.textContent = event.message;
            caption.append(when, line);
            button.append(caption, message);
            button.title = [event.source.file, event.source.line, event.source.function].filter(Boolean).join(":");
            button.onclick = () => { seek(event.at); showDetail(event); };
            list.append(button);
            eventViews.push({ event, button });
        }
        if (!events.length) {
            const p = document.createElement("p");
            p.className = "muted";
            p.textContent = "No matching observations.";
            list.append(p);
        }
        render();
    }
    function showDetail(event) {
        $("observation-detail").hidden = !event?.details;
        text("observation-label", event ? `${seconds(event.at)} · ${event.node}` : "");
        text("observation-json", event?.details ? JSON.stringify(event.details, null, 2) : "");
    }
    function render() {
        text("clock", clock(time));
        $("scrub").value = String(time);
        if (test().clock === "wall") {
            const latest = new Map();
            for (const event of test().events) {
                if (event.at > time)
                    break;
                if (event.kind === "status")
                    latest.set(event.node, event);
            }
            const targets = new Set();
            let live = 0, observed = 0;
            for (const event of latest.values()) {
                const detail = event.details;
                if (detail?.runtime) {
                    observed++;
                    if (detail.runtime.session_live)
                        live++;
                }
                if (typeof detail?.cluster?.target_version === "number")
                    targets.add(detail.cluster.target_version);
            }
            text("checks", targets.size ? [...targets].sort((a, b) => a - b).join(" / ") : "Not observed");
            text("failures", `${live} / ${observed} reports`);
        }
        const outages = scheduled(time), isolated = new Set(outages.map(e => e.target));
        for (const [node, state] of nodeStates(time)) {
            const view = nodeViews.get(node);
            view.ring.setAttribute("stroke", isolated.has(node) ? colors.unhealthy : colors[state.role] || colors.unknown);
            view.ring.setAttribute("stroke-width", state.role === "leader" ? "3" : "1.5");
            view.ring.setAttribute("stroke-dasharray", isolated.has(node) ? "4 4" : "none");
            view.role.textContent = state.role;
            view.role.setAttribute("fill", colors[state.role] || colors.unknown);
            view.meta.textContent = `term ${state.term ?? "—"}`;
            view.health.textContent = `${test().clock === "wall" ? "status" : "health"}: ${state.health}`;
            view.health.setAttribute("fill", colors[state.health] || colors.unknown);
            view.title.textContent = `${node}: ${state.role}, term ${state.term ?? "unknown"}; last role ${state.roleAt === undefined ? "unobserved" : seconds(state.roleAt)}; health ${state.health}, last health ${state.healthAt === undefined ? "unobserved" : seconds(state.healthAt)}`;
        }
        for (const link of links)
            link.line.setAttribute("class", isolated.has(link.from) || isolated.has(link.to) ? "link isolated" : "link");
        text("network-note", outages.length
            ? `Scheduled isolation: ${outages.map(e => `${e.target} until ${seconds(e.at + (e.outage || 0))}`).join(" · ")}. Repair is not logged.`
            : test().clock === "wall" ? "Links show fixture topology. Connectivity and quorum are not measured by this capture."
                : "No active logged isolation schedule. Links show topology, not verified connectivity.");
        for (const cursor of cursorViews) {
            cursor.style.left = `${Math.min(99.8, time / test().duration * 100)}%`;
            cursor.parentElement.setAttribute("aria-valuenow", String(time));
        }
        let index = -1;
        eventViews.forEach((entry, i) => { if (entry.event.at <= time)
            index = i; });
        if (index !== lastEventIndex) {
            eventViews.forEach((entry, i) => { entry.button.classList.toggle("future", i > index); entry.button.classList.toggle("active", i === index); });
            if (index >= 0) {
                const container = $("events"), button = eventViews[index].button;
                container.scrollTop = button.offsetTop - container.offsetTop - container.clientHeight / 2;
            }
            lastEventIndex = index;
            showDetail(index >= 0 ? eventViews[index].event : undefined);
        }
        text("play", playing ? "Ⅱ Pause" : "▶ Play");
        $("play").setAttribute("aria-label", playing ? "Pause animation" : "Play animation");
        saved.set(current, time);
    }
    function seek(value) { time = Math.max(0, Math.min(test().duration, value)); render(); }
    function select(index) {
        playing = false;
        current = index;
        time = saved.get(index) || 0;
        const capture = test();
        for (const [i, button] of Array.from($("tests").children).entries())
            button.setAttribute("aria-current", String(i === index));
        text("test-name", capture.name);
        text("test-meta", `CAPTURE ${String(index + 1).padStart(2, "0")} / ${capture.clock === "wall" ? "DOCKER · WALL CLOCK" : `SEED ${capture.seed ?? "UNKNOWN"}`}`);
        text("duration-label", capture.clock === "wall" ? "CAPTURE DURATION" : "SIMULATED DURATION");
        text("checks-label", capture.clock === "wall" ? "REPORTED TARGET VERSIONS" : "RUN-WIDE QUORUM CHECKS");
        text("failures-label", capture.clock === "wall" ? "LIVE SESSION OBSERVATIONS" : "RUN-WIDE PROBE FAILURES");
        text("follower-label", capture.clock === "wall" ? "Nonleader" : "Follower");
        $("build").hidden = capture.clock === "wall";
        $("scrub").setAttribute("aria-label", capture.clock === "wall" ? "Elapsed capture time" : "Simulated time");
        text("status-band-label", capture.clock === "wall" ? "Lower band: startup readiness / reachability" : "Lower band: reported health");
        $("docker-notes").hidden = capture.clock !== "wall";
        $("sim-notes").hidden = capture.clock === "wall";
        text("capture-scope", capture.scope || "");
        text("result", capture.status);
        $("result").dataset.status = capture.status;
        text("duration", seconds(capture.duration));
        text("node-count", String(capture.nodes.length));
        text("checks", capture.summary ? capture.summary.checks.toLocaleString() : "Not logged");
        text("failures", capture.summary ? String(capture.summary.failures) : "Not logged");
        text("probe-note", capture.clock === "wall"
            ? "Target versions and session liveness use the latest collected response from each node at the playhead. The session denominator counts successful runtime responses, excluding unreachable nodes. These are observations, not fresh quorum proofs."
            : capture.summary ? `Probe cadence ${capture.summary.cadence}; deadline ${capture.summary.deadline}; slowest completion ${capture.summary.slowest}.` : "No quorum-probe summary was logged for this test.");
        text("source", `Source: ${capture.source || data.source}`);
        text("build", capture.build ? `Build identity: ${capture.build}` : "Build identity was not logged.");
        const warnings = [...data.warnings];
        if (capture.status === "INCOMPLETE")
            warnings.push("This capture has no terminal test result. It may be truncated or interrupted.");
        if (capture.diagnostics?.length)
            warnings.push(`${capture.diagnostics.length} non-JSON diagnostic lines retained below.`);
        const notice = $("warnings");
        notice.hidden = warnings.length === 0;
        notice.replaceChildren();
        warnings.forEach(warning => { const p = document.createElement("p"); p.textContent = warning; notice.append(p); });
        if (capture.diagnostics?.length) {
            const details = document.createElement("details"), summary = document.createElement("summary"), pre = document.createElement("pre");
            summary.textContent = "Raw diagnostics";
            pre.style.whiteSpace = "pre-wrap";
            pre.textContent = capture.diagnostics.map(d => `Line ${d.line}: ${d.text}`).join("\n");
            details.append(summary, pre);
            notice.append(details);
        }
        $("scrub").max = String(capture.duration);
        buildScene();
        buildTimeline();
        buildEvents();
        render();
    }
    function install(report) {
        playing = false;
        time = 0;
        current = 0;
        saved.clear();
        data = report;
        $("tests").replaceChildren();
        $("search").value = "";
        $("event-filter").value = "state";
        $("playback").hidden = !data.tests.length;
        $("drop-zone").classList.toggle("empty", !data.tests.length);
        data.tests.forEach((capture, index) => {
            const button = document.createElement("button");
            button.className = "test-button";
            const name = document.createElement("span");
            name.textContent = capture.name;
            const small = document.createElement("span");
            small.className = "test-small";
            const seed = document.createElement("span");
            seed.textContent = `${capture.clock === "wall" ? "DOCKER" : `SEED ${capture.seed ?? "?"}`} · ${seconds(capture.duration)}`;
            const status = document.createElement("span");
            status.textContent = capture.status;
            status.className = capture.status === "PASS" ? "pass" : "fail";
            small.append(seed, status);
            button.append(name, small);
            button.onclick = () => select(index);
            $("tests").append(button);
        });
        text("test-count", String(data.tests.length));
        if (data.tests.length)
            select(0);
        else {
            const hint = document.createElement("p");
            hint.className = "muted";
            hint.textContent = "Load a capture to see its tests here.";
            $("tests").append(hint);
        }
    }
    let loadGeneration = 0;
    async function loadFiles(files) {
        if (!files.length)
            return;
        const generation = ++loadGeneration;
        playing = false;
        if (data.tests.length)
            render();
        $("load-status").classList.remove("error");
        text("load-status", `Reading ${files.length} file${files.length === 1 ? "" : "s"}…`);
        // Yield to paint the status before parsing potentially large captures.
        await new Promise(resolve => setTimeout(resolve, 0));
        try {
            const reports = await Promise.all(files.map(async (file) => {
                try {
                    return SimLog.parse(await file.text(), file.name);
                }
                catch (error) {
                    throw new Error(`${file.name}: ${error instanceof Error ? error.message : String(error)}`);
                }
            }));
            if (generation !== loadGeneration)
                return;
            const tests = reports.flatMap(report => report.tests);
            install({ source: files.map(file => file.name).join(", "), tests,
                warnings: reports.flatMap(report => report.warnings.map(warning => `${report.source}: ${warning}`)) });
            text("load-status", `${tests.length} test replays loaded from ${files.length} file${files.length === 1 ? "" : "s"}. Select a test and press Play.`);
        }
        catch (error) {
            if (generation !== loadGeneration)
                return;
            $("load-status").classList.add("error");
            text("load-status", `${error instanceof Error ? error.message : String(error)}${data.tests.length ? " Previous capture kept." : ""}`);
        }
    }
    const input = $("file-input"), drop = $("drop-zone");
    drop.onclick = () => input.click();
    drop.onkeydown = event => {
        if (event.target === drop && (event.key === "Enter" || event.key === " ")) {
            event.preventDefault();
            input.click();
        }
    };
    input.onchange = () => { void loadFiles(Array.from(input.files || [])); input.value = ""; };
    let dragDepth = 0;
    document.addEventListener("dragenter", event => {
        if (event.dataTransfer?.types.includes("Files")) {
            event.preventDefault();
            dragDepth++;
            drop.classList.add("dragging");
        }
    });
    document.addEventListener("dragover", event => {
        if (event.dataTransfer?.types.includes("Files")) {
            event.preventDefault();
            event.dataTransfer.dropEffect = "copy";
        }
    });
    document.addEventListener("dragleave", () => {
        if (--dragDepth <= 0) {
            dragDepth = 0;
            drop.classList.remove("dragging");
        }
    });
    document.addEventListener("drop", event => {
        event.preventDefault();
        dragDepth = 0;
        drop.classList.remove("dragging");
        void loadFiles(Array.from(event.dataTransfer?.files || []));
    });
    $("scrub").oninput = event => seek(Number(event.target.value));
    $("speed").onchange = event => { speed = Number(event.target.value); };
    $("event-filter").onchange = buildEvents;
    $("search").oninput = buildEvents;
    $("play").onclick = () => { if (time >= test().duration)
        time = 0; playing = !playing; frameAt = performance.now(); render(); };
    $("previous").onclick = () => {
        const values = stateEvents().filter(e => e.at < time - .00001);
        seek(values.length ? values[values.length - 1].at : 0);
    };
    $("next").onclick = () => seek(stateEvents().find(e => e.at > time + .00001)?.at ?? test().duration);
    document.addEventListener("keydown", event => {
        if (!data.tests.length || event.target.matches("input,select,button,[role=slider],[role=button],summary"))
            return;
        if (event.code === "Space") {
            event.preventDefault();
            $("play").click();
        }
        if (event.key === "ArrowRight")
            seek(time + 1);
        if (event.key === "ArrowLeft")
            seek(time - 1);
    });
    function frame(now) {
        if (playing) {
            time = Math.min(test().duration, time + Math.min(.25, (now - frameAt) / 1000) * speed);
            if (time >= test().duration)
                playing = false;
            render();
        }
        frameAt = now;
        requestAnimationFrame(frame);
    }
    install(data);
    requestAnimationFrame(frame);
})();
