// Compiled with the pinned TypeScript compiler; embedded by sim_visualize.py.
(() => {
  type LogEvent = SimLog.Event;
  interface NodeState {role: string; term?: number; health: string; healthAt?: number; roleAt?: number}
  const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
  let data = JSON.parse($("capture-data").textContent || "{}") as SimLog.Report;
  const colors: Record<string, string> = {
    leader: "#c4e77c", follower: "#8db6cb", candidate: "#edb15e", "pre-candidate": "#cfa578",
    unknown: "#a1aaa0", healthy: "#9fbd7d", degraded: "#e6b271", unhealthy: "#e08e76", unavailable: "#e08e76",
    nonleader: "#8db6cb", "startup-ready": "#9fbd7d", "not-ready": "#e6b271", unreachable: "#e08e76",
  };
  let current = 0, time = 0, playing = false, speed = 5, frameAt = 0;
  const saved = new Map<number, number>();
  const svg = document.getElementById("cluster") as unknown as SVGSVGElement;
  const namespace = "http://www.w3.org/2000/svg";
  const nodeViews = new Map<string, {ring: SVGCircleElement; role: SVGTextElement; meta: SVGTextElement;
    health: SVGTextElement; title: SVGTitleElement}>();
  const links: {from: string; to: string; line: SVGLineElement}[] = [];
  let eventViews: {event: LogEvent; button: HTMLButtonElement}[] = [];
  let cursorViews: HTMLElement[] = [];
  let lastEventIndex = -2;
  const test = () => data.tests[current];
  const text = (id: string, value: string) => { $(id).textContent = value; };
  const seconds = (value: number) => `${value.toFixed(2)}s`;
  const clock = (value: number) => {
    const ms = Math.round(value * 1000);
    return `${Math.floor(ms / 60000).toString().padStart(2, "0")}:${(Math.floor(ms / 1000) % 60).toString().padStart(2, "0")}.${(ms % 1000).toString().padStart(3, "0")}`;
  };
  function element<K extends keyof SVGElementTagNameMap>(tag: K, attrs: Record<string, string | number>, content?: string) {
    const el = document.createElementNS(namespace, tag);
    for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, String(value));
    if (content !== undefined) el.textContent = content;
    return el;
  }
  function stateEvents() { return test().events.filter(e => ["role", "health", "partition", "status"].includes(e.kind)); }
  function scheduled(at: number) {
    return test().events.filter(e => e.kind === "partition" && e.at <= at && at < e.at + (e.outage || 0));
  }
  function nodeStates(at: number) {
    const states = new Map(test().nodes.map(name => [name, {role: "unknown", health: "unknown"} as NodeState]));
    for (const event of test().events) {
      if (event.at > at) break;
      const state = states.get(event.node);
      if (!state) continue;
      if (event.kind === "role" || event.kind === "status") {
        state.role = event.role || "unknown"; state.term = event.term; state.roleAt = event.at;
      }
      if (event.kind === "health" || event.kind === "status") {
        state.health = event.health || "unknown"; state.healthAt = event.at;
      }
    }
    return states;
  }
  function buildScene() {
    svg.replaceChildren(); nodeViews.clear(); links.length = 0;
    const nodes = test().nodes;
    const positions = nodes.map((_, i) => {
      if (nodes.length === 1) return {x: 340, y: 170};
      const angle = -Math.PI / 2 + i * Math.PI * 2 / nodes.length;
      return {x: 340 + Math.cos(angle) * 210, y: 175 + Math.sin(angle) * 105};
    });
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const line = element("line", {x1: positions[i].x, y1: positions[i].y, x2: positions[j].x, y2: positions[j].y, class: "link"});
        svg.append(line); links.push({from: nodes[i], to: nodes[j], line});
      }
    }
    nodes.forEach((name, i) => {
      const {x, y} = positions[i];
      const group = element("g", {transform: `translate(${x},${y})`});
      const ring = element("circle", {r: 53, fill: "#24302b", stroke: colors.unknown, "stroke-width": 2});
      const title = element("title", {}, name);
      const role = element("text", {y: 8, class: "node-role"}, "unknown");
      const meta = element("text", {y: 27, class: "node-meta"}, "term —");
      const health = element("text", {y: 75, class: "node-meta"}, "health: unknown");
      group.append(title, ring, element("text", {y: -15, class: "node-name"}, name), role, meta, health);
      svg.append(group); nodeViews.set(name, {ring, role, meta, health, title});
    });
    if (!nodes.length) svg.append(element("text", {x: 340, y: 180, class: "node-name"}, "No node observations in this capture"));
  }
  function buildTimeline() {
    const timeline = $("timeline"); timeline.replaceChildren(); cursorViews = [];
    for (const node of test().nodes) {
      const row = document.createElement("div"); row.className = "timeline-row";
      const label = document.createElement("span"); label.className = "timeline-label"; label.textContent = node;
      const track = document.createElement("div"); track.className = "track";
      track.setAttribute("role", "slider"); track.tabIndex = 0; track.setAttribute("aria-label", `${node} timeline`);
      track.setAttribute("aria-valuemin", "0"); track.setAttribute("aria-valuemax", String(test().duration));
      const segment = (start: number, end: number, value: string, kind: string) => {
        const bar = document.createElement("span"); bar.className = `segment ${kind}`;
        bar.style.left = `${start / test().duration * 100}%`; bar.style.width = `${(end - start) / test().duration * 100}%`;
        bar.style.background = colors[value] || colors.unknown;
        track.append(bar);
      };
      for (const kind of ["role", "health"]) {
        let previous = 0, value = "unknown";
        for (const event of test().events.filter(e => e.node === node && (e.kind === kind || e.kind === "status"))) {
          segment(previous, event.at, value, kind);
          previous = event.at; value = (kind === "role" ? event.role : event.health) || "unknown";
        }
        segment(previous, test().duration, value, kind);
      }
      for (const event of test().events.filter(e => e.kind === "partition" && e.target === node)) {
        const bar = document.createElement("span"); bar.className = "outage";
        bar.style.left = `${event.at / test().duration * 100}%`;
        bar.style.width = `${Math.min(event.outage || 0, test().duration - event.at) / test().duration * 100}%`;
        track.append(bar);
      }
      const cursor = document.createElement("span"); cursor.className = "cursor"; track.append(cursor); cursorViews.push(cursor);
      track.onclick = event => { const rect = track.getBoundingClientRect(); seek((event.clientX - rect.left) / rect.width * test().duration); };
      track.onkeydown = event => {
        if (event.key === "ArrowRight" || event.key === "ArrowLeft") {
          event.preventDefault(); event.stopPropagation(); seek(time + (event.key === "ArrowRight" ? 1 : -1));
        }
      };
      track.onmousemove = event => {
        const rect = track.getBoundingClientRect();
        const at = (event.clientX - rect.left) / rect.width * test().duration;
        const state = nodeStates(at).get(node)!;
        track.title = `${seconds(at)} · ${state.role} · health ${state.health}`;
      };
      row.append(label, track); timeline.append(row);
    }
    const axis = $("time-axis"); axis.replaceChildren();
    for (let i = 0; i <= 5; i++) { const span = document.createElement("span"); span.textContent = seconds(test().duration * i / 5); axis.append(span); }
  }
  function buildEvents() {
    const all = $<HTMLSelectElement>("event-filter").value === "all";
    const query = $<HTMLInputElement>("search").value.toLowerCase();
    const events = (all ? test().events : stateEvents()).filter(e => `${e.node} ${e.message}`.toLowerCase().includes(query));
    const list = $("events"); list.replaceChildren(); eventViews = []; lastEventIndex = -2;
    text("event-count", `${events.length} EVENTS`);
    for (const event of events) {
      const button = document.createElement("button"); button.className = "event";
      const caption = document.createElement("span"); caption.className = "event-caption";
      const when = document.createElement("span"); when.textContent = `${seconds(event.at)} · ${event.node}`;
      const line = document.createElement("span"); line.textContent = `LINE ${event.line}`;
      const message = document.createElement("span"); message.className = "event-message"; message.textContent = event.message;
      caption.append(when, line); button.append(caption, message);
      button.title = [event.source.file, event.source.line, event.source.function].filter(Boolean).join(":");
      button.onclick = () => { seek(event.at); showDetail(event); };
      list.append(button); eventViews.push({event, button});
    }
    if (!events.length) { const p = document.createElement("p"); p.className = "muted"; p.textContent = "No matching observations."; list.append(p); }
    render();
  }
  function showDetail(event?: LogEvent) {
    $("observation-detail").hidden = !event?.details;
    text("observation-label", event ? `${seconds(event.at)} · ${event.node}` : "");
    text("observation-json", event?.details ? JSON.stringify(event.details, null, 2) : "");
  }
  function render() {
    text("clock", clock(time)); $<HTMLInputElement>("scrub").value = String(time);
    if (test().clock === "wall") {
      const latest = new Map<string, LogEvent>();
      for (const event of test().events) {
        if (event.at > time) break;
        if (event.kind === "status") latest.set(event.node, event);
      }
      const targets = new Set<number>();
      let live = 0, observed = 0;
      for (const event of latest.values()) {
        const detail = event.details as {runtime?: {session_live?: boolean}; cluster?: {target_version?: number}};
        if (detail?.runtime) { observed++; if (detail.runtime.session_live) live++; }
        if (typeof detail?.cluster?.target_version === "number") targets.add(detail.cluster.target_version);
      }
      text("checks", targets.size ? [...targets].sort((a, b) => a - b).join(" / ") : "Not observed");
      text("failures", `${live} / ${observed} reports`);
    }
    const outages = scheduled(time), isolated = new Set(outages.map(e => e.target));
    for (const [node, state] of nodeStates(time)) {
      const view = nodeViews.get(node)!;
      view.ring.setAttribute("stroke", isolated.has(node) ? colors.unhealthy : colors[state.role] || colors.unknown);
      view.ring.setAttribute("stroke-width", state.role === "leader" ? "3" : "1.5");
      view.ring.setAttribute("stroke-dasharray", isolated.has(node) ? "4 4" : "none");
      view.role.textContent = state.role; view.role.setAttribute("fill", colors[state.role] || colors.unknown);
      view.meta.textContent = `term ${state.term ?? "—"}`;
      view.health.textContent = `${test().clock === "wall" ? "status" : "health"}: ${state.health}`; view.health.setAttribute("fill", colors[state.health] || colors.unknown);
      view.title.textContent = `${node}: ${state.role}, term ${state.term ?? "unknown"}; last role ${state.roleAt === undefined ? "unobserved" : seconds(state.roleAt)}; health ${state.health}, last health ${state.healthAt === undefined ? "unobserved" : seconds(state.healthAt)}`;
    }
    for (const link of links) link.line.setAttribute("class", isolated.has(link.from) || isolated.has(link.to) ? "link isolated" : "link");
    text("network-note", outages.length
      ? `Scheduled isolation: ${outages.map(e => `${e.target} until ${seconds(e.at + (e.outage || 0))}`).join(" · ")}. Repair is not logged.`
      : test().clock === "wall" ? "Links show fixture topology. Connectivity and quorum are not measured by this capture."
      : "No active logged isolation schedule. Links show topology, not verified connectivity.");
    for (const cursor of cursorViews) {
      cursor.style.left = `${Math.min(99.8, time / test().duration * 100)}%`;
      cursor.parentElement!.setAttribute("aria-valuenow", String(time));
    }
    let index = -1;
    eventViews.forEach((entry, i) => { if (entry.event.at <= time) index = i; });
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
  function seek(value: number) { time = Math.max(0, Math.min(test().duration, value)); render(); }
  function select(index: number) {
    playing = false; current = index; time = saved.get(index) || 0;
    const capture = test();
    for (const [i, button] of Array.from($("tests").children).entries()) button.setAttribute("aria-current", String(i === index));
    text("test-name", capture.name); text("test-meta", `CAPTURE ${String(index + 1).padStart(2, "0")} / ${capture.clock === "wall" ? "DOCKER · WALL CLOCK" : `SEED ${capture.seed ?? "UNKNOWN"}`}`);
    text("duration-label", capture.clock === "wall" ? "CAPTURE DURATION" : "SIMULATED DURATION");
    text("checks-label", capture.clock === "wall" ? "REPORTED TARGET VERSIONS" : "RUN-WIDE QUORUM CHECKS");
    text("failures-label", capture.clock === "wall" ? "LIVE SESSION OBSERVATIONS" : "RUN-WIDE PROBE FAILURES");
    text("follower-label", capture.clock === "wall" ? "Nonleader" : "Follower");
    $("build").hidden = capture.clock === "wall";
    $<HTMLInputElement>("scrub").setAttribute("aria-label", capture.clock === "wall" ? "Elapsed capture time" : "Simulated time");
    text("status-band-label", capture.clock === "wall" ? "Lower band: startup readiness / reachability" : "Lower band: reported health");
    $("docker-notes").hidden = capture.clock !== "wall";
    $("sim-notes").hidden = capture.clock === "wall";
    text("capture-scope", capture.scope || "");
    text("result", capture.status); $("result").dataset.status = capture.status;
    text("duration", seconds(capture.duration)); text("node-count", String(capture.nodes.length));
    text("checks", capture.summary ? capture.summary.checks.toLocaleString() : "Not logged");
    text("failures", capture.summary ? String(capture.summary.failures) : "Not logged");
    text("probe-note", capture.clock === "wall"
      ? "Target versions and session liveness use the latest collected response from each node at the playhead. The session denominator counts successful runtime responses, excluding unreachable nodes. These are observations, not fresh quorum proofs."
      : capture.summary ? `Probe cadence ${capture.summary.cadence}; deadline ${capture.summary.deadline}; slowest completion ${capture.summary.slowest}.` : "No quorum-probe summary was logged for this test.");
    text("source", `Source: ${capture.source || data.source}`);
    text("build", capture.build ? `Build identity: ${capture.build}` : "Build identity was not logged.");
    const warnings = [...data.warnings];
    if (capture.status === "INCOMPLETE") warnings.push("This capture has no terminal test result. It may be truncated or interrupted.");
    if (capture.diagnostics?.length) warnings.push(`${capture.diagnostics.length} non-JSON diagnostic lines retained below.`);
    const notice = $("warnings"); notice.hidden = warnings.length === 0; notice.replaceChildren();
    warnings.forEach(warning => { const p = document.createElement("p"); p.textContent = warning; notice.append(p); });
    if (capture.diagnostics?.length) {
      const details = document.createElement("details"), summary = document.createElement("summary"), pre = document.createElement("pre");
      summary.textContent = "Raw diagnostics"; pre.style.whiteSpace = "pre-wrap";
      pre.textContent = capture.diagnostics.map(d => `Line ${d.line}: ${d.text}`).join("\n");
      details.append(summary, pre); notice.append(details);
    }
    $<HTMLInputElement>("scrub").max = String(capture.duration);
    buildScene(); buildTimeline(); buildEvents(); render();
  }
  function install(report: SimLog.Report) {
    playing = false; time = 0; current = 0; saved.clear(); data = report;
    $("tests").replaceChildren();
    $<HTMLInputElement>("search").value = "";
    $<HTMLSelectElement>("event-filter").value = "state";
    $("playback").hidden = !data.tests.length;
    $("drop-zone").classList.toggle("empty", !data.tests.length);
    data.tests.forEach((capture, index) => {
    const button = document.createElement("button"); button.className = "test-button";
    const name = document.createElement("span"); name.textContent = capture.name;
    const small = document.createElement("span"); small.className = "test-small";
    const seed = document.createElement("span"); seed.textContent = `${capture.clock === "wall" ? "DOCKER" : `SEED ${capture.seed ?? "?"}`} · ${seconds(capture.duration)}`;
    const status = document.createElement("span"); status.textContent = capture.status; status.className = capture.status === "PASS" ? "pass" : "fail";
    small.append(seed, status); button.append(name, small); button.onclick = () => select(index); $("tests").append(button);
    });
    text("test-count", String(data.tests.length));
    if (data.tests.length) select(0);
    else {
      const hint = document.createElement("p"); hint.className = "muted";
      hint.textContent = "Load a capture to see its tests here."; $("tests").append(hint);
    }
  }
  let loadGeneration = 0;
  async function loadFiles(files: File[]) {
    if (!files.length) return;
    const generation = ++loadGeneration;
    playing = false;
    if (data.tests.length) render();
    $("load-status").classList.remove("error");
    text("load-status", `Reading ${files.length} file${files.length === 1 ? "" : "s"}…`);
    // Yield to paint the status before parsing potentially large captures.
    await new Promise(resolve => setTimeout(resolve, 0));
    try {
      const reports = await Promise.all(files.map(async file => {
        try { return SimLog.parse(await file.text(), file.name); }
        catch (error) { throw new Error(`${file.name}: ${error instanceof Error ? error.message : String(error)}`); }
      }));
      if (generation !== loadGeneration) return;
      const tests = reports.flatMap(report => report.tests);
      install({source: files.map(file => file.name).join(", "), tests,
        warnings: reports.flatMap(report => report.warnings.map(warning => `${report.source}: ${warning}`))});
      text("load-status", `${tests.length} test replays loaded from ${files.length} file${files.length === 1 ? "" : "s"}. Select a test and press Play.`);
    } catch (error) {
      if (generation !== loadGeneration) return;
      $("load-status").classList.add("error");
      text("load-status", `${error instanceof Error ? error.message : String(error)}${data.tests.length ? " Previous capture kept." : ""}`);
    }
  }
  const input = $<HTMLInputElement>("file-input"), drop = $("drop-zone");
  drop.onclick = () => input.click();
  drop.onkeydown = event => {
    if (event.target === drop && (event.key === "Enter" || event.key === " ")) {
      event.preventDefault(); input.click();
    }
  };
  input.onchange = () => { void loadFiles(Array.from(input.files || [])); input.value = ""; };
  let dragDepth = 0;
  document.addEventListener("dragenter", event => {
    if (event.dataTransfer?.types.includes("Files")) {
      event.preventDefault(); dragDepth++; drop.classList.add("dragging");
    }
  });
  document.addEventListener("dragover", event => {
    if (event.dataTransfer?.types.includes("Files")) {
      event.preventDefault(); event.dataTransfer.dropEffect = "copy";
    }
  });
  document.addEventListener("dragleave", () => {
    if (--dragDepth <= 0) { dragDepth = 0; drop.classList.remove("dragging"); }
  });
  document.addEventListener("drop", event => {
    event.preventDefault(); dragDepth = 0; drop.classList.remove("dragging");
    void loadFiles(Array.from(event.dataTransfer?.files || []));
  });
  $<HTMLInputElement>("scrub").oninput = event => seek(Number((event.target as HTMLInputElement).value));
  $<HTMLSelectElement>("speed").onchange = event => { speed = Number((event.target as HTMLSelectElement).value); };
  $("event-filter").onchange = buildEvents; $("search").oninput = buildEvents;
  $("play").onclick = () => { if (time >= test().duration) time = 0; playing = !playing; frameAt = performance.now(); render(); };
  $("previous").onclick = () => {
    const values = stateEvents().filter(e => e.at < time - .00001);
    seek(values.length ? values[values.length - 1].at : 0);
  };
  $("next").onclick = () => seek(stateEvents().find(e => e.at > time + .00001)?.at ?? test().duration);
  document.addEventListener("keydown", event => {
    if (!data.tests.length || (event.target as HTMLElement).matches("input,select,button,[role=slider],[role=button],summary")) return;
    if (event.code === "Space") { event.preventDefault(); $("play").click(); }
    if (event.key === "ArrowRight") seek(time + 1);
    if (event.key === "ArrowLeft") seek(time - 1);
  });
  function frame(now: number) {
    if (playing) {
      time = Math.min(test().duration, time + Math.min(.25, (now - frameAt) / 1000) * speed);
      if (time >= test().duration) playing = false;
      render();
    }
    frameAt = now; requestAnimationFrame(frame);
  }
  install(data); requestAnimationFrame(frame);
})();
