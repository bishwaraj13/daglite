async function jget(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

function pill(state) {
  return `<span class="pill">${state}</span>`;
}

async function loadRuns() {
  const data = await jget("/api/runs");
  const tbody = document.querySelector("#runsTable tbody");
  tbody.innerHTML = "";
  for (const run of data.runs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${run.flow_name}:${run.flow_version}</td>
      <td>${pill(run.state)}</td>
      <td class="muted">${run.created_at_utc || ""}</td>
      <td class="muted">${run.started_at_utc || ""}</td>
      <td class="muted">${run.finished_at_utc || ""}</td>
    `;
    tr.addEventListener("click", () => selectRun(run.id));
    tbody.appendChild(tr);
  }
}

function nodeClass(state) {
  return `node state-${state}`;
}

function renderGraph(container, graph) {
  // Simple layered layout: levels by topo distance (server already returns edges; we compute levels client-side)
  const nodes = graph.nodes;
  const edges = graph.edges;

  const byName = new Map(nodes.map(n => [n.task_name, n]));
  const indeg = new Map(nodes.map(n => [n.task_name, 0]));
  const children = new Map(nodes.map(n => [n.task_name, []]));

  for (const [u, v] of edges) {
    if (children.has(u)) children.get(u).push(v);
    indeg.set(v, (indeg.get(v) || 0) + 1);
  }

  // topo
  const q = [];
  for (const [k, v] of indeg.entries()) if (v === 0) q.push(k);
  const order = [];
  while (q.length) {
    const n = q.shift();
    order.push(n);
    for (const c of (children.get(n) || [])) {
      indeg.set(c, indeg.get(c) - 1);
      if (indeg.get(c) === 0) q.push(c);
    }
  }

  const level = new Map(order.map(n => [n, 0]));
  for (const u of order) {
    for (const v of (children.get(u) || [])) {
      level.set(v, Math.max(level.get(v) || 0, (level.get(u) || 0) + 1));
    }
  }

  const layers = new Map();
  for (const n of nodes) {
    const lv = level.get(n.task_name) || 0;
    if (!layers.has(lv)) layers.set(lv, []);
    layers.get(lv).push(n.task_name);
  }

  const width = container.clientWidth || 800;
  const height = 320;
  const padX = 40, padY = 30;
  const layerKeys = Array.from(layers.keys()).sort((a,b) => a-b);
  const maxLayer = layerKeys.length ? Math.max(...layerKeys) : 0;
  const dx = maxLayer === 0 ? 0 : (width - 2*padX) / maxLayer;

  const pos = new Map();
  for (const lv of layerKeys) {
    const names = layers.get(lv);
    const dy = (height - 2*padY) / Math.max(1, names.length);
    names.forEach((name, i) => {
      pos.set(name, { x: padX + lv * dx, y: padY + (i + 0.5) * dy });
    });
  }

  let svg = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">`;

  // edges
  for (const [u, v] of edges) {
    const pu = pos.get(u), pv = pos.get(v);
    if (!pu || !pv) continue;
    const mx = (pu.x + pv.x) / 2;
    svg += `<path class="edge" d="M ${pu.x} ${pu.y} C ${mx} ${pu.y}, ${mx} ${pv.y}, ${pv.x} ${pv.y}" />`;
  }

  // nodes
  for (const n of nodes) {
    const p = pos.get(n.task_name) || {x: padX, y: padY};
    svg += `
      <g data-task="${n.task_name}" data-taskrun="${n.task_run_id || ""}">
        <rect class="${nodeClass(n.state)}" x="${p.x-60}" y="${p.y-16}" width="120" height="32" rx="10"></rect>
        <text class="nodeText" x="${p.x}" y="${p.y+4}" text-anchor="middle">${n.task_name}</text>
      </g>
    `;
  }

  svg += `</svg>`;
  container.innerHTML = svg;

  // click handlers for nodes
  container.querySelectorAll("g[data-taskrun]").forEach(g => {
    g.addEventListener("click", async () => {
      const trId = g.getAttribute("data-taskrun");
      if (!trId) return;
      await showTaskRun(parseInt(trId, 10));
    });
  });
}

async function selectRun(runId) {
  const meta = document.getElementById("runMeta");
  meta.textContent = `Loading run ${runId}...`;

  const detail = await jget(`/api/runs/${runId}`);
  meta.innerHTML = `
    <div><b>Run ${detail.run.id}</b> — ${detail.run.flow_name}:${detail.run.flow_version} — ${pill(detail.run.state)}</div>
    <div class="muted">Created: ${detail.run.created_at_utc || ""}</div>
  `;

  const graph = await jget(`/api/runs/${runId}/graph`);
  renderGraph(document.getElementById("graph"), graph);

  const d = document.getElementById("detail");
  d.innerHTML = `<div class="muted">Click a node to see attempts, logs, and output preview.</div>`;
}

async function showTaskRun(taskRunId) {
  const d = document.getElementById("detail");
  d.innerHTML = `<div class="muted">Loading task run ${taskRunId}...</div>`;

  const data = await jget(`/api/task-runs/${taskRunId}`);
  const tr = data.task_run;

  let html = `<h3 style="margin:10px 0 6px 0">${tr.task_name} — ${pill(tr.state)}</h3>`;
  if (tr.last_error_json) {
    html += `<pre style="white-space:pre-wrap;border:1px solid var(--border);padding:10px;border-radius:10px;">${tr.last_error_json}</pre>`;
  }

  html += `<h4>Attempts</h4>`;
  for (const a of data.attempts) {
    html += `
      <div style="border:1px solid var(--border);padding:10px;border-radius:10px;margin-bottom:8px;">
        <div><b>Attempt ${a.attempt_number}</b> — ${pill(a.state)}</div>
        <div class="muted">Started: ${a.started_at_utc || ""}</div>
        <button data-attempt="${a.id}" style="margin-top:8px;">Tail logs</button>
        <pre id="logs-${a.id}" style="white-space:pre-wrap;margin-top:8px;"></pre>
      </div>
    `;
  }

  if (data.artifact) {
    html += `<h4>Output preview</h4>`;
    html += `<pre style="white-space:pre-wrap;border:1px solid var(--border);padding:10px;border-radius:10px;">${data.artifact.text}</pre>`;
  }

  d.innerHTML = html;

  d.querySelectorAll("button[data-attempt]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const id = btn.getAttribute("data-attempt");
      const logs = await jget(`/api/attempts/${id}/logs?tail=200`);
      document.getElementById(`logs-${id}`).textContent = logs.lines.join("\n");
    });
  });
}

function startSSE() {
  const es = new EventSource("/api/events/stream?since=0");
  es.addEventListener("event", (msg) => {
    // For MVP: simply refresh runs list occasionally.
    // You can evolve this to incremental updates by inspecting msg.data.
  });
}

(async function boot() {
  await loadRuns();
  setInterval(loadRuns, 2000);
  startSSE();
})();