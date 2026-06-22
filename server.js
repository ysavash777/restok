const express = require('express');
const fs      = require('fs');
const path    = require('path');

const app     = express();
const PORT    = 3000;
const DATA    = path.join(__dirname, 'public', 'data');

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── SSE clients pool ─────────────────────────────────────────────────────────
// Map: projectId → Set of res objects
const clients = new Map();

function broadcast(projectId, payload) {
  const pool = clients.get(projectId);
  if (!pool) return;
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of pool) {
    try { res.write(msg); } catch (_) {}
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function projectFile(id) {
  return path.join(DATA, `${id}.json`);
}
function loadProject(id) {
  try { return JSON.parse(fs.readFileSync(projectFile(id), 'utf8')); }
  catch (_) { return null; }
}
function saveProject(id, data) {
  fs.writeFileSync(projectFile(id), JSON.stringify(data, null, 2), 'utf8');
}
function listProjects() {
  if (!fs.existsSync(DATA)) return [];
  return fs.readdirSync(DATA)
    .filter(f => f.endsWith('.json'))
    .map(f => {
      try { return JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8')); }
      catch (_) { return null; }
    })
    .filter(Boolean)
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
}

// ── SSE endpoint ─────────────────────────────────────────────────────────────
app.get('/api/projects/:id/stream', (req, res) => {
  const { id } = req.params;
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  if (!clients.has(id)) clients.set(id, new Set());
  clients.get(id).add(res);

  // Send current state immediately on connect
  const project = loadProject(id);
  if (project) res.write(`data: ${JSON.stringify({ type: 'sync', project })}\n\n`);

  // Keepalive every 25s
  const ka = setInterval(() => { try { res.write(': ka\n\n'); } catch (_) {} }, 25000);

  req.on('close', () => {
    clearInterval(ka);
    const pool = clients.get(id);
    if (pool) { pool.delete(res); if (!pool.size) clients.delete(id); }
  });
});

// ── Projects CRUD ─────────────────────────────────────────────────────────────
app.get('/api/projects', (_req, res) => {
  res.json(listProjects());
});

app.post('/api/projects', (req, res) => {
  const { id, name, createdBy } = req.body;
  if (!id || !name) return res.status(400).json({ error: 'id y name requeridos' });
  if (loadProject(id)) return res.status(409).json({ error: 'Proyecto ya existe' });

  const project = {
    id,
    name,
    createdBy: createdBy || 'Sistema',
    createdAt: new Date().toISOString(),
    entries:   []
  };
  saveProject(id, project);
  res.json(project);
});

app.delete('/api/projects/:id', (req, res) => {
  const { id } = req.params;
  const file = projectFile(id);
  if (!fs.existsSync(file)) return res.status(404).json({ error: 'No existe' });
  fs.unlinkSync(file);
  res.json({ ok: true });
});

// ── Entries ───────────────────────────────────────────────────────────────────
// Add or accumulate entry
app.post('/api/projects/:id/entries', (req, res) => {
  const project = loadProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Proyecto no encontrado' });

  const entry = req.body; // { id, ref, desc, tipo, subtipo, fechaVenc, comentario, qty, ts, user }

  // Accumulate if same key exists
  const key = [entry.ref, entry.tipo, entry.subtipo||'', entry.fechaVenc||'', (entry.comentario||'').trim().toLowerCase()].join('|');
  const existing = project.entries.find(e =>
    [e.ref, e.tipo, e.subtipo||'', e.fechaVenc||'', (e.comentario||'').trim().toLowerCase()].join('|') === key
  );

  if (existing) {
    existing.qty += entry.qty;
    existing.lastUser = entry.user;
    existing.lastTs   = entry.ts;
  } else {
    project.entries.unshift(entry);
  }

  saveProject(project.id, project);
  broadcast(project.id, { type: 'entries', entries: project.entries });
  res.json({ ok: true, entries: project.entries });
});

// Edit entry
app.put('/api/projects/:id/entries/:entryId', (req, res) => {
  const project = loadProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Proyecto no encontrado' });

  const idx = project.entries.findIndex(e => e.id === req.params.entryId);
  if (idx === -1) return res.status(404).json({ error: 'Entry no encontrada' });

  const updated = { ...project.entries[idx], ...req.body };

  // Check collision after edit
  const key = [updated.ref, updated.tipo, updated.subtipo||'', updated.fechaVenc||'', (updated.comentario||'').trim().toLowerCase()].join('|');
  const collision = project.entries.find((e, i) =>
    i !== idx &&
    [e.ref, e.tipo, e.subtipo||'', e.fechaVenc||'', (e.comentario||'').trim().toLowerCase()].join('|') === key
  );

  if (collision) {
    collision.qty += updated.qty;
    project.entries.splice(idx, 1);
  } else {
    project.entries[idx] = updated;
  }

  saveProject(project.id, project);
  broadcast(project.id, { type: 'entries', entries: project.entries });
  res.json({ ok: true, entries: project.entries });
});

// Delete entry
app.delete('/api/projects/:id/entries/:entryId', (req, res) => {
  const project = loadProject(req.params.id);
  if (!project) return res.status(404).json({ error: 'Proyecto no encontrado' });

  project.entries = project.entries.filter(e => e.id !== req.params.entryId);
  saveProject(project.id, project);
  broadcast(project.id, { type: 'entries', entries: project.entries });
  res.json({ ok: true });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
if (!fs.existsSync(DATA)) fs.mkdirSync(DATA, { recursive: true });

app.listen(PORT, () => {
  console.log(`\n  GDSMapiX corriendo en http://localhost:${PORT}\n`);
});
