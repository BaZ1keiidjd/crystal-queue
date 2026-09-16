/* ============================================================
   Crystal Queue — мобильная очередь задач для ПК
   Хранилище: приватный GitHub Gist (queue.json + runner.json)
   Телефон пишет задачи, раннер на ПК забирает и выполняет.
   ============================================================ */
'use strict';

/* ------------------------------ const ------------------------------ */

const GH_API = 'https://api.github.com';
const GIST_DESC = 'Crystal Queue — очередь задач для ПК';
const QUEUE_FILE = 'queue.json';
const RUNNER_FILE = 'runner.json';
const SCHEMA = 1;
const RUNNER_TTL_MS = 45_000;       // heartbeat старше этого — раннер считается отключённым
const TOMBSTONE_TTL_MS = 7 * 864e5;  // следы удалённых задач

const STATUSES = ['new', 'running', 'done', 'failed'];
const STATUS_LABEL = { new: 'в очереди', running: 'выполняется', done: 'готово', failed: 'ошибка', draft: 'черновик' };
const PRIO_WEIGHT = { high: 0, normal: 1, low: 2 };

/** Поля, которые пишет раннер — телефон их не перезаписывает. */
const RUNNER_FIELDS = ['status', 'startedAt', 'finishedAt', 'result', 'error',
                       'claimedBy', 'log', 'host', 'session'];

const LS = {
  settings: 'crystal-queue:settings',
  tasks: 'crystal-queue:tasks',
  drafts: 'crystal-queue:drafts',
  tombstones: 'crystal-queue:tombstones',
  theme: 'crystal-queue:theme',
  seen: 'crystal-queue:seen'
};

const DEFAULT_SETTINGS = {
  token: '',
  gistId: '',
  rememberToken: true,
  pollInterval: 20,
  notifyDone: true,
  confirmDelete: true,
  defaultProject: 'crystal',
  filter: 'active',
  search: ''
};

/* ------------------------------ helpers ------------------------------ */

const $ = (id) => document.getElementById(id);
const uid = (p = 't') => p + '_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g,
  (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const readLS = (key, fallback) => {
  try { const r = localStorage.getItem(key); return r ? JSON.parse(r) : fallback; }
  catch { return fallback; }
};
const writeLS = (key, value) => {
  try { localStorage.setItem(key, JSON.stringify(value)); return true; }
  catch { return false; }
};

function fmtAgo(iso) {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  if (!isFinite(diff)) return '';
  const abs = Math.abs(diff);
  const suffix = diff >= 0 ? 'назад' : 'вперёд';
  if (abs < 45_000) return diff >= 0 ? 'только что' : 'сейчас';
  if (abs < 36e5) return `${Math.round(abs / 6e4)} мин ${suffix}`;
  if (abs < 864e5) return `${Math.round(abs / 36e5)} ч ${suffix}`;
  if (abs < 7 * 864e5) return `${Math.round(abs / 864e5)} дн ${suffix}`;
  return new Date(iso).toLocaleDateString('ru-RU');
}

const fmtFull = (iso) => iso ? new Date(iso).toLocaleString('ru-RU') : '—';

const fmtDur = (ms) => {
  if (!ms && ms !== 0) return '—';
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s} с`;
  return `${Math.floor(s / 60)} мин ${s % 60} с`;
};

function toast(text, kind = '') {
  const t = document.createElement('div');
  t.className = `toast ${kind}`;
  t.textContent = text;
  $('toasts').appendChild(t);
  setTimeout(() => { t.classList.add('out'); setTimeout(() => t.remove(), 220); }, 3400);
}

function notify(title, body) {
  if (!('Notification' in window) || Notification.permission !== 'granted') return;
  try { new Notification(title, { body, icon: 'favicon.svg' }); } catch { /* не критично */ }
}

async function copyText(text) {
  try { await navigator.clipboard.writeText(text); toast('Скопировано', 'ok'); }
  catch {
    const ta = document.createElement('textarea');
    ta.value = text; ta.style.cssText = 'position:fixed;opacity:0';
    document.body.appendChild(ta); ta.select();
    try { document.execCommand('copy'); toast('Скопировано', 'ok'); }
    catch { toast('Не удалось скопировать', 'err'); }
    ta.remove();
  }
}

/* ------------------------------ state ------------------------------ */

let settings = { ...DEFAULT_SETTINGS, ...readLS(LS.settings, {}) };
if (!settings.rememberToken) settings.token = '';

let tasks = readLS(LS.tasks, []);
let drafts = readLS(LS.drafts, []);
let tombstones = readLS(LS.tombstones, []);
let seenStatus = readLS(LS.seen, {});

let runnerState = null;   // содержимое runner.json
let lastSyncAt = null;
let syncing = false;
let pollTimer = null;
let pendingDraftText = '';

/* ------------------------------ github gist api ------------------------------ */

class ApiError extends Error {
  constructor(message, status) { super(message); this.status = status; }
}

async function gh(path, opts = {}) {
  const res = await fetch(GH_API + path, {
    ...opts,
    headers: {
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'Authorization': `Bearer ${settings.token}`,
      ...(opts.body ? { 'Content-Type': 'application/json' } : {})
    }
  });

  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const j = await res.json();
      if (j.message) msg = j.message;
      if (j.errors?.length) msg += ` (${j.errors.map(e => e.message || e.code).join('; ')})`;
    } catch { /* тело не JSON */ }
    throw new ApiError(humanizeGhError(res.status, msg), res.status);
  }
  if (res.status === 204) return null;
  return res.json();
}

function humanizeGhError(status, msg) {
  if (status === 401) return 'Токен неверный или отозван (401). Перевыпусти PAT с правом gist.';
  if (status === 403) return /rate limit/i.test(msg)
    ? 'Упёрлись в лимит запросов GitHub. Увеличьте интервал автообновления.'
    : `Токену не хватает прав (403): ${msg}`;
  if (status === 404) return `Gist не найден (404): ${msg}. Проверьте ID или очистите поле — создам новый.`;
  return `${status}: ${msg}`;
}

async function findGistByDesc() {
  const list = await gh('/gists?per_page=100');
  const hit = (Array.isArray(list) ? list : []).find(g => g.description === GIST_DESC);
  return hit?.id || null;
}

async function ensureGist() {
  if (!settings.token) throw new ApiError('Не задан GitHub-токен', 0);

  if (settings.gistId) {
    try { await gh(`/gists/${settings.gistId}`); return settings.gistId; }
    catch (e) {
      if (e.status !== 404) throw e;
      toast('Указанный gist не найден — ищу или создаю новый', 'err');
      settings.gistId = '';
    }
  }

  const found = await findGistByDesc();
  if (found) { settings.gistId = found; persistSettings(); return found; }

  const created = await gh('/gists', {
    method: 'POST',
    body: JSON.stringify({
      description: GIST_DESC,
      public: false,
      files: {
        [QUEUE_FILE]: { content: JSON.stringify({ schema: SCHEMA, tasks: [] }, null, 2) },
        [RUNNER_FILE]: { content: JSON.stringify({ schema: SCHEMA, state: 'never' }, null, 2) }
      }
    })
  });
  settings.gistId = created.id;
  persistSettings();
  return created.id;
}

function fileContent(gist, name) {
  const f = gist?.files?.[name];
  if (!f) return null;
  // большие файлы GitHub отдаёт через truncated + raw_url
  const raw = f.content ?? null;
  return raw;
}

async function readGist() {
  const id = await ensureGist();
  const gist = await gh(`/gists/${id}`);

  let queueRaw = fileContent(gist, QUEUE_FILE);
  if (queueRaw == null && gist.files?.[QUEUE_FILE]?.raw_url) {
    queueRaw = await (await fetch(gist.files[QUEUE_FILE].raw_url)).text();
  }
  let runnerRaw = fileContent(gist, RUNNER_FILE);
  if (runnerRaw == null && gist.files?.[RUNNER_FILE]?.raw_url) {
    runnerRaw = await (await fetch(gist.files[RUNNER_FILE].raw_url)).text();
  }

  const parse = (raw, fallback) => {
    if (raw == null || raw === '') return fallback;
    try { return JSON.parse(raw); } catch { return fallback; }
  };

  return {
    id,
    queue: parse(queueRaw, { schema: SCHEMA, tasks: [] }),
    runner: parse(runnerRaw, null),
    updatedAt: gist.updated_at
  };
}

async function writeQueue(nextTasks) {
  const id = await ensureGist();
  await gh(`/gists/${id}`, {
    method: 'PATCH',
    body: JSON.stringify({
      files: {
        [QUEUE_FILE]: {
          content: JSON.stringify({ schema: SCHEMA, tasks: nextTasks }, null, 2)
        }
      }
    })
  });
}

/* ------------------------------ merge ------------------------------ */

function liveTombstones() {
  const cutoff = Date.now() - TOMBSTONE_TTL_MS;
  return tombstones.filter(t => new Date(t.at).getTime() > cutoff);
}

/**
 * Слияние локального и удалённого списков.
 *  - задачи с _dirty (правил телефон) — локальная версия побеждает,
 *    но «running» у раннера не затирается;
 *  - остальные — берём раннер-поля из remote;
 *  - удалённые локально (tombstone) — не воскрешаем;
 *  - появившиеся только в remote — добавляем.
 */
function mergeTasks(remoteTasks, localTasks) {
  const dead = new Set(liveTombstones().map(t => t.id));
  const validRemote = (remoteTasks || []).filter(t => t && t.id);
  const remote = validRemote.filter(t => !dead.has(t.id));
  // если tombstone вычеркнул задачу, которая ещё лежит в облаке — надо запушить удаление
  const pruned = validRemote.length - remote.length;
  const byId = new Map(remote.map(t => [t.id, t]));
  const out = [];
  const seen = new Set();
  let dirtyLeft = 0;

  for (const lt of localTasks) {
    if (!lt?.id || dead.has(lt.id)) continue;
    seen.add(lt.id);
    const rt = byId.get(lt.id);

    if (!rt) { if (lt._dirty) dirtyLeft++; out.push(lt); continue; }

    if (lt._dirty) {
      // раннер сейчас выполняет эту задачу — не сбрасываем его статус
      if (rt.status === 'running' && lt.status !== 'running') {
        out.push({ ...lt, ...pick(rt, RUNNER_FIELDS), _dirty: false });
      } else {
        dirtyLeft++;
        out.push(lt);
      }
    } else {
      out.push({ ...lt, ...pick(rt, RUNNER_FIELDS) });
    }
  }

  for (const rt of remote) if (!seen.has(rt.id)) out.push(rt);

  return { tasks: out, dirty: dirtyLeft > 0 || pruned > 0 };
}

const pick = (obj, keys) => keys.reduce((a, k) => (obj?.[k] !== undefined ? (a[k] = obj[k], a) : a), {});

/* ------------------------------ sync ------------------------------ */

async function sync({ silent = false } = {}) {
  if (syncing) return false;
  if (!settings.token) { setSync('warn', 'нет токена'); renderRunner(); return false; }

  syncing = true;
  setSync('busy', 'синхронизация');
  const spin = $('refreshBtn')?.querySelector('svg');
  spin?.classList.add('spin');

  try {
    const remote = await readGist();
    runnerState = remote.runner;

    const { tasks: merged, dirty } = mergeTasks(remote.queue.tasks || [], tasks);
    tasks = sortTasks(merged);
    persistTasks();

    if (dirty) {
      await writeQueue(tasks.map(stripLocal));
      tasks.forEach(t => delete t._dirty);
      persistTasks();
    }

    dropStaleTombstones(remote.queue.tasks || []);
    lastSyncAt = Date.now();
    setSync('ok', navigator.onLine ? 'синхронизировано' : 'офлайн-копия');
    detectFinished();
    renderAll();
    return true;
  } catch (e) {
    setSync('err', 'ошибка связи');
    if (!silent) toast(e.message, 'err');
    $('diag')?.setAttribute('data-last-error', e.message);
    renderAll();
    return false;
  } finally {
    syncing = false;
    spin?.classList.remove('spin');
  }
}

const stripLocal = (t) => { const { _dirty, ...rest } = t; return rest; };

function dropStaleTombstones(remoteTasks) {
  const remoteIds = new Set(remoteTasks.map(t => t.id));
  const before = tombstones.length;
  tombstones = liveTombstones().filter(t => remoteIds.has(t.id));
  if (tombstones.length !== before) writeLS(LS.tombstones, tombstones);
}

function setSync(state, text) {
  const s = $('syncState');
  s.dataset.state = state;
  s.querySelector('.sync-text').textContent = text;
}

/** Заметить переходы в done/failed и показать уведомление. */
function detectFinished() {
  if (!settings.notifyDone) { rememberStatuses(); return; }
  for (const t of tasks) {
    const prev = seenStatus[t.id];
    if (prev && prev !== t.status && (t.status === 'done' || t.status === 'failed')) {
      const title = t.status === 'done' ? 'Задача выполнена' : 'Задача упала';
      const body = (t.text || '').slice(0, 90);
      toast(`${title}: ${body}`, t.status === 'done' ? 'ok' : 'err');
      notify(title, body);
    }
  }
  rememberStatuses();
}

function rememberStatuses() {
  seenStatus = {};
  for (const t of tasks) seenStatus[t.id] = t.status;
  writeLS(LS.seen, seenStatus);
}

/* ------------------------------ persistence ------------------------------ */

function persistTasks() {
  writeLS(LS.tasks, tasks);
  writeLS(LS.drafts, drafts);
}

function persistSettings() {
  const s = { ...settings };
  if (!s.rememberToken) delete s.token;
  writeLS(LS.settings, s);
}

/* ------------------------------ sorting / filtering ------------------------------ */

function sortTasks(list) {
  const active = list.filter(t => t.status === 'new' || t.status === 'running');
  const finished = list.filter(t => t.status === 'done' || t.status === 'failed');
  active.sort((a, b) => {
    if (a.status !== b.status) return a.status === 'running' ? -1 : 1;
    const p = (PRIO_WEIGHT[a.priority] ?? 1) - (PRIO_WEIGHT[b.priority] ?? 1);
    if (p) return p;
    return new Date(a.createdAt) - new Date(b.createdAt);
  });
  finished.sort((a, b) => new Date(b.finishedAt || b.updatedAt || 0) - new Date(a.finishedAt || a.updatedAt || 0));
  return [...active, ...finished];
}

function visibleTasks() {
  const q = settings.search.trim().toLowerCase();
  const f = settings.filter;
  return tasks.filter(t => {
    if (f === 'active' && (t.status === 'done' || t.status === 'failed')) return false;
    if (f === 'done' && t.status !== 'done') return false;
    if (f === 'failed' && t.status !== 'failed') return false;
    if (q && !(`${t.text} ${t.project || ''}`.toLowerCase().includes(q))) return false;
    return true;
  });
}

/* ------------------------------ render ------------------------------ */

function renderAll() { renderStats(); renderRunner(); renderList(); renderProjects(); renderFoot(); }

function renderStats() {
  $('stNew').textContent = tasks.filter(t => t.status === 'new').length + drafts.length;
  $('stRunning').textContent = tasks.filter(t => t.status === 'running').length;
  $('stDone').textContent = tasks.filter(t => t.status === 'done').length;
  $('stFailed').textContent = tasks.filter(t => t.status === 'failed').length;
}

function runnerIsLive() {
  const ts = runnerState?.lastSeen;
  if (!ts) return false;
  return Date.now() - new Date(ts).getTime() < RUNNER_TTL_MS;
}

function renderRunner() {
  const card = $('runnerCard');
  const r = runnerState;
  const live = runnerIsLive();

  let state = 'stale', title = 'Раннер не на связи', sub = 'ПК выключен или раннер не запущен — задачи копятся и ждут';

  if (r && live) {
    if (r.state === 'working') {
      state = 'busy';
      title = 'Раннер выполняет задачу';
      sub = `${r.host || 'ПК'} · с ${fmtAgo(r.currentSince)} · opencode ${r.opencodeVersion || '?'}`;
    } else if (r.state === 'error') {
      state = 'stale';
      title = 'Раннер на связи, но с ошибкой';
      sub = (r.lastError || 'неизвестная ошибка').slice(0, 110);
    } else {
      state = 'online';
      title = 'Раннер на связи';
      sub = `${r.host || 'ПК'} · heartbeat ${fmtAgo(r.lastSeen)} · сделано ${r.stats?.done ?? 0}, ошибок ${r.stats?.failed ?? 0}`;
    }
  } else if (r && !live) {
    title = 'Раннер был на связи';
    sub = `последний heartbeat ${fmtAgo(r.lastSeen)} — ПК выключен или раннер остановлен`;
  } else if (!settings.token) {
    title = 'Подключение не настроено';
    sub = 'Открой настройки и вставь GitHub-токен';
  }

  card.dataset.state = state;
  $('runnerTitle').textContent = title;
  $('runnerSub').textContent = sub;

  const log = $('runnerLog');
  const lines = r?.logTail?.length ? r.logTail : null;
  log.hidden = !lines;
  if (lines) log.textContent = lines.join('\n');
}

function renderProjects() {
  const set = new Set(tasks.map(t => t.project).filter(Boolean));
  if (settings.defaultProject) set.add(settings.defaultProject);
  $('projectList').replaceChildren(...[...set].sort().map(p => {
    const o = document.createElement('option'); o.value = p; return o;
  }));
}

function taskCard(t) {
  const isDraft = t.status === 'draft';
  const wrap = document.createElement('article');
  wrap.className = `task ${isDraft ? 'draft' : t.status}`;
  wrap.dataset.id = t.id;
  wrap.tabIndex = 0;
  wrap.setAttribute('role', 'button');

  const prio = t.priority === 'high' ? 'высокий' : t.priority === 'low' ? 'низкий' : 'обычный';
  const time = isDraft ? 'черновик' : fmtAgo(t.status === 'new' ? t.createdAt : (t.finishedAt || t.updatedAt || t.createdAt));

  wrap.innerHTML = `
    <div class="task-top">
      <span class="task-status ${isDraft ? 'st-draft' : 'st-' + t.status}">
        <span class="sdot"></span>${esc(STATUS_LABEL[isDraft ? 'draft' : t.status])}
      </span>
      <span class="task-prio ${esc(t.priority || 'normal')}">${prio}</span>
      ${t.attempts > 1 ? `<span class="task-prio">попытка ${t.attempts}</span>` : ''}
      <span class="task-time">${esc(time)}</span>
    </div>
    <div class="task-text">${esc(t.text)}</div>
    <div class="task-foot">
      ${t.project ? `<span class="task-proj">${esc(t.project)}</span>` : ''}
      ${t.host ? `<span class="task-prio">${esc(t.host)}</span>` : ''}
      <span class="task-quick">
        ${isDraft
          ? '<button data-act="send">отправить</button>'
          : (t.status === 'failed' || t.status === 'done')
            ? '<button data-act="retry">повторить</button>'
            : (t.status === 'new' ? '<button data-act="top">в начало</button>' : '')}
        <button data-act="delete" class="del">удалить</button>
      </span>
    </div>`;

  wrap.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-act]');
    if (btn) { e.stopPropagation(); handleAction(btn.dataset.act, t.id); return; }
    openTaskSheet(t.id);
  });
  wrap.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openTaskSheet(t.id); }
  });
  return wrap;
}

function renderList() {
  const list = $('taskList');
  const frag = document.createDocumentFragment();

  const q = settings.search.trim().toLowerCase();
  const shownDrafts = drafts.filter(d => !q || d.text.toLowerCase().includes(q));
  const showDrafts = settings.filter !== 'done' && settings.filter !== 'failed';

  if (showDrafts) shownDrafts.forEach(d => frag.appendChild(taskCard({ ...d, status: 'draft' })));
  visibleTasks().forEach(t => frag.appendChild(taskCard(t)));

  if (!frag.childNodes.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.innerHTML = `<svg viewBox="0 0 24 24"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg>
      <p>${tasks.length || drafts.length ? 'Под этот фильтр ничего не подходит' : 'Очередь пуста — добавь первую задачу выше'}</p>`;
    frag.appendChild(empty);
  }

  list.replaceChildren(frag);
}

function renderFoot() {
  const parts = [];
  if (settings.gistId) parts.push(`gist ${settings.gistId.slice(0, 8)}…`);
  parts.push(`${tasks.length} задач в облаке`);
  if (drafts.length) parts.push(`${drafts.length} черновик(ов) локально`);
  const dirty = tasks.filter(t => t._dirty).length;
  if (dirty) parts.push(`${dirty} ждут отправки`);
  parts.push(lastSyncAt ? `обновлено ${fmtAgo(new Date(lastSyncAt).toISOString())}` : 'ещё не синхронизировано');
  if (!navigator.onLine) parts.push('нет сети — работаю локально');
  $('footNote').textContent = parts.join(' · ');
}

/* ------------------------------ actions ------------------------------ */

function makeTask(text, priority, project) {
  return {
    id: uid(),
    text: text.trim(),
    priority: priority || 'normal',
    project: project || settings.defaultProject || '',
    status: 'new',
    attempts: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    createdBy: 'phone',
    _dirty: true
  };
}

async function addTask() {
  const ta = $('taskText');
  const text = ta.value.trim();
  if (!text) { ta.focus(); setHint('Напиши, что нужно сделать', 'err'); return; }
  if (text.length > 8000) { setHint('Слишком длинно — максимум 8000 символов', 'err'); return; }

  const priority = $('prioritySeg').querySelector('.on')?.dataset.priority || 'normal';
  const project = $('taskProject').value.trim() || settings.defaultProject || '';
  const isDraft = $('taskDraft').checked;

  if (isDraft) {
    drafts.unshift({ ...makeTask(text, priority, project), status: 'draft' });
    writeLS(LS.drafts, drafts);
    ta.value = ''; autoGrow(ta);
    setHint('Сохранено как черновик — только на этом телефоне', 'ok');
    renderAll();
    return;
  }

  const task = makeTask(text, priority, project);
  tasks.unshift(task);
  tasks = sortTasks(tasks);
  persistTasks();
  ta.value = ''; autoGrow(ta);
  renderAll();
  setHint('Добавлено. Синхронизирую с gist…', '');

  const ok = await sync({ silent: true });
  setHint(ok
    ? 'Задача в облаке — ПК подхватит её, как только раннер запустится'
    : 'Сохранено локально: нет связи или токена. Отправится автоматически, как только сможет',
    ok ? 'ok' : 'err');
}

function setHint(text, kind) {
  const h = $('addHint');
  h.textContent = text;
  h.className = 'hint' + (kind ? ' ' + kind : '');
}

async function handleAction(act, id) {
  const isDraft = drafts.some(d => d.id === id);

  if (act === 'delete') {
    const label = (isDraft ? drafts.find(d => d.id === id) : tasks.find(t => t.id === id))?.text || '';
    if (settings.confirmDelete && !confirm(`Удалить задачу?\n\n${label.slice(0, 120)}`)) return;
    if (isDraft) {
      drafts = drafts.filter(d => d.id !== id);
      writeLS(LS.drafts, drafts);
      renderAll(); toast('Черновик удалён');
      return;
    }
    tasks = tasks.filter(t => t.id !== id);
    tombstones.push({ id, at: new Date().toISOString() });
    writeLS(LS.tombstones, tombstones);
    persistTasks(); renderAll();
    toast('Удалено');
    await sync({ silent: true });
    return;
  }

  if (act === 'send') {
    const idx = drafts.findIndex(d => d.id === id);
    if (idx < 0) return;
    const [d] = drafts.splice(idx, 1);
    writeLS(LS.drafts, drafts);
    const t = { ...d, status: 'new', createdAt: new Date().toISOString(), _dirty: true };
    delete t.status_draft;
    tasks.unshift(t);
    tasks = sortTasks(tasks);
    persistTasks(); renderAll();
    toast('Черновик отправлен в очередь');
    await sync({ silent: true });
    return;
  }

  if (act === 'retry') {
    const t = tasks.find(x => x.id === id);
    if (!t) return;
    t.status = 'new';
    t.attempts = (t.attempts || 0) + 1;
    t.error = undefined; t.result = undefined; t.finishedAt = undefined;
    t.updatedAt = new Date().toISOString();
    t._dirty = true;
    tasks = sortTasks(tasks);
    persistTasks(); renderAll();
    toast('Задача снова в очереди');
    await sync({ silent: true });
    return;
  }

  if (act === 'top') {
    const t = tasks.find(x => x.id === id);
    if (!t) return;
    t.priority = 'high';
    t.updatedAt = new Date().toISOString();
    t._dirty = true;
    tasks = sortTasks(tasks);
    persistTasks(); renderAll();
    toast('Приоритет поднят до высокого');
    await sync({ silent: true });
  }
}

/* ------------------------------ task sheet ------------------------------ */

let currentTaskId = null;

function openTaskSheet(id) {
  const t = tasks.find(x => x.id === id) || drafts.find(d => d.id === id);
  if (!t) return;
  currentTaskId = id;

  $('tsTitle').textContent = STATUS_LABEL[t.status === 'draft' ? 'draft' : t.status];
  const res = t.result;
  const log = t.log || res?.log || '';

  $('tsBody').innerHTML = `
    <div class="ts-block">
      <span class="ts-label">Задача</span>
      <div class="ts-text">${esc(t.text)}</div>
    </div>
    <div class="ts-block">
      <span class="ts-label">Детали</span>
      <div class="ts-meta">
        <span>id <b>${esc(t.id)}</b></span>
        <span>приоритет <b>${esc(t.priority || 'normal')}</b></span>
        ${t.project ? `<span>проект <b>${esc(t.project)}</b></span>` : ''}
        <span>попыток <b>${t.attempts || 0}</b></span>
      </div>
      <div class="ts-meta">
        <span>создана <b>${fmtFull(t.createdAt)}</b></span>
        ${t.startedAt ? `<span>начата <b>${fmtFull(t.startedAt)}</b></span>` : ''}
        ${t.finishedAt ? `<span>завершена <b>${fmtFull(t.finishedAt)}</b></span>` : ''}
      </div>
      ${t.claimedBy ? `<div class="ts-meta"><span>исполнитель <b>${esc(t.claimedBy)}</b></span></div>` : ''}
      ${res?.durationMs ? `<div class="ts-meta"><span>длительность <b>${fmtDur(res.durationMs)}</b></span></div>` : ''}
    </div>
    ${t.error ? `<div class="ts-block"><span class="ts-label">Ошибка</span>
      <div class="ts-result err">${esc(t.error)}</div></div>` : ''}
    ${res?.summary ? `<div class="ts-block"><span class="ts-label">Итог от opencode</span>
      <div class="ts-result ok">${esc(res.summary)}</div></div>` : ''}
    ${res?.files?.length ? `<div class="ts-block"><span class="ts-label">Изменённые файлы</span>
      <div class="ts-result">${res.files.map(esc).join('\n')}</div></div>` : ''}
    ${log ? `<div class="ts-block"><span class="ts-label">Лог (хвост)</span>
      <div class="ts-result">${esc(log.slice(-6000))}</div></div>` : ''}
    ${t.status === 'draft' ? '<p class="hint">Это черновик: он живёт только в этом браузере и не виден ПК.</p>' : ''}`;

  $('tsRetry').hidden = t.status !== 'done' && t.status !== 'failed';
  $('tsCopy').hidden = false;
  openSheet('taskSheet', 'scrim2');
}

/* ------------------------------ sheets ------------------------------ */

let openScrim = null;

function openSheet(id, scrimId) {
  closeSheet();
  $(id).classList.add('open');
  $(id).setAttribute('aria-hidden', 'false');
  $(scrimId).hidden = false;
  openScrim = scrimId;
  document.body.style.overflow = 'hidden';
}

function closeSheet() {
  ['sheet', 'taskSheet'].forEach(id => {
    $(id).classList.remove('open');
    $(id).setAttribute('aria-hidden', 'true');
  });
  if (openScrim) { $(openScrim).hidden = true; openScrim = null; }
  document.body.style.overflow = '';
}

function openSettings() {
  $('ghToken').value = settings.token || '';
  $('gistId').value = settings.gistId || '';
  $('pollInterval').value = settings.pollInterval;
  $('rememberToken').checked = !!settings.rememberToken;
  $('notifyDone').checked = settings.notifyDone !== false;
  $('confirmDelete').checked = settings.confirmDelete !== false;
  $('diag').hidden = true;
  openSheet('sheet', 'scrim');
}

async function saveSettings() {
  settings.token = $('ghToken').value.trim();
  settings.gistId = $('gistId').value.trim().replace(/^.*\/([a-f0-9]{20,})$/i, '$1');
  settings.pollInterval = Math.min(600, Math.max(5, Number($('pollInterval').value) || 20));
  settings.rememberToken = $('rememberToken').checked;
  settings.notifyDone = $('notifyDone').checked;
  settings.confirmDelete = $('confirmDelete').checked;

  if (settings.notifyDone && 'Notification' in window && Notification.permission === 'default') {
    try { await Notification.requestPermission(); } catch { /* игнорируем */ }
  }

  persistSettings();
  $('saveNote').textContent = 'Сохранено';
  $('saveNote').classList.add('show');
  setTimeout(() => $('saveNote').classList.remove('show'), 1600);

  restartPolling();
  const ok = await sync();
  if (ok) { closeSheet(); toast('Подключено к gist', 'ok'); }
}

async function runDiagnostics() {
  const box = $('diag');
  box.hidden = false;
  box.textContent = 'Проверяю…';
  const lines = [];
  const mark = (okFlag, text) => lines.push(`${okFlag ? '[ok]' : '[!!]'} ${text}`);

  try {
    mark(!!settings.token, settings.token ? 'токен задан' : 'токен НЕ задан');
    if (!settings.token) throw new Error('нет токена — дальше некуда');

    const me = await gh('/user').catch(() => null);
    mark(!!me, me ? `токен валиден, пользователь: ${me.login}` : 'не удалось прочитать профиль (возможно, токен только с правом gist)');

    const id = await ensureGist();
    mark(!!id, `gist: ${id}`);

    const g = await readGist();
    mark(Array.isArray(g.queue.tasks), `queue.json прочитан, задач: ${g.queue?.tasks?.length ?? 0}`);
    mark(!!g.runner, g.runner ? `runner.json прочитан, состояние: ${g.runner.state}` : 'runner.json пуст — раннер ещё не писал');
    mark(runnerIsLive(), runnerIsLive() ? 'раннер на связи' : 'раннер НЕ на связи (heartbeat устарел или отсутствует)');

    const probe = await gh('/rate_limit').catch(() => null);
    if (probe) {
      const c = probe.resources?.core;
      mark(c.remaining > 50, `лимит API: ${c.remaining}/${c.limit} запросов осталось`);
    }

    mark(navigator.onLine, navigator.onLine ? 'сеть есть' : 'НЕТ СЕТИ — работаю с локальной копией');
    const dirty = tasks.filter(t => t._dirty).length;
    mark(dirty === 0, dirty ? `${dirty} задач ждут отправки` : 'все локальные изменения отправлены');
    mark(true, `локально: ${tasks.length} задач, ${drafts.length} черновиков`);
  } catch (e) {
    mark(false, e.message);
  }

  box.innerHTML = lines.map(l => {
    const cls = l.startsWith('[ok]') ? 'ok' : 'err';
    return `<span class="${cls}">${esc(l.replace(/^\[(ok|!!)]\s*/, m => m.startsWith('[ok]') ? '✓ ' : '✗ '))}</span>`;
  }).join('\n');
}

/* ------------------------------ polling ------------------------------ */

function restartPolling() {
  clearInterval(pollTimer);
  const sec = Math.max(5, Number(settings.pollInterval) || 20);
  pollTimer = setInterval(() => {
    if (document.visibilityState === 'visible' && navigator.onLine) sync({ silent: true });
  }, sec * 1000);
}

/* ------------------------------ theme ------------------------------ */

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  writeLS(LS.theme, theme);
  document.querySelector('meta[name="theme-color"]')?.setAttribute('content', theme === 'dark' ? '#0D0D17' : '#F4F5FB');
}

/* ------------------------------ input helpers ------------------------------ */

function autoGrow(ta) {
  ta.style.height = 'auto';
  ta.style.height = Math.min(ta.scrollHeight, 260) + 'px';
}

/* ------------------------------ bind ------------------------------ */

function bind() {
  $('addBtn').addEventListener('click', addTask);
  $('taskText').addEventListener('input', (e) => autoGrow(e.target));
  $('taskText').addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); addTask(); }
  });

  $('prioritySeg').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-priority]'); if (!b) return;
    $('prioritySeg').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
  });

  $('filterSeg').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-filter]'); if (!b) return;
    $('filterSeg').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    settings.filter = b.dataset.filter;
    persistSettings(); renderList();
  });

  let searchTimer = null;
  $('searchInput').addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    const v = e.target.value;
    searchTimer = setTimeout(() => { settings.search = v; persistSettings(); renderList(); }, 180);
  });

  $('refreshBtn').addEventListener('click', () => sync());
  $('settingsBtn').addEventListener('click', openSettings);
  $('closeSheet').addEventListener('click', closeSheet);
  $('closeTaskSheet').addEventListener('click', closeSheet);
  $('tsClose').addEventListener('click', closeSheet);
  $('scrim').addEventListener('click', closeSheet);
  $('scrim2').addEventListener('click', closeSheet);

  $('saveSheet').addEventListener('click', saveSettings);
  $('diagBtn').addEventListener('click', runDiagnostics);
  $('toggleToken').addEventListener('click', () => {
    const i = $('ghToken');
    i.type = i.type === 'password' ? 'text' : 'password';
  });
  $('copyCmd').addEventListener('click', () => copyText($('runnerCmd').textContent.trim()));

  $('tsCopy').addEventListener('click', () => {
    const t = tasks.find(x => x.id === currentTaskId) || drafts.find(d => d.id === currentTaskId);
    if (t) copyText(t.text);
  });
  $('tsRetry').addEventListener('click', () => {
    if (!currentTaskId) return;
    closeSheet();
    handleAction('retry', currentTaskId);
  });

  $('clearDone').addEventListener('click', async () => {
    const finished = tasks.filter(t => t.status === 'done' || t.status === 'failed');
    if (!finished.length) { toast('Выполненных задач нет'); return; }
    if (!confirm(`Убрать из gist ${finished.length} выполненных/упавших задач?`)) return;
    finished.forEach(t => tombstones.push({ id: t.id, at: new Date().toISOString() }));
    writeLS(LS.tombstones, tombstones);
    tasks = tasks.filter(t => t.status !== 'done' && t.status !== 'failed');
    persistTasks(); renderAll();
    const ok = await sync({ silent: true });
    toast(ok ? 'Очищено' : 'Удалено локально, отправится позже', ok ? 'ok' : 'err');
  });

  $('wipeLocal').addEventListener('click', () => {
    if (!confirm('Стереть задачи, черновики и токен из этого браузера? В gist данные останутся.')) return;
    Object.values(LS).forEach(k => localStorage.removeItem(k));
    location.reload();
  });

  $('themeBtn').addEventListener('click', () =>
    applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'));

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeSheet();
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'r' && e.shiftKey) { e.preventDefault(); sync(); }
  });

  window.addEventListener('online', () => { toast('Сеть появилась — синхронизирую'); sync({ silent: true }); });
  window.addEventListener('offline', () => { setSync('warn', 'офлайн'); renderFoot(); });
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && navigator.onLine) sync({ silent: true });
  });

  // черновик текста переживает перезагрузку
  const ta = $('taskText');
  ta.value = readLS('crystal-queue:draftText', '') || '';
  autoGrow(ta);
  ta.addEventListener('input', () => writeLS('crystal-queue:draftText', ta.value));
}

/* ------------------------------ restore UI state ------------------------------ */

function restoreUi() {
  const f = settings.filter || 'active';
  $('filterSeg').querySelectorAll('button').forEach(b => b.classList.toggle('on', b.dataset.filter === f));
  $('searchInput').value = settings.search || '';
  $('taskProject').value = settings.defaultProject || '';
  $('prioritySeg').querySelectorAll('button')
    .forEach(b => b.classList.toggle('on', b.dataset.priority === 'normal'));
}

/* ------------------------------ init ------------------------------ */

async function init() {
  applyTheme(readLS(LS.theme, null) ||
    (window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark'));

  tasks = sortTasks(tasks.filter(t => t && t.id && typeof t.text === 'string'));
  drafts = drafts.filter(d => d && d.id && typeof d.text === 'string');
  seenStatus = seenStatus || {};

  bind();
  restoreUi();
  renderAll();

  if (!settings.token) {
    setSync('warn', 'нет токена');
    renderRunner();
    setTimeout(openSettings, 500);
    return;
  }

  setSync('busy', 'подключение');
  restartPolling();
  const ok = await sync({ silent: true });
  if (!ok) {
    setSync('err', 'нет связи');
    toast('Не удалось связаться с gist — работаю с локальной копией', 'err');
  } else {
    const live = runnerIsLive();
    toast(live ? 'Раннер на связи — задачи начнут выполняться' : 'Подключено. Раннер не запущен — задачи ждут ПК',
      live ? 'ok' : '');
  }

  if (settings.notifyDone && 'Notification' in window && Notification.permission === 'default') {
    try { await Notification.requestPermission(); } catch { /* не критично */ }
  }
}

init();
