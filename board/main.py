"""Agensh shared-context board + live office dashboard.

- Append-only, typed memory shared across workers (OBSERVED, FACT, FAIL,
  CLAIM, PATCH_SUMMARY), over a plain HTTP API.
- /state aggregates server-side: task kanban (derived from the board's
  claim/patch history), one cubicle per worker, Gitea repo state and the
  Mattermost channel. Secrets never reach the browser.
- / renders the live office: kanban (left) | worker cubicles (right)
  and the Mattermost chat below.
"""
import sqlite3, os, time, json, re, base64
import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

DB = os.environ.get("BOARD_DB", "data/board.db")
KINDS = {"OBSERVED", "FACT", "FAIL", "CLAIM", "PATCH_SUMMARY"}

# ---- live-state sources (server-side; secrets never reach the browser) ----
GITEA = os.environ.get("GITEA_URL", "http://10.20.8.134")
GITEA_HOST = os.environ.get("GITEA_HOST", "")
GITEA_USER = os.environ.get("GITEA_USER", "")
GITEA_PASS = os.environ.get("GITEA_PASS", "")
GITEA_OWNER = os.environ.get("TASK_OWNER", "admin")
GITEA_REPO = os.environ.get("GITEA_REPO", "task")
MM = os.environ.get("MATTERMOST_URL", "http://10.20.8.134")
MM_HOST = os.environ.get("MATTERMOST_HOST", "")
MM_TOKEN = os.environ.get("MATTERMOST_TOKEN", "")
MM_TEAM = os.environ.get("MATTERMOST_TEAM", "agentsh")
MM_CHANNEL = os.environ.get("MATTERMOST_CHANNEL", "pbench-task")
HX = httpx.Client(timeout=10)

os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
app = FastAPI(title="agensh-board")

def conn():
    c = sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS entries(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL, content TEXT NOT NULL,
        detail TEXT, author TEXT, created_at INTEGER NOT NULL)""")
    c.execute("""CREATE INDEX IF NOT EXISTS idx_kind ON entries(kind)""")
    return c

class EntryIn(BaseModel):
    kind: str
    content: str = Field(..., max_length=300)
    detail: str | None = None
    author: str = "unknown"

class EntryOut(EntryIn):
    id: int
    created_at: int

@app.post("/board", response_model=EntryOut)
def write(e: EntryIn):
    if e.kind not in KINDS:
        raise HTTPException(400, f"kind must be one of {sorted(KINDS)}")
    if len(e.content) > 100 and e.kind != "PATCH_SUMMARY":
        e.content = e.content[:100]
    try:
        c = conn(); now = int(time.time())
        cur = c.execute("INSERT INTO entries(kind,content,detail,author,created_at) VALUES(?,?,?,?,?)",
                        (e.kind, e.content, e.detail, e.author, now))
        c.commit(); last = cur.lastrowid
        row = c.execute("SELECT id,kind,content,detail,author,created_at FROM entries WHERE id=?", (last,)).fetchone()
        c.close()
    except Exception as ex:
        raise HTTPException(500, f"board write error: {type(ex).__name__}: {ex}")
    return EntryOut(id=row[0], kind=row[1], content=row[2], detail=row[3], author=row[4], created_at=row[5])

@app.get("/board/recent")
def recent(limit: int = Query(2000)):
    c = conn()
    rows = c.execute("SELECT id,kind,content,detail,author,created_at FROM entries ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    c.close()
    return [dict(zip(["id","kind","content","detail","author","created_at"], r)) for r in rows]

@app.get("/board/grep")
def grep(q: str, limit: int = Query(100)):
    terms = [t.strip().lower() for t in q.split(",") if t.strip()]
    if not terms: return []
    c = conn()
    conds = []
    for t in terms:
        parts = [p.strip() for p in t.split("&") if p.strip()]
        anded = " AND ".join(["(lower(content) LIKE ? OR lower(detail) LIKE ?)"] * len(parts))
        if anded: conds.append(anded)
    sql = "SELECT id,kind,content,detail,author,created_at FROM entries WHERE " + " OR ".join(conds)
    args = []
    for t in terms:
        for p in [x.strip() for x in t.split("&") if x.strip()]:
            args += ["%"+p+"%", "%"+p+"%"]
    rows = c.execute(sql + " ORDER BY id DESC LIMIT ?", args + [limit]).fetchall()
    c.close()
    return [dict(zip(["id","kind","content","detail","author","created_at"], r)) for r in rows]

@app.get("/board/unfold/{eid}")
def unfold(eid: int):
    c = conn()
    row = c.execute("SELECT id,kind,content,detail,author,created_at FROM entries WHERE id=?", (eid,)).fetchone()
    c.close()
    if not row: raise HTTPException(404, "no such entry")
    return dict(zip(["id","kind","content","detail","author","created_at"], row))

@app.get("/health")
def health(): return {"ok": True}

# ---------------------------------------------------------------- live state
def _g(path, **kw):
    h = dict(kw.pop("headers", None) or {})
    if GITEA_HOST: h["Host"] = GITEA_HOST
    return HX.request("GET", f"{GITEA}/api/v1{path}", auth=(GITEA_USER, GITEA_PASS), headers=h, **kw)

def _mm(path, **kw):
    h = {"Authorization": f"Bearer {MM_TOKEN}"}
    if MM_HOST: h["Host"] = MM_HOST
    return HX.get(f"{MM}/api/v4{path}", headers=h, **kw)

FILE_RE = re.compile(r"building\s+([\w./-]+?)[:\s]")

def _task_cards():
    """Latest claim per file, files whose PATCH_SUMMARY landed, last activity per author."""
    c = conn()
    rows = c.execute("SELECT kind,content,detail,author,created_at FROM entries "
                     "ORDER BY id DESC LIMIT 6000").fetchall()
    c.close()
    claims, patched, last_seen = {}, {}, {}
    for kind, content, detail, author, ts in rows:
        content = content or ""
        if author and (author not in last_seen or ts > last_seen[author]):
            last_seen[author] = ts
        if kind == "CLAIM":
            m = FILE_RE.search(content)
            f = m.group(1) if m else None
            if not f:
                m2 = re.match(r"([\w./-]+\.\w+)", (detail or "").strip())
                f = m2.group(1) if m2 else None
            if not f: continue
            prev = claims.get(f)
            if prev is None or ts > prev[1]:
                claims[f] = (author, ts, ((detail or content).strip())[:150])
        elif kind == "PATCH_SUMMARY":
            m = re.search(r"files=([\w./-]+)", content)
            if m and m.group(1) not in patched:
                patched[m.group(1)] = (author, ts)
    return claims, patched, last_seen

TASKS_DIR = os.environ.get("TASKS_DIR", "tasks_to_do")

def derive_file(desc):
    """Deterministic filename for an item that does not name one."""
    s = (desc or "").strip()
    m = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", s)
    if m:
        return m.group(1) + ".py"
    m = re.match(r"([A-Za-z_][A-Za-z0-9_.-]*)", s)
    if m:
        name = m.group(1).strip("._-")
        if name:
            return re.sub(r"[^\w.-]", "_", name) + ".py"
    return "task.py"

def parse_items(text):
    """Items are '- <file> :: <what>' or just '- <what>' (filename derived)."""
    items = []
    for line in (text or "").splitlines():
        s = line.strip()
        if not s.startswith("- "):
            continue
        body = s[2:].strip()
        if "::" in body:
            f, d = body.split("::", 1)
            items.append({"file": f.strip(), "desc": d.strip()})
        else:
            items.append({"file": "", "desc": body})
    used = {it["file"] for it in items if it["file"]}
    for it in items:
        if it["file"]:
            continue
        f = derive_file(it["desc"])
        base = f[:-3] if f.endswith(".py") else f
        i = 1
        while f in used:
            i += 1
            f = f"{base}_{i}.py"
        used.add(f)
        it["file"] = f
    return items

def _repo_file(path):
    r = _g(f"/repos/{GITEA_OWNER}/{GITEA_REPO}/contents/{path}", params={"ref": "main"})
    if r.status_code != 200:
        return None
    try:
        return base64.b64decode(r.json()["content"]).decode()
    except Exception:
        return None

_SPEC = {"ts": 0, "title": "", "items": [], "tasks": []}
def spec():
    """Tasks = every .md in tasks_to_do/ (files starting with _ are templates)."""
    if time.time() - _SPEC["ts"] < 30 and _SPEC["items"]:
        return _SPEC
    tasks, items = [], []
    try:
        r = _g(f"/repos/{GITEA_OWNER}/{GITEA_REPO}/contents/{TASKS_DIR}", params={"ref": "main"})
        if r.status_code == 200:
            names = sorted(f["name"] for f in r.json()
                           if f.get("type") == "file" and not f["name"].startswith("_"))
            for name in names:
                txt = _repo_file(f"{TASKS_DIR}/{name}") or ""
                title = next((l.strip().lstrip("# ").strip() for l in txt.splitlines()
                              if l.strip().startswith("#")), name)
                its = parse_items(txt)
                for it in its:
                    it["task"] = name
                items += its
                tasks.append({"file": name, "title": title, "items": len(its)})
    except Exception:
        pass
    title = ", ".join(t["title"] for t in tasks) or f"(nenhuma spec em {TASKS_DIR}/)"
    _SPEC.update(ts=time.time(), title=title, items=items, tasks=tasks)
    return _SPEC

def _gitea_put(path, content, message, branch="main"):
    h = {"Host": GITEA_HOST} if GITEA_HOST else {}
    sha = None
    r = HX.get(f"{GITEA}/api/v1/repos/{GITEA_OWNER}/{GITEA_REPO}/contents/{path}",
               params={"ref": branch}, auth=(GITEA_USER, GITEA_PASS), headers=h)
    if r.status_code == 200:
        sha = r.json().get("sha")
    body = {"content": base64.b64encode(content.encode()).decode(),
            "message": message, "branch": branch}
    if sha:
        body["sha"] = sha
    return HX.put(f"{GITEA}/api/v1/repos/{GITEA_OWNER}/{GITEA_REPO}/contents/{path}",
                  json=body, auth=(GITEA_USER, GITEA_PASS), headers=h)

class TaskIn(BaseModel):
    title: str
    description: str = ""
    items: list[str] = []
    reset: bool = False

@app.post("/task")
def set_task(t: TaskIn):
    """The task-submission door: writes SPEC.md to the workspace repo."""
    norm = []
    for raw in t.items:
        s = (raw or "").strip()
        if not s:
            continue
        if "::" in s:
            f, d = s.split("::", 1)
            f, d = f.strip(), d.strip()
        else:
            f, d = s, ""
        if not re.match(r"^[\w./-]+\.\w+$", f):
            f = re.sub(r"\W+", "_", f).strip("_") + ".py"
        norm.append((f, d))
    if not norm:
        raise HTTPException(400, "informe ao menos um item, no formato 'arquivo.py :: o que implementar'")
    md = (f"# Task: {t.title}\n\n{t.description}\n\n## Itens\n"
          + "\n".join(f"- {f} :: {d}" for f, d in norm) + "\n")
    slug = re.sub(r"[^a-z0-9]+", "-", t.title.lower()).strip("-")[:40] or "tarefa"
    path = f"{TASKS_DIR}/{slug}.md"
    r = _gitea_put(path, md, f"task: {t.title}")
    if r.status_code not in (200, 201):
        raise HTTPException(502, f"gitea {path} write failed {r.status_code}: {r.text[:200]}")
    if t.reset:
        c = conn(); c.execute("DELETE FROM entries"); c.commit(); c.close()
    try:
        write(EntryIn(kind="FACT", content=f"tarefa definida: {t.title}"[:100],
                      detail=f"{len(norm)} itens", author="user"))
    except Exception:
        pass
    _SPEC["ts"] = 0
    return {"ok": True, "title": t.title, "items": len(norm)}

_USERS = {"ts": 0, "map": {}}
def usernames(ids):
    ids = sorted(set(i for i in ids if i))
    if not ids: return {}
    if time.time() - _USERS["ts"] > 300 or not set(ids) <= set(_USERS["map"]):
        try:
            h = {"Authorization": f"Bearer {MM_TOKEN}", "Content-Type": "application/json"}
            if MM_HOST: h["Host"] = MM_HOST
            r = HX.post(f"{MM}/api/v4/users/ids", json=ids, headers=h)
            if r.status_code == 200:
                _USERS["map"] = {u["id"]: u["username"] for u in r.json()}
                _USERS["ts"] = time.time()
        except Exception:
            pass
    return _USERS["map"]

@app.get("/state")
def state():
    repo = {"branches": [], "files": [], "pulls": [], "commits": []}
    try:
        repo["branches"] = [b["name"] for b in _g(f"/repos/{GITEA_OWNER}/{GITEA_REPO}/branches").json()]
    except Exception as e: repo["branches_error"] = str(e)[:120]
    try:
        repo["files"] = [f["name"] for f in _g(f"/repos/{GITEA_OWNER}/{GITEA_REPO}/contents", params={"ref": "main"}).json()]
    except Exception as e: repo["files_error"] = str(e)[:120]
    try:
        repo["commits"] = [{"sha": c["sha"][:8], "msg": c["commit"]["message"].splitlines()[0],
                            "author": c["commit"]["author"]["name"]}
                           for c in _g(f"/repos/{GITEA_OWNER}/{GITEA_REPO}/commits", params={"limit": 15}).json()]
    except Exception as e: repo["commits_error"] = str(e)[:120]
    try:
        repo["pulls"] = [{"title": p["title"], "state": p["state"], "head": p["head"]["label"]}
                         for p in _g(f"/repos/{GITEA_OWNER}/{GITEA_REPO}/pulls", params={"state": "all"}).json()]
    except Exception as e: repo["pulls_error"] = str(e)[:120]

    msgs = []
    try:
        ch = _mm(f"/teams/name/{MM_TEAM}/channels/name/{MM_CHANNEL}")
        if ch.status_code == 200:
            cid = ch.json()["id"]
            posts = _mm(f"/channels/{cid}/posts", params={"per_page": 40}).json()
            order = list(reversed(posts.get("order") or []))[:40]
            raw = [posts["posts"][pid] for pid in order]
            names = usernames([p.get("user_id") for p in raw])
            for p in raw:
                msgs.append({"user": names.get(p.get("user_id"), (p.get("user_id") or "?")[:8]),
                             "msg": p.get("message", "")[:400],
                             "ts": p.get("create_at", 0)})
        else:
            msgs = [{"error": f"channel lookup {ch.status_code}", "user": "system", "msg": "", "ts": 0}]
    except Exception as e:
        msgs = [{"error": str(e)[:160], "user": "system", "msg": "", "ts": 0}]

    claims, patched, last_seen = _task_cards()
    now = int(time.time())
    main_files = set(repo.get("files") or [])
    sp = spec()
    items = sp.get("items") or []
    spec_files = {i["file"] for i in items if i.get("file")}

    backlog, doing, done, merged = [], [], [], []
    for it in items:
        f = it.get("file")
        if f and f in main_files:
            merged.append({"file": f, "desc": it.get("desc", "")})
        elif f and f in patched:
            done.append({"file": f, "desc": it.get("desc", ""), "worker": patched[f][0]})
        elif f and f in claims:
            a, ts, d = claims[f]
            doing.append({"file": f, "desc": it.get("desc") or d, "worker": a, "age": now - ts})
        else:
            backlog.append({"file": f or (it.get("desc", "")[:24]), "desc": it.get("desc", "")})
    doing.sort(key=lambda x: x["age"])

    # work taken outside the task list — shown, not hidden
    extra = []
    for f, (a, ts, d) in claims.items():
        if f not in spec_files and f not in main_files and f not in patched:
            extra.append({"file": f, "worker": a, "age": now - ts})
    extra.sort(key=lambda x: x["age"])

    wmap = {}
    for f, (a, ts, d) in claims.items():
        if a not in wmap or ts > wmap[a]["ts"]:
            wmap[a] = {"name": a, "file": f, "desc": d, "ts": ts}
    workers = sorted(wmap.values(), key=lambda x: x["name"])
    for w in workers:
        w["age"] = now - last_seen.get(w["name"], w["ts"])
        w["working"] = w["age"] < 120
        w["landed"] = w["file"] in patched

    return {"spec": {"title": sp["title"], "items": items,
                     "files": sorted(spec_files), "tasks": sp.get("tasks", [])},
            "kanban": {"backlog": backlog[:16], "doing": doing[:14],
                       "done": done[:16], "merged": merged[:16]},
            "extra": extra[:10],
            "workers": workers, "repo": repo, "messages": msgs, "ts": now}

# ---------------------------------------------------------------- dashboard
DASH = r"""<!doctype html><html lang=pt-BR><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Agensh · escritório ao vivo</title>
<style>
*{box-sizing:border-box}
body{background:#0b0d12;color:#e6eaf2;font:13px/1.45 ui-sans-serif,system-ui,"Segoe UI",Roboto,sans-serif;margin:0;padding:14px}
h1{font-size:15px;margin:0;letter-spacing:.02em}
h2{font-size:11px;margin:0 0 8px;color:#8ab4ff;text-transform:uppercase;letter-spacing:.12em;font-weight:700}
.hdr{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
.hdr .pill{background:#151a24;border:1px solid #232a38;border-radius:999px;padding:3px 10px;font-size:11px;color:#9fb0c8}
.hdr .spec{color:#ffd479}
.live{width:8px;height:8px;border-radius:50%;background:#39d98a;box-shadow:0 0 0 0 rgba(57,217,138,.7);animation:pulse 2s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(57,217,138,.6)}70%{box-shadow:0 0 0 9px rgba(57,217,138,0)}100%{box-shadow:0 0 0 0 rgba(57,217,138,0)}}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}
.panel{background:#101521;border:1px solid #1d2432;border-radius:12px;padding:12px}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.col{background:#0d1119;border:1px solid #1a2130;border-radius:9px;padding:7px;min-height:120px}
.col h3{margin:0 0 6px;font-size:11px;color:#7f8ea8;text-transform:uppercase;letter-spacing:.08em;display:flex;justify-content:space-between}
.col h3 b{color:#c8d4e6}
.cards{display:flex;flex-direction:column;gap:6px}
.card{background:#161d2b;border:1px solid #25304a;border-left:3px solid #4a5a7a;border-radius:7px;padding:6px 8px;font-size:12px;transition:transform .15s,box-shadow .15s}
.card .f{font-family:ui-monospace,Menlo,monospace;color:#9fd0ff;font-weight:600}
.card .w{color:#8ea2c0;font-size:10.5px;display:block;margin-top:2px}
.card .d{color:#7c8ba6;font-size:10.5px;display:block;margin-top:3px;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.card.doing{border-left-color:#ffd479}.card.done{border-left-color:#c9a8ff}.card.merged{border-left-color:#39d98a}
.card.ghost{opacity:.55}
.card.flash{box-shadow:0 0 0 2px rgba(138,180,255,.45)}
.rooms{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.cubicle{background:linear-gradient(180deg,#141b28,#0f141e);border:1px solid #202839;border-radius:10px;padding:10px;position:relative;overflow:hidden}
.cubicle .top{display:flex;align-items:center;gap:7px}
.avatar{width:26px;height:26px;border-radius:7px;background:#1c2740;display:grid;place-items:center;font-size:14px;flex:none}
.who{font-weight:700;font-size:12.5px}
.led{width:9px;height:9px;border-radius:50%;background:#3a4356;margin-left:auto;flex:none}
.led.on{background:#39d98a;animation:pulse 1.6s infinite}
.led.off{background:#5a6478}
.role{font-size:10px;color:#6d7c96;margin-top:1px}
.desk{height:52px;margin-top:8px;border-bottom:2px solid #27324a;position:relative}
.desk .walk{position:absolute;bottom:2px;font-size:18px;transition:left .9s cubic-bezier(.2,.7,.3,1)}
.hand{margin-top:8px;min-height:44px;border:1px dashed #2a3347;border-radius:7px;padding:5px 7px;background:#0d1119}
.hand .lbl{font-size:9.5px;color:#6d7c96;text-transform:uppercase;letter-spacing:.08em}
.hand .card{margin-top:4px;cursor:default}
body.tick .cubicle .walk{left:78%}
.chat{max-height:38vh;overflow:auto;display:flex;flex-direction:column;gap:2px}
.m{display:flex;gap:8px;padding:3px 5px;border-radius:6px;font-size:12.5px}
.m:nth-child(odd){background:#0d1119}
.m .u{color:#8ab4ff;font-weight:600;flex:none;min-width:74px}
.m .t{color:#cfd8e6;word-break:break-word}
.m .ts{color:#55617a;font-size:10px;margin-left:auto;flex:none}
.bar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:10px;font-size:11px;color:#8ea2c0}
.chip{background:#151a24;border:1px solid #232a38;border-radius:999px;padding:2px 9px}
.chip.pr{color:#ffd479}.chip.mg{color:#39d98a}
.tform{display:flex;flex-direction:column;gap:7px}
.tform input,.tform textarea{background:#0d1119;border:1px solid #25304a;border-radius:7px;color:#e6eaf2;padding:7px 9px;font:12.5px/1.4 ui-monospace,Menlo,monospace;width:100%}
.tform textarea{resize:vertical}
.trow{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
button{background:#1d4ed8;border:0;color:#fff;font:600 12.5px ui-sans-serif,system-ui;padding:9px 18px;border-radius:8px;cursor:pointer}
button:disabled{opacity:.5;cursor:default}
.chip.warn{color:#ff9a9a}
.specdesc{color:#8ea2c0;font-size:11.5px;margin:-4px 0 12px}
#fly{position:fixed;pointer-events:none;z-index:99;transition:all .85s cubic-bezier(.2,.7,.3,1)}
.tip{color:#55617a;font-size:11px}
</style></head><body>
<div class=hdr>
 <span class=live></span><h1>AGENSH · organização viva</h1>
 <span class="pill spec" id=spec>—</span>
 <span class=pill id=clock>—</span>
 <span class=pill id=stats>—</span>
</div>
<div class=specdesc id=specdesc></div>
<div class=grid>
 <section class=panel>
  <h2>Quadro de tarefas</h2>
  <div class=cols>
   <div class=col><h3>Backlog <b id=c_backlog></b></h3><div class=cards id=k_backlog></div></div>
   <div class=col><h3>Em andamento <b id=c_doing></b></h3><div class=cards id=k_doing></div></div>
   <div class=col><h3>Entregue <b id=c_done></b></h3><div class=cards id=k_done></div></div>
   <div class=col><h3>No main <b id=c_merged></b></h3><div class=cards id=k_merged></div></div>
  </div>
 </section>
 <section class=panel>
  <h2>Escritório · salas dos workers</h2>
  <div class=rooms id=rooms></div>
  <div class=bar id=bar></div>
 </section>
</div>
<section class="panel" style="margin-top:14px">
 <h2>Passar uma tarefa para a organiza&ccedil;&atilde;o</h2>
 <div class=tform>
  <input id=t_title placeholder="T&iacute;tulo da tarefa" autocomplete=off>
  <textarea id=t_desc rows=2 placeholder="Descri&ccedil;&atilde;o / entreg&aacute;vel: o que a equipe deve produzir"></textarea>
  <textarea id=t_items rows=5 placeholder="Um item por linha, no formato:   arquivo.py :: o que implementar"></textarea>
  <div class=trow>
   <button id=t_send>Enviar tarefa</button>
   <label class=tip><input type=checkbox id=t_reset> limpar hist&oacute;rico do board</label>
   <span id=t_status class=tip></span>
  </div>
 </div>
</section>
<section class=panel style="margin-top:14px">
 <h2>Mattermost · #pbench-task</h2>
 <div class=chat id=chat></div>
</section>
<div id=fly></div>
<script>
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cssEsc=s=>(window.CSS&&CSS.escape)?CSS.escape(s):String(s).replace(/"/g,'\\"');
const prev={}; let first=true;

function cardHTML(c, cls){
  return `<div class="card ${cls}" data-file="${esc(c.file)}">
    <span class=f>${esc(c.file)}</span>
    ${c.desc?`<span class=d>${esc(c.desc)}</span>`:''}
    ${c.worker?`<span class=w>${esc(c.worker)}${c.age!=null?` · ${c.age}s`:''}</span>`:''}
  </div>`;
}
function renderKanban(k){
  const cols=[['backlog','ghost'],['doing','doing'],['done','done'],['merged','merged']];
  for(const [name,cls] of cols){
    const box=document.getElementById('k_'+name);
    const items=(k[name]||[]).map(c=>(typeof c==='string')?{file:c}:c);
    box.innerHTML=items.map(c=>cardHTML(c,cls)).join('')||'<span class=tip>(vazio)</span>';
    document.getElementById('c_'+name).textContent=items.length;
  }
}
function renderRooms(ws){
  const box=document.getElementById('rooms');
  box.innerHTML=(ws||[]).map((w,i)=>`<div class=cubicle data-worker="${esc(w.name)}">
    <div class=top><div class=avatar>${w.working?'👷':'😴'}</div>
      <div><div class=who>${esc(w.name)}</div><div class=role>${w.working?'trabalhando':'ocioso'} · ${w.age}s</div></div>
      <div class="led ${w.working?'on':'off'}"></div></div>
    <div class=desk><span class=walk style="left:${w.working?'18%':'6%'}">🚶</span></div>
    <div class=hand><div class=lbl>tarefa na mão</div>${w.file?cardHTML({file:w.file,desc:w.desc,worker:''}, w.landed?'done':'doing'):'<span class=tip>—</span>'}</div>
  </div>`).join('')||'<span class=tip>(sem workers)</span>';
  document.body.classList.toggle('tick',(ws||[]).some(w=>w.working));
}
function renderChat(m){
  const box=document.getElementById('chat');
  const near=box.scrollHeight-box.scrollTop-box.clientHeight<60;
  box.innerHTML=(m||[]).map(x=>`<div class=m><span class=u>${esc(x.user||'')}</span>
    <span class=t>${esc(x.msg||x.error||'')}</span>
    <span class=ts>${x.ts?new Date(x.ts).toLocaleTimeString():''}</span></div>`).join('')||'<span class=tip>(sem mensagens)</span>';
  if(near) box.scrollTop=box.scrollHeight;
}
function renderBar(r, extra){
  const pr=(r.pulls||[]).filter(p=>p.state==='open');
  const ex=(extra||[]);
  document.getElementById('bar').innerHTML=
    `<span class=chip>branches: <b>${(r.branches||[]).join(', ')||'-'}</b></span>
     <span class="chip pr">PRs abertos: <b>${pr.length}</b> ${pr.map(p=>esc(p.head)).join(' ')||''}</span>
     <span class=chip>commits: ${(r.commits||[]).length}</span>
     <span class=chip>arquivos@main: ${(r.files||[]).length}</span>
     ${ex.length?`<span class="chip warn">fora do SPEC: <b>${ex.length}</b> ${ex.map(e=>esc(e.file)).join(' ')||''}</span>`:''}`;
}
function fly(from, to, file){
  const src=document.querySelector('.card[data-file="'+cssEsc(file)+'"]');
  const el=document.getElementById('fly');
  el.innerHTML=src?src.outerHTML:'<div class=card doing><span class=f>'+esc(file)+'</span></div>';
  el.style.transition='none';
  el.style.left=from.left+'px'; el.style.top=from.top+'px';
  el.style.width=from.width+'px'; el.style.opacity='1';
  el.getBoundingClientRect();
  requestAnimationFrame(()=>{ el.style.transition='all .85s cubic-bezier(.2,.7,.3,1)';
    el.style.left=to.left+'px'; el.style.top=to.top+'px'; el.style.width=to.width+'px'; el.style.opacity='.15'; });
  setTimeout(()=>{ el.innerHTML=''; el.style.cssText='position:fixed;pointer-events:none;z-index:99'; },900);
}
async function tick(){
 try{
  const s=await (await fetch('/state')).json();
  const moves=[];
  for(const w of (s.workers||[])){
    const before=prev[w.name];
    if(before && before!==w.file){
      const src=document.querySelector('.card[data-file="'+cssEsc(before)+'"]');
      if(src) moves.push({worker:w.name,file:w.file,from:src.getBoundingClientRect()});
    }
    prev[w.name]=w.file;
  }
  document.getElementById('spec').textContent='tarefa: '+(s.spec&&s.spec.title||'—');
  const st=(s.spec&&s.spec.tasks)||[];
  document.getElementById('specdesc').innerHTML = st.length
    ? 'specs em tasks_to_do/: ' + st.map(t=>`<b>${esc(t.file)}</b> (${t.items} itens)`).join(' &middot; ')
    : 'nenhuma spec em tasks_to_do/ &mdash; coloque um .md l&aacute; no Gitea, ou use o formul&aacute;rio abaixo';
  document.getElementById('clock').textContent=new Date().toLocaleTimeString();
  document.getElementById('stats').textContent='board entries: '+(s.ts?'live':'—');
  renderKanban(s.kanban||{}); renderRooms(s.workers); renderChat(s.messages); renderBar(s.repo||{}, s.extra);
  for(const m of moves){
    const dst=document.querySelector('.cubicle[data-worker="'+cssEsc(m.worker)+'"] .hand');
    if(dst) fly(m.from,dst.getBoundingClientRect(),m.file);
  }
  for(const el of document.querySelectorAll('.hand .card, .card.doing')){el.classList.add('flash');}
  setTimeout(()=>document.querySelectorAll('.flash').forEach(e=>e.classList.remove('flash')),400);
 }catch(e){document.getElementById('clock').textContent='erro: '+e;}
}
document.getElementById('t_send').onclick=async()=>{
  const btn=document.getElementById('t_send'), st=document.getElementById('t_status');
  const title=document.getElementById('t_title').value.trim();
  const items=document.getElementById('t_items').value.split('\n').map(s=>s.trim()).filter(Boolean);
  if(!title||!items.length){ st.textContent='informe o t\u00edtulo e ao menos 1 item'; return; }
  btn.disabled=true; st.textContent='enviando...';
  try{
    const r=await fetch('/task',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({title, description:document.getElementById('t_desc').value.trim(),
        items, reset:document.getElementById('t_reset').checked})});
    let j={}; try{ j=await r.json(); }catch(e){}
    st.textContent = r.ok ? ('enviada: '+j.items+' itens. Os workers pegam em segundos.')
                          : ('erro '+r.status+': '+((j.detail)||''));
  }catch(e){ st.textContent='erro: '+e; }
  btn.disabled=false;
};
tick(); setInterval(tick,2000);
</script></body></html>"""

@app.get("/", response_class=HTMLResponse)
def dashboard(): return DASH
