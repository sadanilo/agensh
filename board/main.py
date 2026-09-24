"""Agensh shared-context board + live dashboard.

- Append-only, typed memory shared across workers (OBSERVED, FACT, FAIL,
  CLAIM, PATCH_SUMMARY), over a plain HTTP API.
- A server-side /state aggregator (board + Gitea repo + Mattermost channel)
  and a self-refreshing HTML dashboard at /, so the organisation can be
  watched live: who claims what, what lands, and the repo state.
"""
import sqlite3, os, time, json
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
            posts = _mm(f"/channels/{cid}/posts", params={"per_page": 30}).json()
            for pid in (posts.get("order") or [])[:30]:
                p = posts["posts"][pid]
                msgs.append({"user": (p.get("user_id") or "")[:8], "msg": p.get("message", "")[:200]})
        else:
            msgs = [{"error": f"channel lookup {ch.status_code}"}]
    except Exception as e:
        msgs = [{"error": str(e)[:160]}]
    return {"board": recent(200)[:200], "repo": repo, "messages": msgs, "ts": int(time.time())}

DASH = """<!doctype html><html><head><meta charset=utf-8><title>Agensh live</title>
<style>body{background:#0f1115;color:#dfe3ea;font:13px/1.45 ui-monospace,Menlo,monospace;margin:0;padding:16px}
h1{font-size:16px;margin:0 0 10px}h2{color:#8ab4ff;margin:14px 0 6px;font-size:13px;text-transform:uppercase;letter-spacing:.08em}
.wrap{display:grid;grid-template-columns:1.35fr 1fr;gap:16px}
.card{background:#171a21;border:1px solid #242a36;border-radius:8px;padding:10px;max-height:62vh;overflow:auto}
.k{display:inline-block;padding:1px 6px;border-radius:4px;font-weight:700;margin-right:6px}
.CLAIM{background:#3b3216;color:#ffd479}.FACT{background:#12331f;color:#7ee2a8}.OBSERVED{background:#132a3b;color:#7cc4ff}
.FAIL{background:#3b1a1a;color:#ff9a9a}.PATCH_SUMMARY{background:#241a3b;color:#c9a8ff}
.e{padding:4px 0;border-bottom:1px solid #1d222c}.t{color:#5c6675;font-size:11px}
ul{margin:4px 0;padding-left:18px}code{color:#9fd0ff}</style></head><body>
<h1>AGENSH &middot; organiza&ccedil;&atilde;o viva <span class=t id=ts></span></h1>
<div class=wrap>
 <div><h2>Shared context (board)</h2><div class=card id=board></div>
      <h2>Mattermost #pbench-task</h2><div class=card id=msgs></div></div>
 <div><h2>Repo admin/task</h2><div class=card id=repo></div></div>
</div>
<script>
async function tick(){
 try{const s=await (await fetch('/state')).json();
  document.getElementById('ts').textContent='\\u00b7 '+new Date().toLocaleTimeString();
  document.getElementById('board').innerHTML=(s.board||[]).map(e=>
   `<div class=e><span class="k ${e.kind}">${e.kind}</span><b>${e.author||''}</b> ${e.content||''}`
   +(e.detail?`<div class=t>${e.detail}</div>`:'')
   +`<div class=t>${new Date((e.created_at||0)*1000).toLocaleTimeString()}</div></div>`).join('')||'<div class=t>(vazio)</div>';
  const r=s.repo||{};
  document.getElementById('repo').innerHTML=
   `<b>branches:</b> ${(r.branches||[]).map(b=>'<code>'+b+'</code>').join(' ')||'-'}`
   +`<div style="margin-top:8px"><b>files@main:</b> ${(r.files||[]).map(f=>'<code>'+f+'</code>').join(' ')||'-'}</div>`
   +`<div style="margin-top:8px"><b>commits:</b><ul>${(r.commits||[]).map(c=>'<li><code>'+c.sha+'</code> '+c.msg+' <span class=t>('+c.author+')</span></li>').join('')||'<li>-</li>'}</ul></div>`
   +`<div style="margin-top:8px"><b>PRs:</b><ul>${(r.pulls||[]).map(p=>'<li>'+p.head+' &rarr; '+p.state+' '+p.title+'</li>').join('')||'<li>-</li>'}</ul></div>`;
  document.getElementById('msgs').innerHTML=(s.messages||[]).map(m=>`<div class=e><span class=t>${m.user||''}</span> ${m.msg||m.error||''}</div>`).join('')||'<div class=t>(sem mensagens)</div>';
 }catch(e){document.getElementById('ts').textContent='\\u00b7 erro '+e;}
}
tick(); setInterval(tick,3000);
</script></body></html>"""

@app.get("/", response_class=HTMLResponse)
def dashboard(): return DASH
