"""Agensh shared-context board.

Append-only, typed memory shared across workers. Kinds: OBSERVED, FACT, FAIL,
CLAIM, PATCH_SUMMARY. Plain HTTP API (the router calls it directly); the real
Agensh surfaces these over MCP tool returns, which is how findings reach a
worker mid-turn.
"""
import sqlite3, os, time, json
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

DB = os.environ.get("BOARD_DB", "data/board.db")
KINDS = {"OBSERVED", "FACT", "FAIL", "CLAIM", "PATCH_SUMMARY"}

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
        # short entries are the norm; long versions go in 'detail'
        e.content = e.content[:100]
    c = conn(); now = int(time.time())
    c.execute("INSERT INTO entries(kind,content,detail,author,created_at) VALUES(?,?,?,?,?)",
              (e.kind, e.content, e.detail, e.author, now))
    c.commit(); last = c.lastrowid
    row = c.execute("SELECT id,kind,content,detail,author,created_at FROM entries WHERE id=?", (last,)).fetchone()
    c.close()
    return EntryOut(id=row[0], kind=row[1], content=row[2], detail=row[3], author=row[4], created_at=row[5])

@app.get("/board/recent")
def recent(limit: int = Query(2000)):
    c = conn()
    rows = c.execute("SELECT id,kind,content,detail,author,created_at FROM entries ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    c.close()
    return [dict(zip(["id","kind","content","detail","author","created_at"], r)) for r in rows]

@app.get("/board/grep")
def grep(q: str, limit: int = Query(100)):
    # q is a comma-OR / ampersand-AND index query, like the paper's board_grep.
    terms = [t.strip().lower() for t in q.split(",") if t.strip()]
    if not terms: return []
    c = conn()
    conds = []
    for t in terms:
        parts = [p.strip() for p in t.split("&") if p.strip()]
        anded = " AND ".join(["(lower(content) LIKE ? OR lower(detail) LIKE ?)"] * len(parts))
        if anded:
            conds.append(anded)
    sql = f"SELECT id,kind,content,detail,author,created_at FROM entries WHERE " + " OR ".join(conds)
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
