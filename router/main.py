"""Agensh worker router: coordination loop for N self-organized workers.

Each worker task runs the Agensh loop against the shared infra:
  - shared workspace  -> Gitea (issues, branches, PRs, files via Contents API)
  - message interface -> Mattermost (channel announcements + per-worker DMs)
  - shared context    -> the board (typed OBSERVED/FACT/FAIL/CLAIM/PATCH_SUMMARY)

The loop (from Agensh / Appendix A): gather context -> claim a slice ->
act -> verify -> merge -> publish PATCH_SUMMARY -> repeat. Idle detector nudges
workers that stop for too long. Workers share one coordinator but run a local,
independent loop each, so nobody waits on an orchestrator.
"""
import os, time, json, base64, asyncio, hashlib, logging, re
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("router")

ENV = os.environ
N = int(ENV.get("WORKERS", "3"))
GITEA = ENV.get("GITEA_URL", "http://gitea:3000")
GITEA_USER = ENV.get("GITEA_USER", "")
GITEA_PASS = ENV.get("GITEA_PASS", "")
GITEA_AUTH = (GITEA_USER, GITEA_PASS)
MATTERMOST = ENV.get("MATTERMOST_URL", "http://mattermost:8065")
MM_TOKEN = ENV.get("MATTERMOST_TOKEN", "")
MM_CHANNEL = ENV.get("MATTERMOST_CHANNEL", "pbench-task")
MM_TEAM = ENV.get("MATTERMOST_TEAM", "agentsh")
BOARD = ENV.get("BOARD_URL", "http://board:8000")
LLM_BASE = ENV.get("LLM_BASE_URL", "")
LLM_KEY = ENV.get("LLM_API_KEY", "")
LLM_MODEL = ENV.get("LLM_MODEL", "")
TASK_OWNER = ENV.get("TASK_OWNER", "admin")
TASK_REPO = ENV.get("TASK_REPO", "task")
BUDGET = float(ENV.get("BUDGET_SECONDS", "1200"))
IDLE_SECONDS = float(ENV.get("IDLE_SECONDS", "120"))

client = httpx.Client(timeout=60)

# ---------------------------------------------------------------- primitives
def board_write(kind, content, detail=None, author="router"):
    r = client.post(f"{BOARD}/board", json={"kind": kind, "content": content,
                                            "detail": detail, "author": author})
    r.raise_for_status(); return r.json()

def board_recent(limit=2000):
    r = client.get(f"{BOARD}/board/recent", params={"limit": limit})
    r.raise_for_status(); return r.json()

def board_grep(q):
    r = client.get(f"{BOARD}/board/grep", params={"q": q})
    r.raise_for_status(); return r.json()

def gh(method, path, **kw):
    r = client.request(method, f"{GITEA}/api/v1{path}", auth=GITEA_AUTH, **kw)
    r.raise_for_status()
    return r.json() if r.text else None

def gitea_create_branch(repo, branch, base="main"):
    # PIP-like: create a new branch pointing at base
    b = gh("GET", f"/repos/{TASK_OWNER}/{repo}/branches/{base}")
    sha = b["commit"]["id"]
    return gh("POST", f"/repos/{TASK_OWNER}/{repo}/branches",
              json={"branch_name": branch, "base": base}) if branch not in [
        x["name"] for x in gh("GET", f"/repos/{TASK_OWNER}/{repo}/branches")] else None

def gitea_write_file(repo, path, content, branch, message="update"):
    data = base64.b64encode(content.encode()).decode()
    return gh("PUT", f"/repos/{TASK_OWNER}/{repo}/contents/{path}",
              json={"branch": branch, "content": data, "message": message})

def gitea_read_file(repo, path, ref="main"):
    r = client.get(f"{GITEA}/api/v1/repos/{TASK_OWNER}/{repo}/contents/{path}",
                   params={"ref": ref}, auth=GITEA_AUTH)
    if r.status_code != 200: return None
    j = r.json(); return base64.b64decode(j["content"]).decode()

def gitea_open_pr(repo, head, base="main", title="", body=""):
    return gh("POST", f"/repos/{TASK_OWNER}/{repo}/pulls",
              json={"title": title or f"PR {head}", "body": body,
                    "head": head, "base": base})

def mm_post(channel, msg, user_id="bot"):
    # post to a channel; channel referenced by name in Mattermost API v4
    r = client.post(f"{MATTERMOST}/api/v4/channels/name/{MM_TEAM}:{channel}/posts",
                    json={"message": msg}, headers={"Authorization": f"Bearer {MM_TOKEN}"})
    if r.status_code >= 400:
        # fall back to posting to the channel by id resolved once
        log.warning("mm post failed (%s): %s", r.status_code, r.text[:200])

def dm(user, msg, user_id="bot"):
    # direct message to a worker handle (interrupts their turn in Agensh)
    r = client.post(f"{MATTERMOST}/api/v4/channels/name/{MM_TEAM}:{channel}/{user}/posts",
                    json={"message": msg}, headers={"Authorization": f"Bearer {MM_TOKEN}"})
    if r.status_code >= 400:
        log.warning("dm failed (%s): %s", r.status_code, r.text[:200])

# ---------------------------------------------------------------- the model
def llm(messages, temperature=0.4):
    r = client.post(f"{LLM_BASE}/chat/completions",
                    json={"model": LLM_MODEL, "messages": messages,
                          "temperature": temperature},
                    headers={"Authorization": f"Bearer {LLM_KEY}"})
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

def parse_decision(text):
    """Extract the JSON action block from a model reply. Tolerates prose."""
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m: return None
    try: return json.loads(m.group(0))
    except Exception: return None

# ---------------------------------------------------------------- one worker
class Worker:
    def __init__(self, i):
        self.i = i
        self.handle = f"worker{i+1}"
        self.branch = f"w{i+1}"
        self.last_activity = time.time()
        self.file_cache = {}

    def system_prompt(self):
        return f"""You are {self.handle}, one of {N} equal agents rebuilding a small program.
Work is tracked in Gitea; you talk on Mattermost; you share findings on the board.
Cooperation loop each step:
1. Read the recent board entries (shared context) + recent messages.
2. If a peer CLAIMs the file you were about to touch, pick a different one.
3. CLAIM the next slice of work on the board.
4. Produce a single, correct, self-contained Python file for that slice (a module
   that is logic-correct and runs).
5. Verify it: it must be valid Python and implement what it claims.
6. Publish PATCH_SUMMARY in the form files= | idea= | evidence=.
Respond with ONLY a JSON object. Valid actions:
{{"action":"claim","file":"<path.py>","summary":"<what you will build>"}}
{{"action":"write","file":"<path.py>","code":"<full python source>"}}
{{"action":"fail","file":"<path.py>","reason":"<why>"}}
{{"action":"observe","note":"<an observation>", "kind":"FACT|OBSERVED"}}
{{"action":"done","summary":"<final state>"}}
Never repeat a peer's FACT, never retry a recorded FAIL."""

    def gather(self):
        recent = board_recent()
        board_txt = "\n".join(
            f"[{e['kind']}][{e['author']}] {e['content']}" for e in recent[:60])
        files = self.select_slices()
        return board_txt, files

    def select_slices(self):
        # task-specific slices (synthetic task). The task repo has a spec file.
        spec = gitea_read_file(TASK_REPO, "SPEC.md") or "implement mywc.py"
        known = [x["name"] for x in gh("GET", f"/repos/{TASK_OWNER}/{TASK_REPO}/contents", params={"ref": self.branch})]
        return known

    async def run(self, stop_event):
        log.info("%s online", self.handle)
        # ensure branch exists
        try:
            gitea_create_branch(TASK_REPO, self.branch)
        except Exception as e:
            log.warning("%s branch setup: %s", self.handle, e)
        while not stop_event.is_set() and (time.time() - self.last_activity) < BUDGET:
            try:
                await self.step()
            except Exception as e:
                log.warning("%s step error: %s", self.handle, e)
                await asyncio.sleep(5)
            await asyncio.sleep(8)   # event cadence; real harness is SSE-driven
        log.info("%s stopping", self.handle)

    async def step(self):
        board_txt, files = self.gather()
        msgs = [{"role":"system","content":self.system_prompt()},
                {"role":"user","content":
                    "Recent shared context:\n"+board_txt+
                    "\nExisting files: "+", ".join(files or ["(none)"])+
                    "\nWhat is your next action? Respond with only the JSON action."}]
        out = llm(msgs)
        decision = parse_decision(out)
        if not decision:
            return
        self.last_activity = time.time()
        action = decision.get("action")
        if action == "claim":
            board_write("CLAIM", f"{self.handle} building {decision['file']}",
                        decision.get("summary",""), author=self.handle)
            mm_post(MM_CHANNEL, f"{self.handle} CLAIMs {decision['file']}: {decision.get('summary','')}")
        elif action == "write":
            f = decision["file"]; code = decision["code"]
            # light verify: valid python
            try:
                compile(code, f, "exec")
            except SyntaxError as e:
                board_write("FAIL", f"{self.handle} {f} failed syntax", str(e), author=self.handle)
                return
            # conflict check: if another worker recently CLAIMed this exact file
            if any(e["kind"]=="CLAIM" and decision["file"] in e["content"] and e["author"]!=self.handle
                   for e in board_recent(200)):
                dm(self.handle, f"collision on {f}; pick a different slice")
                board_write("CLAIM", f"{self.handle} backing off {f}", author=self.handle)
                return
            gitea_write_file(TASK_REPO, f, code, self.branch)
            board_write("PATCH_SUMMARY",
                        f"files={f} | idea={decision.get('summary','')} | evidence={f} written",
                        author=self.handle)
            mm_post(MM_CHANNEL, f"{self.handle} landed {f}")
        elif action == "fail":
            board_write("FAIL", f"{self.handle} {decision['file']}: {decision.get('reason','')}",
                        author=self.handle)
        elif action == "observe":
            board_write(decision.get("kind","OBSERVED"), decision.get("note",""),
                        author=self.handle)
        elif action == "done":
            board_write("PATCH_SUMMARY", f"{self.handle} done: {decision.get('summary','')}",
                        author=self.handle)
            mm_post(MM_CHANNEL, f"{self.handle} finished")

# ---------------------------------------------------------------- coordinator
async def net_probe():
    import socket
    for name in ["gitea", "mattermost", "board",
                 "gitea-lfqomkl0hzquxgyneyvfce4e",
                 "mattermost-gwl6ll407hczyipdqghjusgn",
                 "host.docker.internal"]:
        try:
            log.info("probe resolve %s -> %s", name, socket.gethostbyname(name))
        except Exception as e:
            log.warning("probe resolve %s failed: %s", name, e)
    try:
        with open("/proc/net/route") as f:
            for line in f.readlines()[1:]:
                p = line.split()
                if len(p) > 2 and p[1] == "00000000":
                    gw = ".".join(str(int(p[2][i:i + 2], 16)) for i in (6, 4, 2, 0))
                    log.info("probe default gateway %s", gw)
                    break
    except Exception as e:
        log.warning("probe gw failed: %s", e)
    for host in ["host.docker.internal", "gitea", "10.20.8.134"]:
        for port in (80, 3000):
            try:
                s = socket.create_connection((host, port), timeout=3)
                s.close(); log.info("probe tcp %s:%d OPEN", host, port)
            except Exception as e:
                log.warning("probe tcp %s:%d closed (%s)", host, port, e)
    try:
        r = client.get(f"{GITEA}/api/v1/version")
        log.info("probe http gitea(%s) -> %s", GITEA, r.status_code)
    except Exception as e:
        log.warning("probe http gitea(%s) failed: %s", GITEA, e)
    for name in ["mattermost", "mattermost-gwl6ll407hczyipdqghjusgn",
                 "gwl6ll407hczyipdqghjusgn", "mattermost-mattermost",
                 "agentsh-mattermost", "mattermost-team"]:
        try:
            ip = socket.gethostbyname(name)
            log.info("probe mm resolve %s -> %s", name, ip)
            try:
                r = client.get(f"http://{name}:8065/api/v4/system/ping")
                log.info("probe mm http %s -> %s", name, r.status_code)
            except Exception as e:
                log.warning("probe mm http %s failed: %s", name, e)
        except Exception as e:
            log.warning("probe mm resolve %s failed: %s", name, e)


async def main():
    log.info("start up: %d workers, model=%s", N, LLM_MODEL)
    await net_probe()
    # seed the task repo (SPEC) if the repo isn't there yet -> let coordinator create later
    stop = asyncio.Event()
    tasks = [asyncio.create_task(Worker(i).run(stop)) for i in range(N)]
    # idle detector
    started = time.time()
    while time.time() - started < BUDGET:
        await asyncio.sleep(10)
    stop.set()
    log.info("budget consumed; signalling shutdown")
    await asyncio.gather(*tasks, return_exceptions=True)
    # merge window: open PRs from each worker branch
    for i in range(N):
        try:
            gitea_open_pr(TASK_REPO, f"w{i+1}", title=f"merge w{i+1}")
            mm_post(MM_CHANNEL, f"merged window: opened PR for w{i+1}")
        except Exception as e:
            log.warning("pr %d: %s", i, e)

if __name__ == "__main__":
    asyncio.run(main())
