import hashlib, json, secrets, sqlite3, threading, time
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

VALID_STRATEGIES = ("甲", "乙")
DEFAULT_STRATEGY = "乙"  # 乙：拒收写拒收快照列，对账排除拒收批次现场重算
SIDE_EFFECT_BOOK = "拒收副作用簿"

# 入站写路径共用一道门：拒收与通过同时抢写必须排队，后到者只记路径争用。
INGRESS_GATE = threading.Lock()
_STATE_LOCK = threading.Lock()
_CURRENT_PATH: str | None = None
WRITE_DELAY = 0.0  # 测试注入的排队窗口；生产默认 0


class SideEffectViolation(Exception):
    """拒收副作用越界：拒收不得推进通过计数。"""


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def _columns(conn, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_column(conn, table: str, column: str, decl: str):
    if column not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL,
          strategy TEXT,
          reject_snapshot TEXT
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reconcile_ledger(
          day TEXT PRIMARY KEY,
          passed INTEGER NOT NULL DEFAULT 0,
          rejected INTEGER NOT NULL DEFAULT 0,
          buffered INTEGER NOT NULL DEFAULT 0,
          ingress_total INTEGER NOT NULL DEFAULT 0,
          strategy TEXT NOT NULL DEFAULT '乙',
          updated_at TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS reject_side_effect_book(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          day TEXT UNIQUE NOT NULL,
          strategy TEXT NOT NULL,
          chosen_at TEXT NOT NULL,
          chosen_by TEXT,
          note TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS side_effect_events(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          path TEXT,
          batch_id INTEGER,
          day TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS trg_ingress_result_guard_ins
        BEFORE INSERT ON ingress_batches
        FOR EACH ROW WHEN NEW.result NOT IN ('通过','缓冲','拒收')
        BEGIN
          SELECT RAISE(ABORT, '非法入站结果：拒收与通过不得一并成立');
        END;
        CREATE TRIGGER IF NOT EXISTS trg_ingress_result_guard_upd
        BEFORE UPDATE OF result ON ingress_batches
        FOR EACH ROW WHEN NEW.result NOT IN ('通过','缓冲','拒收')
        BEGIN
          SELECT RAISE(ABORT, '非法入站结果：拒收与通过不得一并成立');
        END;
        """)
        # 现行库原地加列，不换库文件。
        _ensure_column(conn, "ingress_batches", "strategy", "TEXT")
        _ensure_column(conn, "ingress_batches", "reject_snapshot", "TEXT")
        _ensure_column(conn, "reconcile_ledger", "strategy", "TEXT NOT NULL DEFAULT '乙'")
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3", "订单入站", "严格", "正式", "启用", "order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow", "订单入站", "兼容", "影子", "试跑", "order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2", "库存入站", "严格", "正式", "停用", "sku,qty"))
        _backfill_ledger(conn)


def _backfill_ledger(conn):
    """已有批次但没有账册时按天回填，保证计数落库可核。"""
    if conn.execute("SELECT COUNT(*) c FROM reconcile_ledger").fetchone()["c"]:
        return
    rows = conn.execute("SELECT result, created_at FROM ingress_batches").fetchall()
    if not rows:
        return
    per_day: dict[str, dict[str, int]] = {}
    for r in rows:
        day = (r["created_at"] or "")[:10]
        d = per_day.setdefault(day, {"passed": 0, "buffered": 0, "rejected": 0, "total": 0})
        d["total"] += 1
        if r["result"] == "通过":
            d["passed"] += 1
        elif r["result"] == "缓冲":
            d["buffered"] += 1
        elif r["result"] == "拒收":
            d["rejected"] += 1
    now = datetime.now().isoformat(timespec="seconds")
    for day, d in per_day.items():
        conn.execute("INSERT INTO reconcile_ledger(day,passed,rejected,buffered,ingress_total,strategy,updated_at) VALUES (?,?,?,?,?,?,?)",
                     (day, d["passed"], d["rejected"], d["buffered"], d["total"], DEFAULT_STRATEGY, now))


def _event(conn, kind: str, detail: str, *, path: str | None = None, batch_id: int | None = None,
           day: str, now: str):
    conn.execute("INSERT INTO side_effect_events(kind,detail,path,batch_id,day,created_at) VALUES (?,?,?,?,?,?)",
                 (kind, detail, path, batch_id, day, now))


def _today() -> str:
    return datetime.now().date().isoformat()


def _ledger_row(conn, day: str, strategy: str):
    conn.execute("INSERT OR IGNORE INTO reconcile_ledger(day,passed,rejected,buffered,ingress_total,strategy,updated_at) VALUES (?,?,?,?,?,?,?)",
                 (day, 0, 0, 0, 0, strategy, ""))
    return conn.execute("SELECT * FROM reconcile_ledger WHERE day=?", (day,)).fetchone()


def _ledger_counts(row) -> dict:
    return {"passed": row["passed"], "rejected": row["rejected"],
            "buffered": row["buffered"], "ingress_total": row["ingress_total"]}


def _current_strategy(conn, day: str) -> str:
    book = conn.execute("SELECT strategy FROM reject_side_effect_book WHERE day=?", (day,)).fetchone()
    return book["strategy"] if book else DEFAULT_STRATEGY


def _resolve_strategy(conn, day: str, requested: str | None, user: str, now: str) -> str:
    """收暑选定后写进拒收副作用簿；当天禁止改口，偏离即拒并记副作用偏了。"""
    book = conn.execute("SELECT * FROM reject_side_effect_book WHERE day=?", (day,)).fetchone()
    if book:
        if requested and requested != book["strategy"]:
            detail = f"副作用偏了：当天已选{book['strategy']}路径，收暑当天禁止改口（请求{requested}）"
            _event(conn, "副作用偏了", detail, path="策略附页", day=day, now=now)
            conn.commit()
            raise HTTPException(409, detail)
        return book["strategy"]
    if requested and requested not in VALID_STRATEGIES:
        detail = f"副作用偏了：非法收暑{requested}，只允许甲/乙路径"
        _event(conn, "副作用偏了", detail, path="策略附页", day=day, now=now)
        conn.commit()
        raise HTTPException(409, detail)
    strategy = requested or DEFAULT_STRATEGY
    try:
        conn.execute("INSERT INTO reject_side_effect_book(day,strategy,chosen_at,chosen_by,note) VALUES (?,?,?,?,?)",
                     (day, strategy, now, user, f"{SIDE_EFFECT_BOOK}：收暑选定{strategy}路径，当天禁止改口"))
    except sqlite3.IntegrityError:
        # 另一条写路径刚选定当天收暑：以簿上记录为准，绝不并行混用副作支。
        book = conn.execute("SELECT * FROM reject_side_effect_book WHERE day=?", (day,)).fetchone()
        if book["strategy"] != strategy:
            detail = f"副作用偏了：当天已选{book['strategy']}路径，收暑当天禁止改口（请求{strategy}）"
            _event(conn, "副作用偏了", detail, path="策略附页", day=day, now=now)
            conn.commit()
            raise HTTPException(409, detail)
        return book["strategy"]
    _event(conn, "收暑选定", f"收暑选定{strategy}路径，写入{SIDE_EFFECT_BOOK}", path="策略附页", day=day, now=now)
    return strategy


def _enter_write() -> tuple[bool, str | None]:
    """非阻塞探路再排队；后到者只记录路径争用，不另起一支通过计数。"""
    global _CURRENT_PATH
    with _STATE_LOCK:
        acquired = INGRESS_GATE.acquire(blocking=False)
        if acquired:
            _CURRENT_PATH = "校验中"
            other = None
        else:
            other = _CURRENT_PATH
    if not acquired:
        INGRESS_GATE.acquire()
        with _STATE_LOCK:
            _CURRENT_PATH = "校验中"
    return (not acquired), other


def _mark_path(path: str):
    global _CURRENT_PATH
    with _STATE_LOCK:
        _CURRENT_PATH = path


def _hold_write_window():
    if WRITE_DELAY:
        time.sleep(WRITE_DELAY)


def _exit_write():
    global _CURRENT_PATH
    with _STATE_LOCK:
        _CURRENT_PATH = None
    INGRESS_GATE.release()


def _apply_ledger_effect(conn, result: str, day: str, now: str):
    """按入站结果推进落库账册；拒收分支绝不触碰通过计数。"""
    if result == "通过":
        conn.execute("UPDATE reconcile_ledger SET passed=passed+1, ingress_total=ingress_total+1, updated_at=? WHERE day=?", (now, day))
    elif result == "缓冲":
        conn.execute("UPDATE reconcile_ledger SET buffered=buffered+1, ingress_total=ingress_total+1, updated_at=? WHERE day=?", (now, day))
    else:
        # 拒收只追加拒收批次与原因，绝不推进通过计数。
        conn.execute("UPDATE reconcile_ledger SET rejected=rejected+1, ingress_total=ingress_total+1, updated_at=? WHERE day=?", (now, day))


def _record_batch(conn, *, subject: str, payload_fields: str, version: str, result: str, detail: str,
                  now: str, strategy: str, contended: bool, other_path: str | None) -> tuple[int, dict, dict]:
    day = now[:10]
    row = _ledger_row(conn, day, strategy)
    before = _ledger_counts(row)
    snapshot_json = json.dumps(before, ensure_ascii=False)
    # 乙路径把拒收前值写进拒收快照列；甲路径只把前值留在副作用簿事件里。两支不混用。
    reject_snapshot = snapshot_json if (result == "拒收" and strategy == "乙") else None
    cur = conn.execute(
        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,strategy,reject_snapshot) VALUES (?,?,?,?,?,?,?,?)",
        (subject, payload_fields, version, result, detail, now, strategy, reject_snapshot))
    batch_id = cur.lastrowid
    _apply_ledger_effect(conn, result, day, now)
    if result == "缓冲":
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, "缺字段进双写缓冲"))
    elif result == "拒收" and strategy == "甲":
        _event(conn, "拒收快照", snapshot_json, path="复核队列", batch_id=batch_id, day=day, now=now)
    _event(conn, "入站", f"{result}：{detail}", path="入站列表", batch_id=batch_id, day=day, now=now)
    if contended:
        prior = conn.execute("SELECT result FROM ingress_batches WHERE id<? ORDER BY id DESC LIMIT 1",
                             (batch_id,)).fetchone()
        prior_path = prior["result"] if prior else (other_path or "未识别路径")
        _event(conn, "路径争用",
               f"路径争用：{prior_path}先到，{result}后到，已排队；后到只记争用，不另起通过计数",
               path="复核队列", batch_id=batch_id, day=day, now=now)
    after = _ledger_counts(_ledger_row(conn, day, strategy))
    if result == "拒收" and after["passed"] != before["passed"]:
        # 拒收与通过不得一并成立：整笔回滚，坏账不落库。
        raise SideEffectViolation(f"拒收推进了通过计数（{before['passed']}→{after['passed']}）")
    return batch_id, before, after


@app.on_event("startup")
def _s():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
        ledger = c.execute("SELECT COUNT(*) c FROM reconcile_ledger").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n,
            "ledger_rows": ledger, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}


class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present
    strategy: str | None = None  # 收暑选定；当天选定后不可改口


@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    now = datetime.now().isoformat(timespec="seconds")
    day = now[:10]
    contended, other_path = _enter_write()
    try:
        with connect() as c:
            ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
            if not ctr:
                raise HTTPException(404, "无启用正式契约")
            strategy = _resolve_strategy(c, day, body.strategy, user, now)
            need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
            missing = sorted(need - fields)
            if missing and ctr["mode"] == "兼容":
                _mark_path("缓冲")
                _hold_write_window()
                batch_id, before, after = _record_batch(
                    c, subject=body.subject, payload_fields=body.payload_fields, version=ctr["version"],
                    result="缓冲", detail="缺字段:" + ",".join(missing), now=now,
                    strategy=strategy, contended=contended, other_path=other_path)
                return {"result": "缓冲", "version": ctr["version"], "missing": missing,
                        "batch_id": batch_id, "strategy": strategy, "snapshot": before,
                        "passed": after["passed"], "contended": contended,
                        "side_effect_book": SIDE_EFFECT_BOOK}
            if missing:
                _mark_path("拒收")
                _hold_write_window()
                batch_id, before, after = _record_batch(
                    c, subject=body.subject, payload_fields=body.payload_fields, version=ctr["version"],
                    result="拒收", detail="缺字段:" + ",".join(missing), now=now,
                    strategy=strategy, contended=contended, other_path=other_path)
                return {"result": "拒收", "version": ctr["version"], "missing": missing,
                        "batch_id": batch_id, "strategy": strategy, "snapshot": before,
                        "passed": after["passed"],
                        "passed_unchanged": before["passed"] == after["passed"],
                        "contended": contended, "side_effect_book": SIDE_EFFECT_BOOK}
            _mark_path("通过")
            _hold_write_window()
            batch_id, before, after = _record_batch(
                c, subject=body.subject, payload_fields=body.payload_fields, version=ctr["version"],
                result="通过", detail="字段齐全", now=now,
                strategy=strategy, contended=contended, other_path=other_path)
            return {"result": "通过", "version": ctr["version"], "batch_id": batch_id,
                    "strategy": strategy, "snapshot": before, "passed": after["passed"],
                    "contended": contended, "side_effect_book": SIDE_EFFECT_BOOK}
    except SideEffectViolation as exc:
        # 拒收副作用越界：整笔事务回滚，拒绝并记“副作用偏了”。
        detail = f"副作用偏了：{exc}，已拒绝并入拒收副作用簿"
        with connect() as c:
            _event(c, "副作用偏了", detail, path="复核队列", day=day, now=now)
            c.commit()
        raise HTTPException(409, detail)
    finally:
        _exit_write()


@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        items = []
        for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall():
            d = dict(r)
            d["reject_snapshot"] = json.loads(d["reject_snapshot"]) if d.get("reject_snapshot") else None
            items.append(d)
        return {"items": items}


@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items": [dict(r) for r in rows]}


def _reject_snapshot_for(conn, batch: sqlite3.Row) -> dict | None:
    """拒收前值：乙读拒收快照列，甲读副作用簿事件；两支不混用。"""
    if batch["strategy"] == "乙":
        return json.loads(batch["reject_snapshot"]) if batch["reject_snapshot"] else None
    if batch["strategy"] == "甲":
        ev = conn.execute("SELECT detail FROM side_effect_events WHERE kind='拒收快照' AND batch_id=? ORDER BY seq DESC LIMIT 1",
                          (batch["id"],)).fetchone()
        return json.loads(ev["detail"]) if ev and ev["detail"] else None
    return None


@app.get("/api/rejects")
def rejects(authorization: str | None = Header(default=None)):
    """拒收详情；与入站列表、对账账册同源。"""
    require_user(authorization)
    with connect() as c:
        items = []
        for r in c.execute("SELECT * FROM ingress_batches WHERE result='拒收' ORDER BY id DESC").fetchall():
            items.append({"batch_id": r["id"], "subject": r["subject"], "payload_fields": r["payload_fields"],
                          "version": r["contract_version"], "reason": r["detail"],
                          "strategy": r["strategy"], "snapshot": _reject_snapshot_for(c, r),
                          "created_at": r["created_at"], "review_status": "待复核"})
        return {"items": items}


@app.get("/api/review-queue")
def review_queue(authorization: str | None = Header(default=None)):
    """复核队列由同一批拒收批次派生，不另存计数。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM ingress_batches WHERE result='拒收' ORDER BY id").fetchall()
        return {"items": [{"batch_id": r["id"], "subject": r["subject"], "reason": r["detail"],
                           "strategy": r["strategy"], "status": "待复核",
                           "created_at": r["created_at"],
                           "snapshot": _reject_snapshot_for(c, r)} for r in rows]}


@app.get("/api/reject-policy")
def reject_policy(authorization: str | None = Header(default=None)):
    """策略附页：收暑选定记录 + 偏离/争用事件。"""
    require_user(authorization)
    today = _today()
    with connect() as c:
        books = [dict(r) for r in c.execute("SELECT * FROM reject_side_effect_book ORDER BY day DESC, id DESC").fetchall()]
        events = [dict(r) for r in c.execute("SELECT * FROM side_effect_events ORDER BY seq DESC").fetchall()]
        today_row = c.execute("SELECT * FROM reject_side_effect_book WHERE day=?", (today,)).fetchone()
    return {"book_name": SIDE_EFFECT_BOOK, "today": today,
            "strategy": today_row["strategy"] if today_row else None,
            "default_strategy": DEFAULT_STRATEGY, "book": books,
            "events": [e for e in events if e["kind"] in ("收暑选定", "副作用偏了", "路径争用")]}


@app.get("/api/side-effects")
def side_effects(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM side_effect_events ORDER BY seq DESC").fetchall()]}


@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    today = _today()
    with connect() as c:
        led = c.execute("""SELECT COALESCE(SUM(passed),0) passed, COALESCE(SUM(rejected),0) rejected,
                                  COALESCE(SUM(buffered),0) buffered, COALESCE(SUM(ingress_total),0) ingress_total
                           FROM reconcile_ledger""").fetchone()
        rows = c.execute("SELECT * FROM ingress_batches ORDER BY id").fetchall()
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        strategy = _current_strategy(c, today)
        ledger_rows = [dict(r) for r in c.execute("SELECT * FROM reconcile_ledger ORDER BY day").fetchall()]

        # 按天走账：拒收前值快照必须等于当天、该笔之前的账册值。
        run_by_day: dict[str, dict[str, int]] = {}
        totals = {"passed": 0, "rejected": 0, "buffered": 0, "ingress_total": 0}
        clean, violations, latest_snapshot = True, [], None
        for r in rows:
            day = (r["created_at"] or "")[:10]
            run = run_by_day.setdefault(day, {"passed": 0, "rejected": 0, "buffered": 0, "ingress_total": 0})
            if r["result"] == "拒收" and r["strategy"] in VALID_STRATEGIES:
                snap = _reject_snapshot_for(c, r)
                ok = bool(snap) and all(snap.get(k) == run[k] for k in run)
                if not ok:
                    clean = False
                    violations.append({"batch_id": r["id"], "strategy": r["strategy"], "snapshot": snap,
                                       "expected": dict(run)})
                if snap:
                    latest_snapshot = snap
            run["ingress_total"] += 1
            totals["ingress_total"] += 1
            if r["result"] == "通过":
                run["passed"] += 1
                totals["passed"] += 1
            elif r["result"] == "拒收":
                run["rejected"] += 1
                totals["rejected"] += 1
            elif r["result"] == "缓冲":
                run["buffered"] += 1
                totals["buffered"] += 1

        passed_live, rejected_live = totals["passed"], totals["rejected"]
        buffered_live, total_live = totals["buffered"], totals["ingress_total"]
        # 乙路径：对账排除拒收批次现场重算；通过数只可能来自“通过”批次。
        passed_excl_reject = total_live - rejected_live - buffered_live
        ledger_ok = (led["passed"] == passed_live and led["rejected"] == rejected_live
                     and led["buffered"] == buffered_live and led["ingress_total"] == total_live
                     and buffered_live == buf_rows)
        if strategy == "乙":
            path_name = "乙-拒收写拒收快照列，对账排除拒收批次现场重算"
            path_ok = clean and passed_excl_reject == passed_live
        else:
            path_name = "甲-拒收只追加批次与原因，对账快照保持拒收前值"
            path_ok = clean
        aligned = ledger_ok and path_ok

    return {"ingress_total": led["ingress_total"], "passed": led["passed"],
            "buffered": led["buffered"], "rejected": led["rejected"],
            "buffer_rows": buf_rows, "aligned": aligned,
            "ledger_ok": ledger_ok, "reconcile_path": path_name,
            "reject_side_effect_clean": clean,
            "reject_side_effect_violations": violations,
            "strategy": strategy, "snapshot": latest_snapshot,
            "snapshot_passed": (latest_snapshot or {}).get("passed"),
            "passed_live": passed_live, "passed_excl_reject": passed_excl_reject,
            "rejected_live": rejected_live, "buffered_live": buffered_live,
            "ingress_total_live": total_live, "buffer_rows_live": buf_rows,
            "ledger_persisted": True, "ledger": ledger_rows,
            "side_effect_book": SIDE_EFFECT_BOOK}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
