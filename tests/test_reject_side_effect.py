import sqlite3
import threading

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import app as m


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(m, "WRITE_DELAY", 0.0)
    with m._STATE_LOCK:
        m._CURRENT_PATH = None
    m.init_db()
    yield


def client_and_headers():
    c = TestClient(m.app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def post_ingress(c, h, payload_fields, strategy=None):
    body = {"subject": "订单入站", "payload_fields": payload_fields}
    if strategy is not None:
        body["strategy"] = strategy
    return c.post("/api/ingress/validate", headers=h, json=body)


def raw_counts():
    with sqlite3.connect(m.DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM ingress_batches").fetchone()[0]
        buffered = conn.execute("SELECT COUNT(*) FROM ingress_batches WHERE result='缓冲'").fetchone()[0]
        rejected = conn.execute("SELECT COUNT(*) FROM ingress_batches WHERE result='拒收'").fetchone()[0]
        passed = conn.execute("SELECT COUNT(*) FROM ingress_batches WHERE result='通过'").fetchone()[0]
        ledger = conn.execute(
            "SELECT COALESCE(SUM(passed),0), COALESCE(SUM(rejected),0), COALESCE(SUM(ingress_total),0) FROM reconcile_ledger"
        ).fetchone()
    return {"total": total, "buffered": buffered, "rejected": rejected, "passed": passed,
            "ledger_passed": ledger[0], "ledger_rejected": ledger[1], "ledger_total": ledger[2]}


# 1. 先走拒收路径复现“通过对账加一”，再断言修复后不推进通过计数。
def test_reject_path_reproduces_legacy_passed_bump_then_stays_unchanged():
    c, h = client_and_headers()
    before = c.get("/api/reconcile", headers=h).json()
    assert before["passed"] == 0

    r = post_ingress(c, h, "order_id,amount")
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    assert r.json()["passed_unchanged"] is True

    counts = raw_counts()
    # 缺陷口径：通过 = 总数 - 缓冲（只排除缓冲），一条拒收即被算成一次通过。
    legacy_passed = counts["total"] - counts["buffered"]
    assert legacy_passed == 1 and counts["passed"] == 0  # 复现“通过对账加一”
    assert counts["ledger_passed"] == 0                  # 落库账册未被拒收推进

    after = c.get("/api/reconcile", headers=h).json()
    assert after["passed"] == 0
    assert after["rejected"] == 1
    assert after["aligned"] is True and after["ledger_ok"] is True
    assert after["reject_side_effect_clean"] is True

    # 再走一次通过路径：通过计数只加在通过批批次上。
    ok = post_ingress(c, h, "order_id,amount,currency")
    assert ok.json()["result"] == "通过"
    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["passed"] == 1 and rec["rejected"] == 1
    assert rec["passed_excl_reject"] == 1 and rec["aligned"] is True


# 2. 副作用失败测：拒收若推进通过计数，必须拒绝并入簿，且整笔回滚。
def test_reject_side_effect_failure_is_refused_and_booked(monkeypatch):
    c, h = client_and_headers()
    real = m._apply_ledger_effect

    def faulty(conn, result, day, now):
        real(conn, result, day, now)
        if result == "拒收":
            conn.execute("UPDATE reconcile_ledger SET passed=passed+1 WHERE day=?", (day,))

    monkeypatch.setattr(m, "_apply_ledger_effect", faulty)
    r = post_ingress(c, h, "order_id,amount")
    assert r.status_code == 409
    assert "副作用偏了" in r.json()["detail"]

    counts = raw_counts()
    assert counts["total"] == 0 and counts["rejected"] == 0  # 回滚，坏账不落库
    assert counts["ledger_passed"] == 0 and counts["ledger_rejected"] == 0

    events = c.get("/api/side-effects", headers=h).json()["items"]
    assert any(e["kind"] == "副作用偏了" for e in events)


# 3. 拒收与通过并发抢写：排队，后到只记路径争用，计数不重不漏。
def test_reject_and_pass_concurrent_writes_queue_and_record_contention(monkeypatch):
    monkeypatch.setattr(m, "WRITE_DELAY", 0.2)
    c, h = client_and_headers()
    tok = h["Authorization"]
    barrier = threading.Barrier(2)
    results, errors = {}, {}

    def run(name, fields):
        try:
            barrier.wait()
            results[name] = m.validate(m.IngressIn(subject="订单入站", payload_fields=fields), authorization=tok)
        except Exception as exc:  # noqa: BLE001
            errors[name] = exc

    t_reject = threading.Thread(target=run, args=("reject", "order_id,amount"))
    t_pass = threading.Thread(target=run, args=("pass", "order_id,amount,currency"))
    t_reject.start(); t_pass.start(); t_reject.join(); t_pass.join()

    assert not errors, errors
    assert results["reject"]["result"] == "拒收"
    assert results["pass"]["result"] == "通过"
    assert sum(1 for r in results.values() if r["contended"]) == 1

    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["passed"] == 1 and rec["rejected"] == 1 and rec["ingress_total"] == 2
    assert rec["aligned"] is True and rec["ledger_ok"] is True

    events = c.get("/api/side-effects", headers=h).json()["items"]
    contention = [e for e in events if e["kind"] == "路径争用"]
    assert len(contention) == 1
    assert "后到只记争用" in contention[0]["detail"]


# 4. 甲乙两条收暑路径各自只用自己的副作支。
def test_strategy_branch_uses_its_own_side_effect_only():
    c, h = client_and_headers()
    assert post_ingress(c, h, "order_id,amount", strategy="甲").json()["result"] == "拒收"

    rows = c.get("/api/ingress", headers=h).json()["items"]
    assert rows[0]["strategy"] == "甲"
    assert rows[0]["reject_snapshot"] is None  # 甲不写拒收快照列
    rejects = c.get("/api/rejects", headers=h).json()["items"]
    assert rejects[0]["snapshot"]["passed"] == 0  # 拒收前值仍可从副作用簿核到
    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["strategy"] == "甲" and rec["snapshot_passed"] == 0 and rec["aligned"] is True


def test_strategy_branch_yi_writes_snapshot_column():
    c, h = client_and_headers()
    assert post_ingress(c, h, "order_id,amount", strategy="乙").json()["result"] == "拒收"
    rows = c.get("/api/ingress", headers=h).json()["items"]
    assert rows[0]["strategy"] == "乙"
    assert rows[0]["reject_snapshot"]["passed"] == 0
    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["strategy"] == "乙" and rec["aligned"] is True
    assert "排除拒收批次现场重算" in rec["reconcile_path"]


# 5. 收暑当天禁止改口：偏离即拒绝并记副作用偏了。
def test_strategy_deviation_same_day_is_refused_and_booked():
    c, h = client_and_headers()
    assert post_ingress(c, h, "order_id,amount", strategy="乙").status_code == 200
    r = post_ingress(c, h, "order_id,amount", strategy="甲")
    assert r.status_code == 409 and "收暑当天禁止改口" in r.json()["detail"]

    event_kinds = [e["kind"] for e in c.get("/api/side-effects", headers=h).json()["items"]]
    assert "副作用偏了" in event_kinds
    assert raw_counts()["total"] == 1  # 偏离的请求未落批次

    bad = post_ingress(c, h, "order_id,amount", strategy="丙")
    assert bad.status_code == 409


# 6. 五处同源：入站列表、拒收详情、复核队列、策略附页、对账账册对同一批拒收一致。
def test_reject_views_share_one_source():
    c, h = client_and_headers()
    for fields in ("order_id", "order_id,amount"):
        assert post_ingress(c, h, fields).json()["result"] == "拒收"
    assert post_ingress(c, h, "order_id,amount,currency").json()["result"] == "通过"

    ingress_ids = {b["id"] for b in c.get("/api/ingress", headers=h).json()["items"] if b["result"] == "拒收"}
    reject_ids = {b["batch_id"] for b in c.get("/api/rejects", headers=h).json()["items"]}
    queue_ids = {b["batch_id"] for b in c.get("/api/review-queue", headers=h).json()["items"]}
    assert ingress_ids == reject_ids == queue_ids == {1, 2}

    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["rejected"] == len(reject_ids) == 2
    policy = c.get("/api/reject-policy", headers=h).json()
    assert policy["book_name"] == m.SIDE_EFFECT_BOOK
    assert policy["strategy"] == rec["strategy"] == "乙"
    assert any(e["kind"] == "收暑选定" for e in policy["events"])


# 7. 拒收不得改正式启用，也不得与通过一并成立。
def test_reject_cannot_enable_contract_or_merge_with_pass():
    c, h = client_and_headers()
    before = {x["version"]: x for x in c.get("/api/contracts", headers=h).json()["items"]}
    post_ingress(c, h, "order_id")
    after = {x["version"]: x for x in c.get("/api/contracts", headers=h).json()["items"]}
    assert before == after
    assert after["v3"]["track"] == "正式" and after["v3"]["status"] == "启用"

    with sqlite3.connect(m.DB_PATH) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) "
                         "VALUES ('订单入站','order_id','v3','拒收通过','坏结果','2026-11-18T00:00:00')")
