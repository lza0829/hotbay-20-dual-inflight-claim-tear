"""排队串行占坑口径的闭合校验（服务层 + 并发 + HTTP 端到端）。

运行：cd backend && python3 tests/migrate_queue_check.py
覆盖：
  1. 发起即占坑：占坑申请与温层改写同成败
  2. 占坑中再发起：整笔入队、回执告队次、温层不动、回执成功必已入队
  3. 并发双发起：不得双占坑改温层（服务层线程 + HTTP 并发各验一次）
  4. 完结升占坑：按单号次序可重放；失败回滚温层；同层等候单升占坑被拒记拒因
  5. 表面同刷：迁移单表 / 分区温层列 / 总览徽章 / 回执 同出一库
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

TMP = Path(tempfile.mkdtemp(prefix="hotbay-check-"))
os.environ["HOTBAY_DB"] = str(TMP / "svc.db")

from fastapi import HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.db import db  # noqa: E402
from app.main import app  # noqa: E402  （导入即建库播种）
from app.services import migrate as svc  # noqa: E402

FAILED = []
client = TestClient(app)


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f" | {extra}" if extra else ""))
    if not cond:
        FAILED.append(name)


def tier_of(pid):
    with db() as conn:
        return conn.execute("SELECT tier FROM partitions WHERE id=?", (pid,)).fetchone()["tier"]


def job_of(jid):
    with db() as conn:
        return conn.execute("SELECT * FROM migrate_jobs WHERE id=?", (jid,)).fetchone()


def invariants(tag):
    """全库闭合不变量：单占坑、标记与温层不分叉、有等候必有占坑。"""
    with db() as conn:
        rows = conn.execute(
            "SELECT partition_id, COUNT(*) c FROM migrate_jobs WHERE status='占坑中' GROUP BY partition_id"
        ).fetchall()
        check(f"{tag}: 同分区至多一笔占坑", all(r["c"] == 1 for r in rows),
              str([dict(r) for r in rows]))
        div = conn.execute(
            '''SELECT j.id FROM migrate_jobs j JOIN partitions p ON p.id=j.partition_id
               WHERE j.status='占坑中' AND p.tier <> j.to_tier'''
        ).fetchall()
        check(f"{tag}: 占坑标记与温层不分叉", not div, str([dict(r) for r in div]))
        lone = conn.execute(
            '''SELECT partition_id FROM migrate_jobs WHERE status='等候中'
               GROUP BY partition_id
               HAVING (SELECT COUNT(*) FROM migrate_jobs j2
                       WHERE j2.partition_id=migrate_jobs.partition_id AND j2.status='占坑中') = 0'''
        ).fetchall()
        check(f"{tag}: 有等候必有在途占坑", not lone, str([dict(r) for r in lone]))


def surfaces(tag, pid):
    """迁移单表 / 分区温层列 / 总览徽章 同源核对（走真实 API）。"""
    jobs = client.get("/api/迁移").json()
    parts = {p["id"]: p for p in client.get("/api/分区").json()}
    dash = client.get("/api/总览").json()
    p = parts[pid]
    hold_jobs = [j for j in jobs if j["status"] == "占坑中" and j["partition_id"] == pid]
    wait_jobs = [j for j in jobs if j["status"] == "等候中" and j["partition_id"] == pid]
    check(f"{tag}: 分区占坑标记==单表占坑",
          (p["hold_job_id"] or None) == (hold_jobs[0]["id"] if hold_jobs else None),
          f"hold_job_id={p['hold_job_id']}")
    check(f"{tag}: 分区温层列==占坑目标层", not hold_jobs or p["tier"] == hold_jobs[0]["to_tier"],
          f"tier={p['tier']}")
    check(f"{tag}: 分区等候数==单表等候数", p["wait_count"] == len(wait_jobs),
          f"{p['wait_count']} vs {len(wait_jobs)}")
    check(f"{tag}: 总览占坑徽章==单表占坑数",
          dash["迁移占坑中"] == len([j for j in jobs if j["status"] == "占坑中"]))
    check(f"{tag}: 总览等候徽章==单表等候数",
          dash["迁移等候中"] == len([j for j in jobs if j["status"] == "等候中"]))
    poss = sorted(j["queue_pos"] for j in wait_jobs)
    check(f"{tag}: 等候队次连续无重", poss == list(range(1, len(poss) + 1)), str(poss))


def main():
    with db() as conn:
        pids = [r["id"] for r in conn.execute(
            "SELECT id FROM partitions WHERE status='在册' ORDER BY id"
        ).fetchall()]
    pid1, pid2, pid3 = pids[0], pids[1], pids[2]
    tier1 = tier_of(pid1)  # 热档

    # 1) 发起即占坑：占坑与温层改写同成败
    r1 = svc.start_migrate(pid1, "冷档")
    check("发起: 首笔占坑", r1["status"] == "占坑中" and r1["from_tier"] == tier1, str(r1))
    check("发起: 温层同单改写", tier_of(pid1) == "冷档", f"tier={tier_of(pid1)}")

    # 2) 占坑中再发起：整笔入队，温层不动，回执告队次，回执成功必已入队
    r2 = svc.start_migrate(pid1, "温档")
    check("再发起: 入队等候", r2["status"] == "等候中" and r2["queue_pos"] == 1
          and r2["hold_job_id"] == r1["job_id"], str(r2))
    r3 = svc.start_migrate(pid1, "热档")
    check("再发起: 队次递增", r3["status"] == "等候中" and r3["queue_pos"] == 2, str(r3))
    check("再发起: 回执成功必已入队",
          job_of(r2["job_id"])["status"] == "等候中" and job_of(r3["job_id"])["status"] == "等候中")
    check("再发起: 等候单不改温层", tier_of(pid1) == "冷档", f"tier={tier_of(pid1)}")
    invariants("再发起")
    surfaces("再发起", pid1)

    # 3) 并发双发起（服务层线程，异目标）：恰一笔占坑，另一笔整笔入队
    barrier = threading.Barrier(2)

    def race(target):
        barrier.wait()
        return svc.start_migrate(pid2, target)

    with ThreadPoolExecutor(2) as ex:
        rs = list(ex.map(race, ["冷档", "热档"]))
    st = sorted(r["status"] for r in rs)
    check("并发双发起: 一笔占坑一笔等候", st == ["占坑中", "等候中"], str(rs))
    open_job = None
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM migrate_jobs WHERE partition_id=? AND status='占坑中'", (pid2,)
        ).fetchall()
        open_job = rows[0] if rows else None
    check("并发双发起: 单占坑且温层只随占坑改写",
          open_job is not None and tier_of(pid2) == open_job["to_tier"],
          f"tier={tier_of(pid2)}")
    invariants("并发双发起")
    surfaces("并发双发起", pid2)
    # 清场：顺次完结 pid2 的占坑链
    while open_job:
        f = svc.finish_migrate(open_job["id"], True)
        open_job = job_of(f["promoted"]["job_id"]) if f["promoted"] else None
    invariants("并发清场")

    # 4) 完结升占坑：成功完结 → 按单号升占坑并同单改温层
    f1 = svc.finish_migrate(r1["job_id"], True)
    check("完结: 首笔完成", f1["status"] == "已完成" and f1["tier"] == "冷档", str(f1))
    check("完结: 次笔升占坑", bool(f1["promoted"]) and f1["promoted"]["job_id"] == r2["job_id"],
          str(f1.get("promoted")))
    j2 = job_of(r2["job_id"])
    check("完结: 升占坑同单改温层",
          tier_of(pid1) == "温档" and j2["status"] == "占坑中" and j2["from_tier"] == "冷档",
          f"tier={tier_of(pid1)} from={j2['from_tier']}")
    invariants("完结")
    surfaces("完结", pid1)

    # 5) 失败回滚：温层回卷与关单同事务；余下等候单按序升占坑
    f2 = svc.finish_migrate(r2["job_id"], False)
    check("回滚: 占坑单已失败", f2["status"] == "已失败" and f2["tier"] == "冷档", str(f2))
    j3 = job_of(r3["job_id"])
    check("回滚: 升占坑见到回卷后温层", j3["from_tier"] == "冷档", f"from={j3['from_tier']}")
    check("回滚: 下一等候单升占坑", bool(f2["promoted"]) and f2["promoted"]["job_id"] == r3["job_id"],
          str(f2.get("promoted")))
    check("回滚: 升占坑再改温层", tier_of(pid1) == "热档", f"tier={tier_of(pid1)}")
    f3 = svc.finish_migrate(r3["job_id"], True)
    check("收尾: 队列清空", f3["promoted"] is None and not f3["rejected"], str(f3))
    invariants("回滚")
    surfaces("回滚", pid1)

    # 6) 升占坑被拒：前单失败回滚后，同层等候单整笔拒绝并记拒因
    s1 = svc.start_migrate(pid3, "冷档")
    s2 = svc.start_migrate(pid3, "热档")
    check("拒因铺垫: 占坑+等候就位", s1["status"] == "占坑中" and s2["status"] == "等候中",
          f"{s1['status']}/{s2['status']}")
    g1 = svc.finish_migrate(s1["job_id"], False)
    check("拒因: 同层等候单升占坑被拒",
          bool(g1["rejected"]) and g1["rejected"][0]["job_id"] == s2["job_id"] and g1["promoted"] is None,
          str(g1.get("rejected")))
    js2 = job_of(s2["job_id"])
    check("拒因: 拒因落账可查", js2["status"] == "已拒绝" and "升占坑被拒" in js2["note"], js2["note"])
    check("拒因: 温层保持回滚后原层", tier_of(pid3) == "热档", f"tier={tier_of(pid3)}")
    invariants("拒因")
    surfaces("拒因", pid3)

    # 7) 拒单形态：同层发起整笔拒绝，不产生任何单
    try:
        svc.start_migrate(pid1, tier_of(pid1))
        check("拒单: 同层发起被拒", False)
    except HTTPException as e:
        check("拒单: 同层发起被拒", "目标温层与现行相同" in e.detail, e.detail)

    # 8) HTTP 端到端：uvicorn 子进程，先占坑后 6 路并发入队
    http_db = TMP / "http.db"
    env = dict(os.environ, HOTBAY_DB=str(http_db))
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", "11399"],
        cwd=BACKEND, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    def call(path, payload):
        req = urllib.request.Request(
            "http://127.0.0.1:11399" + quote(path),
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())

    try:
        for _ in range(60):
            try:
                urllib.request.urlopen("http://127.0.0.1:11399" + quote("/api/健康"), timeout=1)
                break
            except Exception:
                time.sleep(0.5)
        hconn = sqlite3.connect(str(http_db))
        hconn.row_factory = sqlite3.Row
        hp = hconn.execute(
            "SELECT id FROM partitions WHERE status='在册' ORDER BY id LIMIT 1"
        ).fetchone()["id"]

        first = call("/api/迁移", {"partition_id": hp, "to_tier": "冷档"})
        check("HTTP: 首笔占坑", first["status"] == "占坑中", str(first))

        with ThreadPoolExecutor(6) as ex:
            receipts = list(ex.map(
                lambda i: call("/api/迁移", {"partition_id": hp, "to_tier": "温档" if i % 2 == 0 else "热档"}),
                range(6),
            ))
        check("HTTP并发: 全部整笔入队", all(r["status"] == "等候中" for r in receipts),
              str([r["status"] for r in receipts]))
        poss = sorted(r["queue_pos"] for r in receipts)
        check("HTTP并发: 队次1..6不重不漏", poss == [1, 2, 3, 4, 5, 6], str(poss))
        ids_in_db = {
            r["id"] for r in hconn.execute("SELECT id FROM migrate_jobs WHERE partition_id=?", (hp,)).fetchall()
        }
        check("HTTP并发: 回执成功必已入队",
          all(r["job_id"] in ids_in_db for r in receipts))
        holds = hconn.execute(
            "SELECT COUNT(*) c FROM migrate_jobs WHERE partition_id=? AND status='占坑中'", (hp,)
        ).fetchone()["c"]
        tier = hconn.execute("SELECT tier FROM partitions WHERE id=?", (hp,)).fetchone()["tier"]
        check("HTTP并发: 单占坑且温层一致", holds == 1 and tier == "冷档", f"holds={holds} tier={tier}")

        # 顺次完结：升占坑次序必须按单号可重放（被拒者跳过且记拒因）
        promoted_order = []
        while True:
            row = hconn.execute(
                "SELECT id FROM migrate_jobs WHERE partition_id=? AND status='占坑中'", (hp,)
            ).fetchone()
            if not row:
                break
            body = call(f"/api/迁移/{row['id']}/完成", {"success": True})
            if body["promoted"]:
                promoted_order.append(body["promoted"]["job_id"])
        check("HTTP顺次完结: 升占坑次序按单号递增",
              promoted_order == sorted(promoted_order), str(promoted_order))
        left = hconn.execute(
            "SELECT COUNT(*) c FROM migrate_jobs WHERE partition_id=? AND status IN ('占坑中','等候中')", (hp,)
        ).fetchone()["c"]
        rej = hconn.execute(
            "SELECT COUNT(*) c FROM migrate_jobs WHERE partition_id=? AND status='已拒绝' AND note<>''", (hp,)
        ).fetchone()["c"]
        check("HTTP顺次完结: 队列清空", left == 0)
        check("HTTP顺次完结: 被拒者拒因落账",
              rej == hconn.execute(
                  "SELECT COUNT(*) c FROM migrate_jobs WHERE partition_id=? AND status='已拒绝'", (hp,)
              ).fetchone()["c"])
        hconn.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    if FAILED:
        print(f"未闭合 {len(FAILED)} 项: {FAILED}")
        sys.exit(1)
    print("全部闭合")


main()
