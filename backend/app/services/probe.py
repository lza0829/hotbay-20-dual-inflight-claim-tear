from __future__ import annotations
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from fastapi import HTTPException

from app.db import db, db_write
from app.services.core import now, partition_row
from app.services import migrate

# 并发探针：以真实写口 migrate.start_migrate 多线程齐发，
# 把「双锁同改 / 第二笔假成功 / 锁行温层分叉 / 并发半截」四类夹具
# 收敛成同库可查的探针流水（probe_runs + probe_events）。
# 全局口径 A：探针只承认「读路径最多一把温层锁」，任一不变量破口即判未闭合。

OPEN = "占坑中"
WAITING = "等候中"


def _ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


class _EventLog:
    def __init__(self, run_id: int):
        self.run_id = run_id
        self._items: list[dict] = []
        self._lock = threading.Lock()
        self._seq = 0

    def add(self, event: str, detail: dict | str, thread_label: str = "", job_id: int | None = None):
        with self._lock:
            self._seq += 1
            self._items.append({
                "run_id": self.run_id,
                "seq": self._seq,
                "thread_label": thread_label,
                "job_id": job_id,
                "event": event,
                "detail": detail if isinstance(detail, str) else json.dumps(detail, ensure_ascii=False),
                "at": _ts(),
            })

    def flush(self, conn):
        conn.executemany(
            "INSERT INTO probe_events(run_id, seq, thread_label, job_id, event, detail, at) "
            "VALUES (:run_id,:seq,:thread_label,:job_id,:event,:detail,:at)",
            self._items,
        )


def _snapshot(conn, partition_id: int) -> dict:
    locks = [dict(r) for r in conn.execute(
        "SELECT j.id, j.to_tier, p.tier AS current_tier, "
        "CASE WHEN p.tier=j.to_tier THEN 1 ELSE 0 END AS tier_frozen "
        "FROM migrate_jobs j JOIN partitions p ON p.id=j.partition_id "
        "WHERE j.partition_id=? AND j.status='占坑中' ORDER BY j.id",
        (partition_id,),
    ).fetchall()]
    waits = [dict(r) for r in conn.execute(
        "SELECT id, to_tier FROM migrate_jobs WHERE partition_id=? AND status='等候中' ORDER BY id",
        (partition_id,),
    ).fetchall()]
    tier = conn.execute("SELECT tier FROM partitions WHERE id=?", (partition_id,)).fetchone()["tier"]
    return {"locks": locks, "waits": waits, "tier": tier}


def run_probe(partition_id: int, workers: int = 4) -> dict:
    if not 2 <= workers <= 8:
        raise HTTPException(400, detail="探针并发度需在 2..8 之间")
    with db_write() as conn:
        p = partition_row(conn, partition_id)
        if p["status"] != "在册":
            raise HTTPException(400, detail="仅在册分区可跑并发探针")
        open_cnt = conn.execute(
            "SELECT COUNT(*) AS c FROM migrate_jobs WHERE partition_id=? AND status='占坑中'",
            (partition_id,),
        ).fetchone()["c"]
        if open_cnt:
            raise HTTPException(400, detail="分区存在在途占坑，请清场后再跑探针")
        wait_cnt = conn.execute(
            "SELECT COUNT(*) AS c FROM migrate_jobs WHERE partition_id=? AND status='等候中'",
            (partition_id,),
        ).fetchone()["c"]
        if wait_cnt:
            raise HTTPException(400, detail="分区存在等候单，请清场后再跑探针")
        origin_tier = p["tier"]
        cur = conn.execute(
            "INSERT INTO probe_runs(kind, partition_id, status, summary, created_at) VALUES (?,?,?,?,?)",
            ("并发双发起", partition_id, "运行中", "", now()),
        )
        run_id = cur.lastrowid

    log = _EventLog(run_id)
    alternatives = [t for t in ("热档", "温档", "冷档") if t != origin_tier]
    targets = [alternatives[i % len(alternatives)] for i in range(workers)]
    log.add("探针起步", {"partition_id": partition_id, "原温层": origin_tier, "并发度": workers, "目标序列": targets})
    barrier = threading.Barrier(workers)
    receipts: list[dict] = []
    receipts_lock = threading.Lock()
    # 同层拒单是互斥正确的业务拒绝（整笔不留单、不改温层），单列核验；
    # 只有 errors 里的意外异常才判探针未闭合。
    same_tier_rejects: list[dict] = []
    errors: list[dict] = []

    def strike(idx: int, target_tier: str):
        label = f"探针笔-{idx + 1}"
        barrier.wait()
        try:
            out = migrate.start_migrate(partition_id, target_tier)
            log.add("发起回执", out, thread_label=label, job_id=out.get("job_id"))
            with receipts_lock:
                receipts.append(out)
        except HTTPException as e:
            if "目标温层与现行相同" in str(e.detail):
                same_tier_rejects.append({"label": label, "target_tier": target_tier, "detail": e.detail})
                log.add("同层整笔拒绝", e.detail, thread_label=label)
            else:
                errors.append({"label": label, "detail": e.detail})
                log.add("发起异常", e.detail, thread_label=label)

    with ThreadPoolExecutor(workers) as ex:
        list(ex.map(lambda i: strike(i, targets[i]), range(workers)))

    checks: list[dict] = []

    def record(name: str, ok: bool, detail):
        checks.append({"name": name, "ok": ok, "detail": detail})
        log.add("不变量核验", {"name": name, "通过": ok, "证据": detail}, thread_label="核验")

    # 核验读段：只读快照，不持有写事务；finish_migrate 各自开独立写口，避免嵌套
    with db() as conn:
        snap = _snapshot(conn, partition_id)
        # 夹具一：双锁同时改温层 —— 读路径只允许一把锁
        record("同分区至多一笔占坑", len(snap["locks"]) == 1,
               f"锁行数={len(snap['locks'])}")
        # 夹具三：锁行与温层分叉 —— 每把锁必须温层锁死
        frozen = all(x["tier_frozen"] == 1 for x in snap["locks"])
        record("锁行温层锁死不分叉", bool(snap["locks"]) and frozen,
               json.dumps(snap["locks"], ensure_ascii=False))
        # 夹具二：第二笔成功未入等候却改温层 —— 回执成功必已按态落账
        open_receipts = [r for r in receipts if r["status"] == OPEN]
        wait_receipts = [r for r in receipts if r["status"] == WAITING]
        record("回执恰一笔占坑", len(open_receipts) == 1,
               f"占坑回执={len(open_receipts)} 等候回执={len(wait_receipts)} 同层拒绝={len(same_tier_rejects)} 异常={len(errors)}")
        db_wait_ids = {w["id"] for w in snap["waits"]}
        receipt_wait_ok = all(r["job_id"] in db_wait_ids for r in wait_receipts)
        record("等候回执必已入等候读模型", receipt_wait_ok and len(wait_receipts) == len(snap["waits"]),
               f"回执等候={sorted(r['job_id'] for r in wait_receipts)} 库等候={sorted(db_wait_ids)}")
        # 同层拒单必须整笔不留单：回执笔数 + 拒单笔数 == 并发度，且库里无幽灵单
        accounted = len(receipts) + len(same_tier_rejects) + len(errors)
        total_jobs = len(snap["locks"]) + len(snap["waits"])
        record("同层拒单整笔不留幽灵单",
               accounted == workers and total_jobs == len(receipts),
               f"回执={len(receipts)} 拒单={len(same_tier_rejects)} 异常={len(errors)} 库占坑+等候={total_jobs}")
        lock_id = snap["locks"][0]["id"] if snap["locks"] else None
        hold_ok = all(r.get("hold_job_id") == lock_id for r in wait_receipts)
        record("等候单前方占坑指向唯一锁行", hold_ok, f"锁行={lock_id}")
        record("温层只随占坑改写", bool(snap["locks"]) and snap["tier"] == snap["locks"][0]["to_tier"],
               f"现行温层={snap['tier']} 锁行目标={snap['locks'][0]['to_tier'] if snap['locks'] else None}")
        poss = sorted(r["queue_pos"] for r in wait_receipts)
        record("等候队次连续可重放", poss == list(range(1, len(poss) + 1)), str(poss))

    # 夹具四：并发双发起半截 —— 失败回滚（独立写口），验证温层回卷与升占坑同事务收敛
    if lock_id is not None:
        with db() as conn:
            before = conn.execute("SELECT tier FROM partitions WHERE id=?", (partition_id,)).fetchone()["tier"]
        fin = migrate.finish_migrate(lock_id, False)
        log.add("半截失败回滚", fin, thread_label="核验", job_id=lock_id)
        record("失败回滚温层回卷",
               fin["status"] == "已失败" and fin["tier"] == origin_tier,
               f"回滚后温层={fin['tier']} 原温层={origin_tier} 回滚前={before}")
    else:
        record("失败回滚温层回卷", False, "无锁行可回滚")

    # 顺次完结清场：升占坑链必须按单号收敛，拒绝者记拒因
    drain = []
    while True:
        with db() as conn:
            row = conn.execute(
                "SELECT id FROM migrate_jobs WHERE partition_id=? AND status='占坑中'",
                (partition_id,),
            ).fetchone()
        if not row:
            break
        fin = migrate.finish_migrate(row["id"], True)
        drain.append(fin)
        log.add("顺次完结清场", fin, thread_label="核验", job_id=row["id"])

    with db() as conn:
        snap2 = _snapshot(conn, partition_id)
    record("半截收敛队列清空", not snap2["locks"] and not snap2["waits"],
           f"余锁={len(snap2['locks'])} 余候={len(snap2['waits'])}")
    record("清场后温层有据可依", snap2["tier"] in ("热档", "温档", "冷档"),
           f"清场后温层={snap2['tier']}")

    all_ok = all(c["ok"] for c in checks) and not errors
    summary = (f"核验 {len(checks)} 项，通过 {sum(c['ok'] for c in checks)} 项；"
               f"占坑 1、等候 {len([r for r in receipts if r['status'] == WAITING])}、"
               f"同层拒绝 {len(same_tier_rejects)}、异常 {len(errors)}")
    with db_write() as conn:
        conn.execute(
            "UPDATE probe_runs SET status=?, summary=?, finished_at=? WHERE id=?",
            ("闭合" if all_ok else "未闭合", summary, now(), run_id),
        )
        log.flush(conn)
    return {
        "run_id": run_id,
        "status": "闭合" if all_ok else "未闭合",
        "summary": summary,
        "checks": checks,
        "receipts": receipts,
        "same_tier_rejects": same_tier_rejects,
        "errors": errors,
        "drain": drain,
    }


def list_runs():
    with db() as conn:
        runs = [dict(r) for r in conn.execute(
            "SELECT * FROM probe_runs ORDER BY id DESC"
        ).fetchall()]
        for r in runs:
            r["events"] = [dict(e) for e in conn.execute(
                "SELECT seq, thread_label, job_id, event, detail, at FROM probe_events "
                "WHERE run_id=? ORDER BY seq", (r["id"],)
            ).fetchall()]
        return runs
