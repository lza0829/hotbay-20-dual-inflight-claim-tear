from __future__ import annotations
import sqlite3
from fastapi import HTTPException
from app.db import db, db_write
from app.services.core import can_migrate, now, partition_row, require_tier
from app.services import claim_surface

# 全局口径：排队串行。同分区同时至多一笔占坑，占坑即改写温层；
# 再发起整笔挂等候队列，等候单不触碰温层；占坑完结后按单号升占坑。
OPEN = "占坑中"
WAITING = "等候中"


def list_jobs():
    with db() as conn:
        rows = conn.execute(
            '''SELECT j.*, p.part_key, p.title,
                      CASE WHEN j.status='等候中' THEN (
                        SELECT COUNT(*) FROM migrate_jobs w
                        WHERE w.partition_id=j.partition_id AND w.status='等候中' AND w.id<j.id
                      ) + 1 END AS queue_pos
               FROM migrate_jobs j
               JOIN partitions p ON p.id=j.partition_id
               ORDER BY j.id DESC'''
        ).fetchall()
        return [dict(r) for r in rows]


def _queue_pos(conn, partition_id: int, job_id: int) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM migrate_jobs "
        "WHERE partition_id=? AND status='等候中' AND id<?",
        (partition_id, job_id),
    ).fetchone()["c"] + 1


def start_migrate(partition_id: int, to_tier: str):
    to_tier = require_tier(to_tier)
    with db_write() as conn:
        p = partition_row(conn, partition_id)
        if p["status"] != "在册":
            raise HTTPException(400, detail="仅在册分区可迁移")
        if not can_migrate(p["tier"], to_tier):
            raise HTTPException(400, detail=f"目标温层与现行相同（{p['tier']}）")
        hold = conn.execute(
            "SELECT id FROM migrate_jobs WHERE partition_id=? AND status='占坑中'",
            (partition_id,),
        ).fetchone()
        if hold:
            # 已有占坑：整笔挂入等候队列，回执如实告队次；等候单不改写温层
            cur = conn.execute(
                "INSERT INTO migrate_jobs(partition_id, from_tier, to_tier, status, created_at) VALUES (?,?,?,?,?)",
                (partition_id, p["tier"], to_tier, WAITING, now()),
            )
            jid = cur.lastrowid
            return {
                "job_id": jid,
                "status": WAITING,
                "queue_pos": _queue_pos(conn, partition_id, jid),
                "hold_job_id": hold["id"],
                "from_tier": p["tier"],
                "to_tier": to_tier,
            }
        # 无占坑：占坑申请与温层改写同一事务，可核同成败
        try:
            cur = conn.execute(
                "INSERT INTO migrate_jobs(partition_id, from_tier, to_tier, status, created_at) VALUES (?,?,?,?,?)",
                (partition_id, p["tier"], to_tier, OPEN, now()),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, detail="占坑冲突：同分区已有在途占坑")
        conn.execute("UPDATE partitions SET tier=? WHERE id=?", (to_tier, partition_id))
        return {
            "job_id": cur.lastrowid,
            "status": OPEN,
            "from_tier": p["tier"],
            "to_tier": to_tier,
        }


def _promote_next(conn, partition_id: int):
    """占坑位空出，按单号次序升占坑（次序可重放）。
    目标层与现行相同的等候单整笔拒绝并记拒因，续看下一笔；
    升占坑与温层改写同一事务。"""
    promoted, rejected = None, []
    while True:
        nxt = conn.execute(
            "SELECT * FROM migrate_jobs WHERE partition_id=? AND status='等候中' ORDER BY id LIMIT 1",
            (partition_id,),
        ).fetchone()
        if not nxt:
            break
        cur_tier = conn.execute(
            "SELECT tier FROM partitions WHERE id=?", (partition_id,)
        ).fetchone()["tier"]
        if nxt["to_tier"] == cur_tier:
            note = f"升占坑被拒：目标温层与现行相同（{cur_tier}）"
            conn.execute(
                "UPDATE migrate_jobs SET status='已拒绝', finished_at=?, note=? WHERE id=?",
                (now(), note, nxt["id"]),
            )
            rejected.append({"job_id": nxt["id"], "status": "已拒绝", "note": note})
            continue
        conn.execute(
            "UPDATE migrate_jobs SET status='占坑中', from_tier=? WHERE id=?",
            (cur_tier, nxt["id"]),
        )
        conn.execute("UPDATE partitions SET tier=? WHERE id=?", (nxt["to_tier"], partition_id))
        promoted = {
            "job_id": nxt["id"],
            "status": OPEN,
            "from_tier": cur_tier,
            "to_tier": nxt["to_tier"],
        }
        break
    return promoted, rejected


def finish_migrate(job_id: int, success: bool = True):
    with db_write() as conn:
        j = conn.execute("SELECT * FROM migrate_jobs WHERE id=?", (job_id,)).fetchone()
        if not j:
            raise HTTPException(404, detail="迁移单不存在")
        if j["status"] != OPEN:
            raise HTTPException(400, detail="迁移单不在占坑中")
        if success:
            conn.execute(
                "UPDATE migrate_jobs SET status='已完成', finished_at=? WHERE id=?",
                (now(), job_id),
            )
            result = {"job_id": job_id, "status": "已完成", "tier": j["to_tier"]}
        else:
            # 失败回滚：温层回卷与关单同一事务，不留半截占坑
            conn.execute("UPDATE partitions SET tier=? WHERE id=?", (j["from_tier"], j["partition_id"]))
            conn.execute(
                "UPDATE migrate_jobs SET status='已失败', finished_at=?, note='执行失败回滚温层' WHERE id=?",
                (now(), job_id),
            )
            result = {"job_id": job_id, "status": "已失败", "tier": j["from_tier"]}
        promoted, rejected = _promote_next(conn, j["partition_id"])
        result["promoted"] = promoted
        result["rejected"] = rejected
        return result

_start_migrate_impl = start_migrate


def start_migrate(partition_id: int, to_tier: str):
    out = _start_migrate_impl(partition_id, to_tier)
    return claim_surface.after_start(out)

