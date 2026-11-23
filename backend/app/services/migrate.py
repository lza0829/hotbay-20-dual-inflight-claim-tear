from __future__ import annotations
import sqlite3
from fastapi import HTTPException
from app.db import db, db_write
from app.services.core import can_migrate, now, partition_row, require_tier

# 全局唯一口径（读路径最多一把温层锁可重放）：
# 同分区同时至多一笔占坑，占坑即同事务改写温层；
# 再发起整笔挂等候队列，等候单不触碰温层；占坑完结后按单号升占坑。
# 任何造成双锁的写口整笔回滚，禁止只改占坑列表标题。
OPEN = "占坑中"
WAITING = "等候中"


def list_jobs():
    with db() as conn:
        rows = conn.execute(
            '''SELECT j.*, p.part_key, p.title,
                      CASE WHEN j.status='等候中' THEN (
                        SELECT COUNT(*) FROM migrate_jobs w
                        WHERE w.partition_id=j.partition_id AND w.status='等候中' AND w.id<j.id
                      ) + 1 END AS queue_pos,
                      CASE WHEN j.status='等候中' THEN (
                        SELECT h.id FROM migrate_jobs h
                        WHERE h.partition_id=j.partition_id AND h.status='占坑中'
                      ) END AS hold_job_id
               FROM migrate_jobs j
               JOIN partitions p ON p.id=j.partition_id
               ORDER BY j.id DESC'''
        ).fetchall()
        return [dict(r) for r in rows]


def list_locks():
    """锁行列表：读路径上同分区至多一行。
    每行自带温层锁死标记 tier_frozen（锁行目标层==分区现行温层），
    总览占坑徽章必须能由本投影重放，旁路读模型一律不得参与。"""
    with db() as conn:
        rows = conn.execute(
            '''SELECT j.id AS job_id, j.partition_id, p.part_key, p.title,
                      j.from_tier, j.to_tier, p.tier AS current_tier,
                      j.created_at,
                      CASE WHEN p.tier=j.to_tier THEN 1 ELSE 0 END AS tier_frozen
               FROM migrate_jobs j
               JOIN partitions p ON p.id=j.partition_id
               WHERE j.status='占坑中'
               ORDER BY j.id'''
        ).fetchall()
        return [dict(r) for r in rows]


def list_waiting():
    """等候读模型：由 migrate_jobs 单表投影重放，不另立旁路存储。
    等候单必有所属在途占坑（hold_job_id 非空），队次按单号连续。"""
    with db() as conn:
        rows = conn.execute(
            '''SELECT j.id AS job_id, j.partition_id, p.part_key, p.title,
                      j.from_tier, j.to_tier, j.created_at,
                      (SELECT COUNT(*) FROM migrate_jobs w
                        WHERE w.partition_id=j.partition_id AND w.status='等候中' AND w.id<j.id) + 1 AS queue_pos,
                      (SELECT h.id FROM migrate_jobs h
                        WHERE h.partition_id=j.partition_id AND h.status='占坑中') AS hold_job_id
               FROM migrate_jobs j
               JOIN partitions p ON p.id=j.partition_id
               WHERE j.status='等候中'
               ORDER BY j.partition_id, j.id'''
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
    # BEGIN IMMEDIATE：查再写全程持写锁，并发双发起在此串行，
    # 后到者看到的是已落账的占坑行；部分唯一索引是最后防线。
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
            # 假第二笔收口（全局唯一口径）：整笔挂等候，不触碰温层，
            # 回执如实告队次与前方占坑单；回执成功则该单必已落账为等候中。
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
        # 无占坑：占坑申请与温层改写同一事务、可核同成败。
        # 若并发下索引拦截出双锁，IntegrityError 令整笔回滚，温层不会被改。
        try:
            cur = conn.execute(
                "INSERT INTO migrate_jobs(partition_id, from_tier, to_tier, status, created_at) VALUES (?,?,?,?,?)",
                (partition_id, p["tier"], to_tier, OPEN, now()),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, detail="占坑冲突：同分区已有在途占坑，本笔整笔回滚未改温层")
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
