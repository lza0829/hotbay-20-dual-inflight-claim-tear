from __future__ import annotations
from fastapi import APIRouter
from app.db import db

router = APIRouter(tags=["总览"])


@router.get("/总览")
def overview():
    # 全局口径：占坑徽章只能由锁行（migrate_jobs.status='占坑中'）
    # ∪ 温层锁死标记（partitions.tier=锁行.to_tier）重放，不接任何旁路读模型。
    # 锁行数与锁死数同源同库计算，二者恒等由写口同事务保证、由探针流水核验。
    with db() as conn:
        def cnt(sql, *a):
            return conn.execute(sql, a).fetchone()["c"]

        return {
            "热档": cnt("SELECT COUNT(*) AS c FROM partitions WHERE tier='热档' AND status='在册'"),
            "温档": cnt("SELECT COUNT(*) AS c FROM partitions WHERE tier='温档' AND status='在册'"),
            "冷档": cnt("SELECT COUNT(*) AS c FROM partitions WHERE tier='冷档' AND status='在册'"),
            "迁移占坑中": cnt("SELECT COUNT(*) AS c FROM migrate_jobs WHERE status='占坑中'"),
            "迁移等候中": cnt("SELECT COUNT(*) AS c FROM migrate_jobs WHERE status='等候中'"),
            "占坑温层锁死": cnt(
                "SELECT COUNT(*) AS c FROM migrate_jobs j JOIN partitions p ON p.id=j.partition_id "
                "WHERE j.status='占坑中' AND p.tier=j.to_tier"
            ),
            "待销毁候选": cnt("SELECT COUNT(*) AS c FROM partitions WHERE status='已过期'"),
            "已销毁": cnt("SELECT COUNT(*) AS c FROM partitions WHERE status='已销毁'"),
            "目录对象": cnt("SELECT COUNT(*) AS c FROM inventory_objects"),
            "最近互证": (
                lambda r: dict(r) if r else None
            )(conn.execute("SELECT status, drift_count, created_at FROM verify_runs ORDER BY id DESC LIMIT 1").fetchone()),
            "片库间": [dict(x) for x in conn.execute("SELECT id, name, capacity, active FROM vaults ORDER BY id").fetchall()],
        }
