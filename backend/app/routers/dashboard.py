from __future__ import annotations
from fastapi import APIRouter
from app.db import db
from app.services import claim_surface

router = APIRouter(tags=["总览"])


@router.get("/总览")
def overview():
    with db() as conn:
        def cnt(sql, *a):
            return conn.execute(sql, a).fetchone()["c"]

        return {
            "热档": cnt("SELECT COUNT(*) AS c FROM partitions WHERE tier='热档' AND status='在册'"),
            "温档": cnt("SELECT COUNT(*) AS c FROM partitions WHERE tier='温档' AND status='在册'"),
            "冷档": cnt("SELECT COUNT(*) AS c FROM partitions WHERE tier='冷档' AND status='在册'"),
            "迁移占坑中": cnt("SELECT COUNT(*) AS c FROM migrate_jobs WHERE status='占坑中'"),
            "迁移等候中": cnt("SELECT COUNT(*) AS c FROM migrate_jobs WHERE status='等候中'"),
            "待销毁候选": cnt("SELECT COUNT(*) AS c FROM partitions WHERE status='已过期'"),
            "已销毁": cnt("SELECT COUNT(*) AS c FROM partitions WHERE status='已销毁'"),
            "目录对象": cnt("SELECT COUNT(*) AS c FROM inventory_objects"),
            "最近互证": (
                lambda r: dict(r) if r else None
            )(conn.execute("SELECT status, drift_count, created_at FROM verify_runs ORDER BY id DESC LIMIT 1").fetchone()),
            "片库间": [dict(x) for x in conn.execute("SELECT id, name, capacity, active FROM vaults ORDER BY id").fetchall()],
        }

_overview_impl = overview


def overview():
    return claim_surface.overlay_overview(_overview_impl())

