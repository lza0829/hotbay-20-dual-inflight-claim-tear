from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel, Field
from app.db import db
from app.services import migrate as svc
from app.services import probe as probe_svc

router = APIRouter(tags=["迁移"])


class MigrateIn(BaseModel):
    partition_id: int
    to_tier: str


class FinishIn(BaseModel):
    success: bool = True


class ProbeIn(BaseModel):
    partition_id: int
    workers: int = Field(default=4, ge=2, le=8)


@router.get("/迁移")
def list_jobs():
    # 迁移单表：占坑/等候/完成/失败/拒绝全量流水，读模型都由它投影
    return svc.list_jobs()


@router.get("/迁移/锁行")
def list_locks():
    # 锁行列表（含温层锁死标记 tier_frozen）：同分区读路径至多一行
    return svc.list_locks()


@router.get("/迁移/等候")
def list_waiting():
    # 等候读模型：单表投影，队次与前方占坑单可重放
    return svc.list_waiting()


@router.get("/迁移/探针流水")
def probe_runs():
    return probe_svc.list_runs()


@router.post("/迁移/并发探针")
def run_probe(body: ProbeIn):
    return probe_svc.run_probe(body.partition_id, body.workers)


@router.get("/迁移/{job_id}")
def job_detail(job_id: int):
    # 迁移详情拒因页：拒因/失败注记与锁行证据同库回放
    with db() as conn:
        row = conn.execute(
            '''SELECT j.*, p.part_key, p.title, p.tier AS current_tier,
                      CASE WHEN j.status='占坑中' AND p.tier=j.to_tier THEN 1 ELSE 0 END AS tier_frozen
               FROM migrate_jobs j JOIN partitions p ON p.id=j.partition_id
               WHERE j.id=?''',
            (job_id,),
        ).fetchone()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, detail="迁移单不存在")
        detail = dict(row)
        if detail["status"] == "等候中":
            hold = conn.execute(
                "SELECT id AS hold_job_id, to_tier AS hold_to_tier FROM migrate_jobs "
                "WHERE partition_id=? AND status='占坑中'", (detail["partition_id"],),
            ).fetchone()
            detail["hold"] = dict(hold) if hold else None
            detail["queue_pos"] = conn.execute(
                "SELECT COUNT(*) AS c FROM migrate_jobs "
                "WHERE partition_id=? AND status='等候中' AND id<?",
                (detail["partition_id"], job_id),
            ).fetchone()["c"] + 1
        else:
            detail["hold"] = None
        return detail


@router.post("/迁移")
def start(body: MigrateIn):
    return svc.start_migrate(body.partition_id, body.to_tier)


@router.post("/迁移/{job_id}/完成")
def finish(job_id: int, body: FinishIn):
    return svc.finish_migrate(job_id, body.success)
