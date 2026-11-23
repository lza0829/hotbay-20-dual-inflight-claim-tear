from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from app.config import DB_PATH

SCHEMA = '''
CREATE TABLE IF NOT EXISTS vaults (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  capacity INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS partitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  part_key TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  release_day TEXT NOT NULL,
  vault_id INTEGER NOT NULL,
  tier TEXT NOT NULL,
  object_prefix TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT '在册',
  created_at TEXT NOT NULL,
  FOREIGN KEY(vault_id) REFERENCES vaults(id)
);
CREATE TABLE IF NOT EXISTS clips (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  partition_id INTEGER NOT NULL,
  object_key TEXT NOT NULL UNIQUE,
  bytes INTEGER NOT NULL,
  entered_day TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(partition_id) REFERENCES partitions(id)
);
CREATE TABLE IF NOT EXISTS migrate_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  partition_id INTEGER NOT NULL,
  from_tier TEXT NOT NULL,
  to_tier TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  note TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(partition_id) REFERENCES partitions(id)
);
CREATE TABLE IF NOT EXISTS retention_policies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  max_age_days INTEGER NOT NULL,
  action TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS destroy_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  partition_id INTEGER NOT NULL,
  policy_id INTEGER NOT NULL,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(partition_id) REFERENCES partitions(id),
  FOREIGN KEY(policy_id) REFERENCES retention_policies(id)
);
CREATE TABLE IF NOT EXISTS inventory_objects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_key TEXT NOT NULL UNIQUE,
  bytes INTEGER NOT NULL,
  vault_name TEXT NOT NULL,
  scanned_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS verify_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  status TEXT NOT NULL,
  drift_count INTEGER NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS probe_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  partition_id INTEGER,
  status TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE TABLE IF NOT EXISTS probe_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  thread_label TEXT NOT NULL DEFAULT '',
  job_id INTEGER,
  event TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES probe_runs(id)
);
'''


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db():
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def db_write():
    # 写事务入口：BEGIN IMMEDIATE 立即持写锁，把「查再写」串行化，
    # 并发双发起在此排队，后到者看到的是已落账的占坑状态。
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _migrate_legacy(conn) -> None:
    # 旧状态名并入占坑口径
    conn.execute("UPDATE migrate_jobs SET status='占坑中' WHERE status='执行中'")
    # 历史双占坑收敛：同分区最早一笔留坑，其余整笔回排队等候
    dups = conn.execute(
        "SELECT partition_id FROM migrate_jobs WHERE status='占坑中' "
        "GROUP BY partition_id HAVING COUNT(*) > 1"
    ).fetchall()
    for d in dups:
        keep = conn.execute(
            "SELECT MIN(id) AS k FROM migrate_jobs WHERE partition_id=? AND status='占坑中'",
            (d["partition_id"],),
        ).fetchone()["k"]
        conn.execute(
            "UPDATE migrate_jobs SET status='等候中' "
            "WHERE partition_id=? AND status='占坑中' AND id<>?",
            (d["partition_id"], keep),
        )
    # 占坑口径：分区温层以在途占坑目标层为准，消除占坑标记与温层分叉
    conn.execute(
        '''UPDATE partitions SET tier = (
             SELECT j.to_tier FROM migrate_jobs j
             WHERE j.partition_id = partitions.id AND j.status = '占坑中')
           WHERE EXISTS (
             SELECT 1 FROM migrate_jobs j
             WHERE j.partition_id = partitions.id AND j.status = '占坑中')'''
    )
    # 旧旁路读模型（claim_surface）已废弃：其 settings 键不再是占坑证据，清场防旧库残留撒谎
    conn.execute(
        "DELETE FROM settings WHERE key LIKE 'claim_surface%' OR key='claim_surface_ok'"
    )
    # 旧旁路写库时可能给等候/完成单留过「成功注记」，与占坑口径无关，清场
    conn.execute(
        "UPDATE migrate_jobs SET note='' WHERE note='成功注记'"
    )


def init_db() -> None:
    with db() as conn:
        conn.executescript(SCHEMA)
        _migrate_legacy(conn)
        # 硬约束：同分区同时至多一笔占坑（并发双发起的最后防线）
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS migrate_open_idx "
            "ON migrate_jobs(partition_id) WHERE status='占坑中'"
        )
