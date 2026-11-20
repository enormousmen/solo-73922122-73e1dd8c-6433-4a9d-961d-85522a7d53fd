"""SQLite 持久化层。

单连接 + 可重入锁：所有读写串行；写事务使用 BEGIN IMMEDIATE，
即使未来跑多进程，也能在数据库层面串行化并发申请，守住多数票。
许可、探活报告与缺票时段均落盘，服务重启后保留。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterable

from .models import (
    LatestReport,
    PermitRecord,
    VoteLossEvent,
    VoteLossPeriod,
    now_utc,
    parse_iso,
    to_iso,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS liveness_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    sampled_at TEXT NOT NULL,   -- ISO8601 UTC，采样时间
    healthy INTEGER NOT NULL,   -- 1/0
    boot_id TEXT NOT NULL,      -- 启动标识
    received_at TEXT NOT NULL   -- 服务端接收时间（审计用）
);
CREATE INDEX IF NOT EXISTS idx_liveness_node_sampled
    ON liveness_reports(node_id, sampled_at);

CREATE TABLE IF NOT EXISTS permits (
    request_id TEXT PRIMARY KEY,          -- 申请号（幂等键）
    nodes TEXT NOT NULL,                  -- JSON 数组，有序去重
    status TEXT NOT NULL CHECK (status IN ('open', 'finished')),
    approved_at TEXT NOT NULL,
    finished_at TEXT,
    boot_ids_at_approval TEXT NOT NULL    -- JSON 对象 node -> boot_id
);

CREATE TABLE IF NOT EXISTS vote_loss_periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shard TEXT NOT NULL,
    started_at TEXT NOT NULL,   -- 服务观察到缺票的时间（ISO8601 UTC）
    recovered_at TEXT           -- 服务观察到恢复的时间；NULL = 尚未恢复
);
CREATE INDEX IF NOT EXISTS idx_vote_loss_shard ON vote_loss_periods(shard, id);
-- 同一分片至多一条未结束的缺票记录（数据库层面兜底不变式）
CREATE UNIQUE INDEX IF NOT EXISTS idx_vote_loss_open
    ON vote_loss_periods(shard) WHERE recovered_at IS NULL;
"""

# 每个节点取采样时间最新的一份；采样时间并列时取后写入的一条。
_LATEST_SQL = """
SELECT node_id, sampled_at, healthy, boot_id
FROM (
    SELECT node_id, sampled_at, healthy, boot_id,
           ROW_NUMBER() OVER (
               PARTITION BY node_id ORDER BY sampled_at DESC, id DESC
           ) AS rn
    FROM liveness_reports
) t
WHERE rn = 1
"""


class Store:
    def __init__(self, db_path: str):
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self):
        """写事务：BEGIN IMMEDIATE 立即拿写锁，并发申请在此串行。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    # ---- 探活报告 ----

    def insert_reports(self, reports: Iterable) -> None:
        """插入一批探活报告。

        不自带事务：调用方须用 transaction() 包裹，使探活与其衍生的
        缺票记录在同一事务里提交或回滚。
        """
        received = to_iso(now_utc())
        rows = [
            (r.node_id, to_iso(r.sampled_at), 1 if r.healthy else 0, r.boot_id, received)
            for r in reports
        ]
        with self._lock:
            self._conn.executemany(
                "INSERT INTO liveness_reports "
                "(node_id, sampled_at, healthy, boot_id, received_at) "
                "VALUES (?,?,?,?,?)",
                rows,
            )

    def latest_reports(self) -> dict[str, LatestReport]:
        with self._lock:
            rows = self._conn.execute(_LATEST_SQL).fetchall()
        return {
            r["node_id"]: LatestReport(
                node_id=r["node_id"],
                sampled_at=parse_iso(r["sampled_at"]),
                healthy=bool(r["healthy"]),
                boot_id=r["boot_id"],
            )
            for r in rows
        }

    # ---- 缺票时段 ----

    def apply_vote_loss_events(
        self, events: Iterable[VoteLossEvent], observed_at
    ) -> None:
        """按翻转事件开/补缺票记录。

        调用方须持有事务，与对应探活批次一同提交或回滚。
        lost：新开一条记录（started_at=观察时间）；recovered：给该分片
        未结束的同一条记录补上 recovered_at（没有未结束记录则为无操作）。
        """
        with self._lock:
            for ev in events:
                if ev.kind == "lost":
                    self._conn.execute(
                        "INSERT INTO vote_loss_periods (shard, started_at, recovered_at) "
                        "VALUES (?,?,NULL)",
                        (ev.shard, to_iso(observed_at)),
                    )
                else:  # recovered
                    self._conn.execute(
                        "UPDATE vote_loss_periods SET recovered_at = ? "
                        "WHERE shard = ? AND recovered_at IS NULL",
                        (to_iso(observed_at), ev.shard),
                    )

    def list_vote_losses(self, shard: str) -> list[VoteLossPeriod]:
        """某分片的全部缺票时段，按发生顺序（即批次提交顺序）排列。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, shard, started_at, recovered_at "
                "FROM vote_loss_periods WHERE shard = ? ORDER BY id",
                (shard,),
            ).fetchall()
        return [
            VoteLossPeriod(
                id=r["id"],
                shard=r["shard"],
                started_at=parse_iso(r["started_at"]),
                recovered_at=parse_iso(r["recovered_at"]) if r["recovered_at"] else None,
            )
            for r in rows
        ]

    # ---- 许可 ----

    @staticmethod
    def _row_to_permit(row: sqlite3.Row) -> PermitRecord:
        return PermitRecord(
            request_id=row["request_id"],
            nodes=tuple(json.loads(row["nodes"])),
            status=row["status"],
            approved_at=parse_iso(row["approved_at"]),
            finished_at=parse_iso(row["finished_at"]) if row["finished_at"] else None,
            boot_ids_at_approval=json.loads(row["boot_ids_at_approval"]),
        )

    def get_permit(self, request_id: str) -> PermitRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM permits WHERE request_id = ?", (request_id,)
            ).fetchone()
        return self._row_to_permit(row) if row else None

    def list_permits(self, status: str | None = None) -> list[PermitRecord]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM permits WHERE status = ? ORDER BY approved_at, request_id",
                    (status,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM permits ORDER BY approved_at, request_id"
                ).fetchall()
        return [self._row_to_permit(r) for r in rows]

    def open_permits(self) -> list[PermitRecord]:
        return self.list_permits(status="open")

    def insert_permit(self, p: PermitRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO permits "
                "(request_id, nodes, status, approved_at, finished_at, boot_ids_at_approval) "
                "VALUES (?,?,?,?,?,?)",
                (
                    p.request_id,
                    json.dumps(list(p.nodes)),
                    p.status,
                    to_iso(p.approved_at),
                    to_iso(p.finished_at) if p.finished_at else None,
                    json.dumps(p.boot_ids_at_approval),
                ),
            )

    def mark_finished(self, request_id: str, finished_at) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE permits SET status = 'finished', finished_at = ? "
                "WHERE request_id = ?",
                (to_iso(finished_at), request_id),
            )
