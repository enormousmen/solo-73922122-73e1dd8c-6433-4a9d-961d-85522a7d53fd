"""领域模型与时间工具。所有时间统一为 UTC 的 aware datetime。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class LatestReport:
    """某节点采样时间最新的一份探活报告。"""

    node_id: str
    sampled_at: datetime
    healthy: bool
    boot_id: str


@dataclass(frozen=True)
class VoteLossEvent:
    """一次多数票状态翻转。kind: lost（跌破过半）| recovered（恢复过半）。"""

    shard: str
    kind: str
    votes_before: int
    votes_after: int


@dataclass(frozen=True)
class VoteLossPeriod:
    """一段缺票时段：某分片有效票数跌破过半线到恢复过半的区间。

    started_at / recovered_at 均为服务端观察到对应翻转的时间；
    recovered_at 为 None 表示尚未恢复。
    """

    id: int
    shard: str
    started_at: datetime
    recovered_at: datetime | None


@dataclass(frozen=True)
class PermitRecord:
    """一条许可。status: open（未结束）| finished（已结束）。"""

    request_id: str
    nodes: tuple[str, ...]  # 有序、去重
    status: str
    approved_at: datetime
    finished_at: datetime | None
    boot_ids_at_approval: dict[str, str]  # 获批时各节点最新报告中的启动标识
