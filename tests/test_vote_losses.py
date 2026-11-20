"""缺票时段：开启/恢复、过期与重复上报、整批比较、原子性、重启持久化、并发。"""
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

MANIFEST = {
    "shards": {
        "shard-a": ["n1", "n2", "n3"],
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
    }
}
ALL_NODES = ["n1", "n2", "n3", "n4", "n5"]
T0 = datetime(2026, 11, 20, 8, 0, tzinfo=timezone.utc)  # 初始探活采样时间


@pytest.fixture()
def make_client(tmp_path):
    """工厂：同一 db 路径可反复建 app，模拟服务重启。"""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "permits.db"
    clients = []

    def _make():
        app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
        c = TestClient(app)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture()
def client(make_client):
    return make_client()


def rep(node, healthy, boot="boot-1", sampled_at=T0):
    return {
        "node_id": node,
        "sampled_at": sampled_at.isoformat(),
        "healthy": healthy,
        "boot_id": boot,
    }


def post_batch(client, reports):
    return client.post("/liveness", json={"reports": reports})


def seed_all_healthy(client, sampled_at=T0):
    r = post_batch(client, [rep(n, True, sampled_at=sampled_at) for n in ALL_NODES])
    assert r.status_code == 201, r.text


def periods(client, shard):
    r = client.get(f"/shards/{shard}/vote-losses")
    assert r.status_code == 200, r.text
    return r.json()["periods"]


# ---------- 开启与恢复 ----------


def test_loss_opens_then_recovers_same_record(client):
    seed_all_healthy(client)
    t1 = T0 + timedelta(minutes=5)
    # n1、n2 同时异常：shard-a 3 -> 1（过半线 2）缺票；shard-b 3 -> 2 仍过半
    r = post_batch(client, [rep("n1", False, sampled_at=t1), rep("n2", False, sampled_at=t1)])
    assert r.status_code == 201

    pa = periods(client, "shard-a")
    assert len(pa) == 1
    assert pa[0]["status"] == "open"
    assert pa[0]["started_at"] and pa[0]["recovered_at"] is None
    assert periods(client, "shard-b") == []  # 未跌破过半线，无记录

    # n1 恢复健康：shard-a 回到 2/3，同一条记录补上恢复时间（id 不变）
    t2 = t1 + timedelta(minutes=10)
    post_batch(client, [rep("n1", True, boot="boot-2", sampled_at=t2)])
    pa = periods(client, "shard-a")
    assert len(pa) == 1
    assert pa[0]["status"] == "recovered"
    assert pa[0]["recovered_at"] is not None
    assert pa[0]["recovered_at"] >= pa[0]["started_at"]

    body = client.get("/shards/shard-a/vote-losses").json()
    assert body["voters"] == ["n1", "n2", "n3"]
    assert body["majority_needed"] == 2
    assert body["current_healthy_votes"] == 2  # n1、n3 健康，n2 仍异常


def test_one_batch_can_touch_multiple_shards(client):
    seed_all_healthy(client)
    t1 = T0 + timedelta(minutes=5)
    # n2,n3,n4 异常：shard-a 剩 n1、shard-b 全灭、shard-c 剩 n5，三个分片同时缺票
    post_batch(client, [rep(n, False, sampled_at=t1) for n in ("n2", "n3", "n4")])
    for shard in ("shard-a", "shard-b", "shard-c"):
        ps = periods(client, shard)
        assert len(ps) == 1 and ps[0]["status"] == "open"

    # 仅 n3 恢复：shard-a(n1,n3)、shard-c(n3,n5) 回到 2/3；shard-b 仅 n3 = 1/3 仍缺票
    t2 = t1 + timedelta(minutes=5)
    post_batch(client, [rep("n3", True, boot="boot-2", sampled_at=t2)])
    assert periods(client, "shard-a")[0]["status"] == "recovered"
    assert periods(client, "shard-c")[0]["status"] == "recovered"
    assert periods(client, "shard-b")[0]["status"] == "open"


def test_no_record_until_majority_observed_first(client):
    # 服务从未观察到过半（首批上报即异常）：无“从过半跌落”的翻转，不开记录
    t1 = T0 + timedelta(minutes=5)
    post_batch(client, [rep("n1", False, sampled_at=t1), rep("n2", False, sampled_at=t1)])
    assert periods(client, "shard-a") == []
    # 随后恢复到过半：没有未结束记录可补，也不凭空造记录
    t2 = t1 + timedelta(minutes=5)
    post_batch(client, [rep(n, True, sampled_at=t2) for n in ALL_NODES])
    assert periods(client, "shard-a") == []
    # 再次跌破：此时已观察过过半，正常开记录
    t3 = t2 + timedelta(minutes=5)
    post_batch(client, [rep("n1", False, sampled_at=t3), rep("n2", False, sampled_at=t3)])
    ps = periods(client, "shard-a")
    assert len(ps) == 1 and ps[0]["status"] == "open"


# ---------- 过期 / 重复上报 ----------


def test_stale_and_duplicate_reports_do_not_rewrite_records(client):
    seed_all_healthy(client)
    t1 = T0 + timedelta(minutes=5)
    post_batch(client, [rep("n1", False, sampled_at=t1), rep("n2", False, sampled_at=t1)])
    assert len(periods(client, "shard-a")) == 1

    # 过期上报：采样时间早于该节点当前最新，即使内容相反也不生效
    post_batch(client, [rep("n1", True, sampled_at=T0 + timedelta(minutes=1))])
    post_batch(client, [rep("n2", True, sampled_at=T0 + timedelta(minutes=2))])
    # 重复上报：同样的内容原样再来一遍
    post_batch(client, [rep("n1", False, sampled_at=t1), rep("n2", False, sampled_at=t1)])

    pa = periods(client, "shard-a")
    assert len(pa) == 1 and pa[0]["status"] == "open"
    # 过期上报同样不能伪造“恢复”
    assert pa[0]["recovered_at"] is None


def test_whole_batch_compared_as_a_unit(client):
    seed_all_healthy(client)
    t1 = T0 + timedelta(minutes=5)
    t2 = t1 + timedelta(minutes=5)
    # 同一批内先异常后恢复（采样时间更新者生效）：整批生效前后票数不变 -> 无记录
    post_batch(
        client,
        [
            rep("n1", False, sampled_at=t1),
            rep("n2", False, sampled_at=t1),
            rep("n1", True, boot="boot-2", sampled_at=t2),
            rep("n2", True, boot="boot-2", sampled_at=t2),
        ],
    )
    assert periods(client, "shard-a") == []

    # 一批内两节点同时异常 -> 只开一条记录（按批次翻转，不是每节点一条）
    t3 = t2 + timedelta(minutes=5)
    post_batch(client, [rep("n1", False, sampled_at=t3), rep("n2", False, sampled_at=t3)])
    pa = periods(client, "shard-a")
    assert len(pa) == 1 and pa[0]["status"] == "open"


# ---------- 原子性 ----------


def test_unknown_node_batch_writes_nothing(client):
    seed_all_healthy(client)
    t1 = T0 + timedelta(minutes=5)
    r = post_batch(client, [rep("n1", False, sampled_at=t1), rep("ghost", False, sampled_at=t1)])
    assert r.status_code == 422
    # 整批拒绝：n1 的最新探活仍是种子里的健康报告，且无缺票记录
    assert client.get("/liveness/n1").json()["healthy"] is True
    assert periods(client, "shard-a") == []


def test_reports_and_loss_records_rollback_together(client, monkeypatch):
    seed_all_healthy(client)
    store = client.app.state.store

    def boom(*args, **kwargs):
        raise RuntimeError("injected failure")

    # 缺票记录写入失败时，同事务的探活也必须回滚
    monkeypatch.setattr(store, "apply_vote_loss_events", boom)
    t1 = T0 + timedelta(minutes=5)
    with pytest.raises(RuntimeError):
        post_batch(client, [rep("n1", False, sampled_at=t1), rep("n2", False, sampled_at=t1)])
    assert client.get("/liveness/n1").json()["healthy"] is True
    assert periods(client, "shard-a") == []


# ---------- 持久化 ----------


def test_vote_losses_survive_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1)
    t1 = T0 + timedelta(minutes=5)
    post_batch(c1, [rep("n1", False, sampled_at=t1), rep("n2", False, sampled_at=t1)])
    assert len(periods(c1, "shard-a")) == 1
    c1.close()

    # 模拟服务重启：缺票记录仍可查；未结束的记录继续被后续上报更新
    c2 = make_client()
    pa = periods(c2, "shard-a")
    assert len(pa) == 1 and pa[0]["status"] == "open"
    t2 = t1 + timedelta(minutes=10)
    post_batch(c2, [rep("n1", True, boot="boot-2", sampled_at=t2)])
    pa = periods(c2, "shard-a")
    assert len(pa) == 1
    assert pa[0]["status"] == "recovered" and pa[0]["recovered_at"] is not None


# ---------- 查询 ----------


def test_unknown_shard_404(client):
    r = client.get("/shards/shard-zzz/vote-losses")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "unknown_shard"


def test_periods_listed_in_submission_order(client):
    seed_all_healthy(client)
    # 两轮 缺票 -> 恢复，记录按批次提交顺序排列
    for round_no in range(2):
        base = T0 + timedelta(minutes=10 * (round_no + 1))
        post_batch(client, [rep("n1", False, sampled_at=base), rep("n2", False, sampled_at=base)])
        post_batch(
            client,
            [rep("n1", True, boot=f"boot-r{round_no}", sampled_at=base + timedelta(minutes=1))],
        )
    pa = periods(client, "shard-a")
    assert [p["status"] for p in pa] == ["recovered", "recovered"]
    assert pa[0]["started_at"] <= pa[0]["recovered_at"] <= pa[1]["started_at"]


# ---------- 并发 ----------


def test_concurrent_batches_serialized(client):
    seed_all_healthy(client)
    base = T0 + timedelta(minutes=5)

    # 10 批异常（采样时间递增）+ 10 批健康（采样时间更晚），并发提交
    def make_batch(i, healthy):
        ts = base + timedelta(seconds=(100 + i) if healthy else i)
        return [
            rep("n1", healthy, boot=f"b{i}", sampled_at=ts),
            rep("n2", healthy, boot=f"b{i}", sampled_at=ts),
        ]

    batches = [make_batch(i, False) for i in range(10)]
    batches += [make_batch(i, True) for i in range(10)]

    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(lambda b: post_batch(client, b).status_code, batches))
    assert codes == [201] * 20

    pa = periods(client, "shard-a")
    # 批次串行生效：要么异常批次全部沦为过期上报（0 条记录），
    # 要么先开一条、再由更晚的健康批次恢复（恰好 1 条且已恢复）。
    assert len(pa) <= 1
    if pa:
        assert pa[0]["status"] == "recovered"
        assert pa[0]["recovered_at"] is not None
    # 最终生效状态：健康批次采样时间最晚，票数恢复过半
    assert client.get("/liveness/n1").json()["healthy"] is True
    body = client.get("/shards/shard-a/vote-losses").json()
    assert body["current_healthy_votes"] == 3
