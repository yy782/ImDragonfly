"""ImDragonfly 全部测试 —— 一个文件，通过命令行选择运行哪些测试.

一键运行全部:
    python3 -m pytest test_all.py -v

按类别运行:
    python3 -m pytest test_all.py -v -m basic
    python3 -m pytest test_all.py -v -m expire
    python3 -m pytest test_all.py -v -m concurrent
    python3 -m pytest test_all.py -v -m benchmark

按名称运行单个测试:
    python3 -m pytest test_all.py::test_set_and_get -v
    python3 -m pytest test_all.py::test_concurrent_connections -v

运行 benchmark:
    python3 -m pytest test_all.py -v --benchmark
"""

import threading
import time

import pytest
import redis


# ═══════════════════════════════════════════════════════════
# 基本命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.basic
def test_set_and_get(clean_redis):
    r, track = clean_redis
    key = "test:basic:key"
    track(key)
    assert r.set(key, "hello") is True
    assert r.get(key) == "hello"


@pytest.mark.basic
def test_mset_and_mget(clean_redis):
    r, track = clean_redis
    k1, k2 = "test:basic:k1", "test:basic:k2"
    track(k1); track(k2)
    assert r.mset({k1: "v1", k2: "v2"}) is True
    assert r.mget([k1, k2]) == ["v1", "v2"]


@pytest.mark.basic
def test_exists_and_del(clean_redis):
    r, track = clean_redis
    k1, k2 = "test:basic:ek1", "test:basic:ek2"
    track(k1); track(k2)
    r.set(k1, "a"); r.set(k2, "b")
    assert r.exists(k1, k2) == 2
    assert r.delete(k1, k2) == 2
    assert r.exists(k1, k2) == 0
    assert r.get(k1) is None


# ═══════════════════════════════════════════════════════════
# 过期命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.expire
def test_expire_and_ttl(clean_redis):
    r, track = clean_redis
    key = "test:expire:key"
    track(key)
    r.set(key, "expire_value")
    assert r.expire(key, 10) is True
    assert r.ttl(key) > 0


@pytest.mark.expire
def test_expiretime(clean_redis):
    r, track = clean_redis
    key = "test:expire:key2"
    track(key)
    r.set(key, "v")
    r.expire(key, 5)
    assert r.expiretime(key) > 0


# ═══════════════════════════════════════════════════════════
# 并发命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.concurrent
def test_concurrent_mset_mget(clean_redis, pytestconfig):  
    # 测试服务端还是会崩溃，等完善调试工具再修,
    # 目前测试稳定了，应该还是事务对象生命周期导致UB
    """多线程并发 MSET/MGET 测试.

    3 个线程, 各用独立连接, 各执行 100 轮:
      MSET 写入 10 个键 → MGET 读出 → 断言一致
    共 3×100=300 轮, 每轮 10 个键, 合计 3000 次读写.
    """
    _, track = clean_redis
    host = pytestconfig.getoption("--redis-host")
    port = pytestconfig.getoption("--redis-port")
    db = pytestconfig.getoption("--redis-db")
    errors = []
    def worker(tid):
        try:
            c = redis.Redis(host=host, port=port, db=db, protocol=2,
                            decode_responses=True,
                            socket_connect_timeout=5, socket_timeout=10)
            c.ping()
            for round_i in range(100):
                kv = {}
                for k in range(10):
                    key = f"test:c:{tid}_{round_i}_{k}"
                    kv[key] = f"v{tid}{round_i}{k}"
                    track(key)
                assert c.mset(kv)
                assert c.mget(kv.keys()) == list(kv.values())
            c.close()
        except Exception as e:
            errors.append(e)
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in ts: t.start()
    deadline = time.time() + 60
    for t in ts:
        remaining = deadline - time.time()
        if remaining > 0:
            t.join(timeout=remaining)
    alive = [t for t in ts if t.is_alive()]
    assert not alive, f"并发测试超时({60}s)，仍有 {len(alive)} 个线程未结束"
    assert not errors, f"并发测试异常: {errors}"
