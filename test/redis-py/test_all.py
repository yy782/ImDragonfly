"""ImDragonfly 全部测试 —— 一个文件，通过命令行选择运行哪些测试.

一键运行全部:
    python3 -m pytest test_all.py -v

按类别运行:
    python3 -m pytest test/redis-py/test_all.py -v -m basic
    python3 -m pytest test/redis-py/test_all.py -v -m expire
    python3 -m pytest test/redis-py/test_all.py -v -m list
    python3 -m pytest test/redis-py/test_all.py -v -m hash
    python3 -m pytest test/redis-py/test_all.py -v -m set
    python3 -m pytest test/redis-py/test_all.py -v -m zset
    python3 -m pytest test/redis-py/test_all.py -v -m transaction
    python3 -m pytest test/redis-py/test_all.py -v -m concurrent
    python3 -m pytest test/redis-py/test_all.py -v -m benchmark

按名称运行单个测试:
    python3 -m pytest test/redis-py/test_all.py::test_set_and_get -v
    python3 -m pytest test/redis-py/test_all.py::test_concurrent_connections -v

运行 benchmark:
    python3 -m pytest test/redis-py/test_all.py -v --benchmark
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
# 列表命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.list
def test_lpush_and_llen(clean_redis):
    r, track = clean_redis
    key = "test:list:key"
    track(key)
    assert r.lpush(key, "a", "b", "c") == 3
    assert r.llen(key) == 3


@pytest.mark.list
def test_lrange_and_lindex(clean_redis):
    r, track = clean_redis
    key = "test:list:key2"
    track(key)
    r.lpush(key, "a", "b", "c")
    assert r.lrange(key, 0, -1) == ["c", "b", "a"]
    assert r.lindex(key, 0) == "c"
    assert r.lindex(key, -1) == "a"


@pytest.mark.list
def test_rpush(clean_redis):
    r, track = clean_redis
    key = "test:list:key3"
    track(key)
    r.lpush(key, "a", "b", "c")
    assert r.rpush(key, "d") == 4
    assert r.lrange(key, 0, -1) == ["c", "b", "a", "d"]


@pytest.mark.list
def test_lset(clean_redis):
    r, track = clean_redis
    key = "test:list:key4"
    track(key)
    r.lpush(key, "a", "b", "c")
    assert r.lset(key, 1, "x") is True
    assert r.lrange(key, 0, -1) == ["c", "x", "a"]


@pytest.mark.list
def test_lpop_rpop(clean_redis):
    r, track = clean_redis
    key = "test:list:key5"
    track(key)
    r.lpush(key, "a", "b", "c")
    assert r.lpop(key) == "c"
    assert r.rpop(key) == "a"
    assert r.llen(key) == 1


@pytest.mark.list
def test_lrem(clean_redis):
    r, track = clean_redis
    key = "test:list:key6"
    track(key)
    r.rpush(key, "x", "a", "x")
    assert r.lrem(key, 0, "x") == 2
    assert r.llen(key) == 1


@pytest.mark.list
def test_linsert(clean_redis):
    r, track = clean_redis
    key = "test:list:key7"
    track(key)
    r.rpush(key, "a")
    assert r.linsert(key, "AFTER", "a", "new") == 2
    assert r.lrange(key, 0, -1) == ["a", "new"]


# ═══════════════════════════════════════════════════════════
# 哈希命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.hash
def test_hset_and_hget(clean_redis):
    r, track = clean_redis
    key = "test:hash:key"
    track(key)
    assert r.hset(key, "f1", "v1") == 1
    assert r.hset(key, "f2", "v2") == 1
    assert r.hget(key, "f1") == "v1"
    assert r.hget(key, "f3") is None


@pytest.mark.hash
def test_hexists_and_hlen(clean_redis):
    r, track = clean_redis
    key = "test:hash:key2"
    track(key)
    r.hset(key, "f1", "v1"); r.hset(key, "f2", "v2")
    assert r.hexists(key, "f1") is True
    assert r.hexists(key, "f3") is False
    assert r.hlen(key) == 2


@pytest.mark.hash
def test_hdel(clean_redis):
    r, track = clean_redis
    key = "test:hash:key3"
    track(key)
    r.hset(key, "f1", "v1"); r.hset(key, "f2", "v2")
    assert r.hdel(key, "f1") == 1
    assert r.hlen(key) == 1


# ═══════════════════════════════════════════════════════════
# 集合命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.set
def test_sadd_and_scard(clean_redis):
    r, track = clean_redis
    key = "test:set:key"
    track(key)
    assert r.sadd(key, "a", "b", "c") == 3
    assert r.sadd(key, "a") == 0
    assert r.scard(key) == 3


@pytest.mark.set
def test_srem(clean_redis):
    r, track = clean_redis
    key = "test:set:key3"
    track(key)
    r.sadd(key, "a", "b", "c")
    assert r.srem(key, "a", "b") == 2
    assert r.scard(key) == 1


# ═══════════════════════════════════════════════════════════
# 有序集合命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.zset
def test_zadd_and_zcard(clean_redis):
    r, track = clean_redis
    key = "test:zset:key"
    track(key)
    assert r.execute_command("ZADD", key, 1, "a", 2, "b", 3, "c") == 3
    assert r.execute_command("ZCARD", key) == 3


@pytest.mark.zset
def test_zscore(clean_redis):
    r, track = clean_redis
    key = "test:zset:key2"
    track(key)
    r.execute_command("ZADD", key, 1, "a", 2, "b")
    assert r.execute_command("ZSCORE", key, "a") == 1.0


@pytest.mark.zset
def test_zrank_zrevrank(clean_redis):
    r, track = clean_redis
    key = "test:zset:key3"
    track(key)
    r.execute_command("ZADD", key, 1, "a", 2, "b", 3, "c")
    assert r.execute_command("ZRANK", key, "a") == 0
    assert r.execute_command("ZRANK", key, "c") == 2
    assert r.execute_command("ZREVRANK", key, "a") == 2
    assert r.execute_command("ZREVRANK", key, "c") == 0


@pytest.mark.zset
def test_zrange(clean_redis):
    r, track = clean_redis
    key = "test:zset:key4"
    track(key)
    r.execute_command("ZADD", key, 1, "a", 2, "b", 3, "c")
    assert r.execute_command("ZRANGE", key, 0, 1) == ["a", "b"]


@pytest.mark.zset
def test_zrem(clean_redis):
    r, track = clean_redis
    key = "test:zset:key5"
    track(key)
    r.execute_command("ZADD", key, 1, "a", 2, "b", 3, "c")
    assert r.execute_command("ZREM", key, "b") == 1
    assert r.execute_command("ZCARD", key) == 2


# ═══════════════════════════════════════════════════════════
# 事务命令
# ═══════════════════════════════════════════════════════════

@pytest.mark.transaction
def test_multi_exec(clean_redis):
    r, track = clean_redis
    k1, k2 = "test:tx:k1", "test:tx:k2"
    track(k1); track(k2)
    r.set(k1, "100"); r.set(k2, "200")
    r.execute_command("MULTI")
    r.execute_command("SET", k1, "150")
    r.execute_command("SET", k2, "250")
    assert r.execute_command("EXEC") == [True, True]
    assert r.get(k1) == "150"
    assert r.get(k2) == "250"


@pytest.mark.transaction
def test_discard(clean_redis):
    r, track = clean_redis
    key = "test:tx:discard"
    track(key)
    r.set(key, "100")
    r.execute_command("MULTI")
    r.execute_command("SET", key, "should_not_set")
    r.execute_command("DISCARD")
    assert r.get(key) == "100"


@pytest.mark.transaction
def test_watch_exec(clean_redis):
    r, track = clean_redis
    key = "test:tx:watch"
    track(key)
    r.set(key, "100")
    r.watch(key)
    r.execute_command("MULTI")
    r.execute_command("SET", key, "watched_value")
    assert r.execute_command("EXEC") == [True]
    assert r.get(key) == "watched_value"
    r.unwatch()
