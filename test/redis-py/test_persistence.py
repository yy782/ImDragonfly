"""RDB 持久化集成测试：SAVE 生成 dump、重启恢复、过期键扫描淘汰。

这些测试自管理服务器进程（subprocess 启动 imdragonfly 二进制），每个测试
使用独立的临时数据目录和空闲端口，互不影响。RDB 是唯一持久化机制，
重启加载完全依赖 dump-<sid>.rdb 文件。

运行：
    pytest test_persistence.py --imdragonfly-bin /path/to/imdragonfly
"""

import os
import signal
import socket
import subprocess
import time

import pytest
import redis

SHARDS = 4
STARTUP_TIMEOUT = 15


def _free_port():
    """找一个空闲端口（绑定端口 0 后立即释放）."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(port, proc, timeout=STARTUP_TIMEOUT):
    """等待服务器就绪：进程存活 + ping 通过，返回客户端连接."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(
                f"服务器启动即退出, returncode={proc.returncode}, 见 data_dir/server.log"
            )
        try:
            c = redis.Redis(
                host="127.0.0.1",
                port=port,
                protocol=2,
                decode_responses=True,
                socket_connect_timeout=2,
            )
            c.ping()
            return c
        except redis.ConnectionError:
            time.sleep(0.2)
    pytest.fail(f"服务器 {timeout}s 内未就绪 (port={port})")


class Server:
    """自管理服务器：启动/停止/重启，数据目录为独立临时目录."""

    def __init__(self, bin_path, port, data_dir, log_path):
        self.bin_path = bin_path
        self.port = port
        self.data_dir = data_dir
        self.log_path = log_path
        self.proc = None
        self.client = None
        self.start()

    def start(self):
        # RDB 持久化测试依赖快照的保存与加载，故不传 --no-rdb（与一般集成测试相反）。
        with open(self.log_path, "ab") as logf:
            self.proc = subprocess.Popen(
                [self.bin_path, str(SHARDS), str(self.port)],
                cwd=str(self.data_dir),
                stdout=logf,
                stderr=subprocess.STDOUT,
            )
        self.client = _wait_ready(self.port, self.proc)

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self.proc = None
        self.client = None

    def restart(self):
        """停止后重新启动（保留数据目录，验证重启加载）."""
        self.stop()
        self.start()

    def kill(self):
        """SIGKILL 强杀，模拟崩溃：无优雅退出、无额外保存路径."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None
        self.client = None


@pytest.fixture
def server(request, tmp_path):
    """每个测试一个独立服务器实例（独立端口 + 数据目录）."""
    bin_path = os.path.abspath(request.config.getoption("--imdragonfly-bin"))
    if not os.path.exists(bin_path):
        pytest.fail(f"imdragonfly 二进制不存在: {bin_path}")

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    svr = Server(bin_path, _free_port(), data_dir, data_dir / "server.log")
    yield svr
    svr.stop()


def _write_all_types(r):
    assert r.set("it:str", "hello")
    assert r.set("it:int", 42)
    assert r.rpush("it:list", "a", "b", "c") == 3
    assert r.hset("it:hash", "f1", "v1") == 1
    assert r.sadd("it:set", "x", "y", "z") == 3
    assert r.zadd("it:zset", {"one": 1, "two": 2}) == 2


def _verify_all_types(r):
    assert r.get("it:str") == "hello"
    assert r.get("it:int") == "42"
    assert r.lrange("it:list", 0, -1) == ["a", "b", "c"]
    assert r.hget("it:hash", "f1") == "v1"
    assert r.smembers("it:set") == {"x", "y", "z"}
    assert r.zrange("it:zset", 0, -1, withscores=True) == [("one", 1.0), ("two", 2.0)]


def _dump_paths(svr):
    return [svr.data_dir / ".rdb" / f"dump-{i}.rdb" for i in range(SHARDS)]


def _all_dump_bytes(svr):
    return b"".join(p.read_bytes() for p in _dump_paths(svr) if p.exists())


def test_save_generates_all_dump_files(server):
    """SAVE 为每个 shard 生成一个非空 dump 文件."""
    r = server.client
    _write_all_types(r)
    assert r.save() is True

    for p in _dump_paths(server):
        assert p.exists(), f"缺少 dump 文件: {p}"
        assert p.stat().st_size > 0, f"dump 文件为空: {p}"


def test_restart_restores_all_types(server):
    """写入全类型数据 → SAVE → 重启 → 数据完整恢复."""
    r = server.client
    _write_all_types(r)
    assert r.save() is True

    server.restart()
    _verify_all_types(server.client)


def test_expired_key_purged_on_save_and_restart(server):
    """过期键被 SAVE 扫描淘汰：不落盘、内存删除、重启后不恢复."""
    r = server.client
    assert r.set("exp:ttl", "gone")
    assert r.expire("exp:ttl", 1)
    assert r.set("exp:keep", "alive")

    time.sleep(2.2)  # 等 exp:ttl 过期（不访问，验证 SAVE 扫描路径而非惰性删除）

    assert r.save() is True

    all_dump = _all_dump_bytes(server)
    assert b"exp:ttl" not in all_dump, "过期键不应写入 dump"
    assert b"exp:keep" in all_dump, "存活键应写入 dump"

    # 内存已删 + 重启后不恢复
    assert r.get("exp:ttl") is None
    server.restart()
    assert server.client.get("exp:ttl") is None
    assert server.client.get("exp:keep") == "alive"


def test_save_is_repeatable(server):
    """连续多次 SAVE 全部成功（幂等）. """
    r = server.client
    _write_all_types(r)
    for _ in range(3):
        assert r.save() is True


def test_without_save_data_lost_after_restart(server):
    """RDB 是唯一持久化机制：不 SAVE 就重启，数据丢失."""
    r = server.client
    assert r.set("volatile:key", "gone")
    server.restart()
    assert server.client.get("volatile:key") is None


def test_crash_recovery_via_auto_snapshot(server):
    """SET 键 → 等自动快照落盘 → SIGKILL 崩溃 → 重启 → GET 全部恢复.

    与 test_restart_restores_all_types 的区别：不显式 SAVE，依赖启动约 3 秒后
    的自动周期快照把数据写入 dump；SIGKILL 保证无任何优雅退出路径参与保存，
    恢复完全靠已落盘的 dump 文件。
    """
    r = server.client
    assert r.set("crash:str", "hello")
    assert r.set("crash:int", 123)
    assert r.rpush("crash:list", "a", "b") == 2
    assert r.hset("crash:hash", "f1", "v1") == 1

    # 等自动快照（周期 3s）至少完成一轮落盘
    deadline = time.time() + 10
    while time.time() < deadline:
        if _all_dump_bytes(server):
            break
        time.sleep(0.5)
    assert _all_dump_bytes(server), "自动快照未在超时内生成任何 dump"

    # SIGKILL 模拟崩溃
    server.kill()
    # 同数据目录重启
    server.start()

    c = server.client
    assert c.get("crash:str") == "hello"
    assert c.get("crash:int") == "123"
    assert c.lrange("crash:list", 0, -1) == ["a", "b"]
    assert c.hget("crash:hash", "f1") == "v1"
