"""Raft 集成测试：三节点复制 / leader 崩溃后继续写入.

对应两条用例：
  1. 三节点：向 leader 写 SET/MSET，在 T1/T2 上 GET 能读到（验证多数派复制）。
  2. 在 1 的基础上杀死 leader，等新 leader 选出，再写 SET/MSET，
     在 follower 上 GET 能读到（验证选主 + 复制在新 term 下继续）。

与 test_all.py 不同，这里不连外部已启动的服务，而是自己拉起 imdragonfly
进程。三个节点使用 test/redis-py/test_conf 下的固定配置（raft_n{0,1,2}.conf，
redis 端口 6390/6391/6392，raft 端口 7000/7001/7002），日志与 raft 数据落盘
到配置里 log_dir / raft_log_path 指定的相对位置（以 test/redis-py 为基准）。

运行（从项目根 ImDragonfly 目录）:
    python3 -m pytest test/redis-py/test_raft_integration.py -v

可选指定二进制路径:
    python3 -m pytest test/redis-py/test_raft_integration.py -v \\
        --imdragonfly-bin=./build/imdragonfly
"""

import json
import os
import socket
import subprocess
import time

import pytest
import redis

HERE = os.path.dirname(os.path.abspath(__file__))
CONF_DIR = os.path.join(HERE, "test_conf")
NODE_CONFS = ["raft_n0.conf", "raft_n1.conf", "raft_n2.conf"]


# ═══════════════════════════════════════════════════════════
# 进程 / 端口 / 等待 工具
# ═══════════════════════════════════════════════════════════

def _wait_until(cond, timeout=20.0, interval=0.05):
    """轮询等待条件成立，超时返回 False."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


def _tcp_connectable(host, port):
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _binary_path(pytestconfig):
    raw = pytestconfig.getoption("--imdragonfly-bin")
    cand_here = os.path.normpath(os.path.join(HERE, raw))
    cand_cwd = os.path.abspath(raw)
    binary = cand_here if os.path.exists(cand_here) else cand_cwd
    if not os.path.exists(binary):
        pytest.fail(
            f"找不到 imdragonfly 二进制: {binary} "
            f"(尝试了 {cand_here} 和 {cand_cwd})"
        )
    return binary


class NodeProc:
    """一个 imdragonfly 进程 + 它的 redis 端口."""

    def __init__(self, proc, redis_port):
        self.proc = proc
        self.redis_port = redis_port

    def alive(self):
        return self.proc.poll() is None

    def client(self, timeout=5):
        return redis.Redis(
            host="127.0.0.1",
            port=self.redis_port,
            protocol=2,
            decode_responses=True,
            socket_connect_timeout=timeout,
            socket_timeout=timeout,
        )

    def stdout(self):
        try:
            if self.proc.poll() is not None:
                out, _ = self.proc.communicate(timeout=3)
                return out or ""
        except Exception:
            pass
        return ""


def _conf_port(conf_path):
    """读取配置 JSON 里的 redis 端口（test_conf 里三个 conf 固定为 6390/6391/6392）."""
    with open(conf_path) as f:
        return int(json.load(f)["port"])


def _raft_log_paths(conf_paths):
    """从各配置读取 raft_log_path，返回其（相对 HERE）绝对路径及 .state 路径."""
    paths = []
    for conf in conf_paths:
        with open(conf) as f:
            rel = json.load(f).get("raft_log_path", "")
        if rel:
            abs_path = os.path.join(HERE, rel)
            paths.append(abs_path)
            paths.append(abs_path + ".state")
    return paths


def _start_node(binary, conf_path, port, proc_list):
    """启动一个节点进程并等待其端口可连，返回 NodeProc.

    cwd 固定为 test/redis-py（配置文件所在目录），让 conf 里的 log_dir、
    raft_log_path 等相对路径按配置字面解析到该目录下。
    """
    proc = subprocess.Popen(
        [binary, "config=" + conf_path],
        cwd=HERE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    node = NodeProc(proc, port)
    proc_list.append(node)
    if not _wait_until(lambda: _tcp_connectable("127.0.0.1", port)):
        out = node.stdout()
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        pytest.fail(f"节点启动失败 (redis port={port}):\n{out}")
    return node


def _kill_all(nodes):
    for n in nodes:
        if n.proc.poll() is None:
            n.proc.kill()
            try:
                n.proc.wait(timeout=5)
            except Exception:
                pass


def _wait_leader(nodes, timeout=20.0):
    """轮询直到能确定某个节点是 leader。

    判定方式：向每个节点发写命令，能成功写入（返回 True）的那个即 leader。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in nodes:
            if not n.alive():
                continue
            try:
                c = n.client(timeout=2)
                if c.set("__leader_probe__", "1") is True:
                    c.delete("__leader_probe__")
                    c.close()
                    return n
                c.close()
            except redis.RedisError:
                pass
            except Exception:
                pass
        time.sleep(0.1)
    return None


# ═══════════════════════════════════════════════════════════
# 三节点公共 fixture：用 test_conf 的三个 .conf 拉起 3 个节点，选出一个 leader
# ═══════════════════════════════════════════════════════════

class Cluster:
    def __init__(self, nodes, leader):
        self.nodes = nodes      # 三个 NodeProc
        self.leader = leader    # NodeProc（当下 leader）

    def followers(self):
        return [n for n in self.nodes if n is not self.leader and n.alive()]

    def find_leader(self, timeout=20):
        self.leader = _wait_leader(self.nodes, timeout=timeout)
        return self.leader

    def follower_clients(self, timeout=5):
        """为每个 follower 建一个客户端，返回 [(node, client)]."""
        out = []
        for n in self.followers():
            out.append((n, n.client(timeout=timeout)))
        return out


@pytest.fixture
def cluster3(pytestconfig):
    """用 test_conf 的三个 .conf 拉起 3 节点集群并选出 leader，测试结束回收所有进程."""
    binary = _binary_path(pytestconfig)
    nodes = []
    confs = [os.path.join(CONF_DIR, name) for name in NODE_CONFS]
    redis_ports = [_conf_port(c) for c in confs]
    # 清理上次运行残留的 raft 日志/状态文件，保证每个用例从干净状态开始。
    for path in _raft_log_paths(confs):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    try:
        for conf, port in zip(confs, redis_ports):
            _start_node(binary, conf, port, nodes)
        leader = _wait_leader(nodes, timeout=20)
        if leader is None:
            logs = "\n".join(
                f"--- node {i} ---\n{n.stdout()}" for i, n in enumerate(nodes)
            )
            pytest.fail(f"3 节点集群未选出 leader:\n{logs}")
        yield Cluster(nodes, leader)
    finally:
        _kill_all(nodes)


# ═══════════════════════════════════════════════════════════
# 用例 1：三节点复制（leader 写入，follower 能读到）
# ═══════════════════════════════════════════════════════════

def test_three_node_replication(cluster3):
    """三节点：向 leader 写 SET/MSET，T1/T2 上 GET 能读到."""
    leader = cluster3.leader
    assert leader is not None
    lc = leader.client()
    assert lc.set("it:repl:k1", "v1") is True
    assert lc.mset({"it:repl:k2": "v2", "it:repl:k3": "v3"}) is True
    assert lc.get("it:repl:k1") == "v1"
    lc.close()

    # 复制是异步的，轮询等两个 follower 都读到
    for node, fc in cluster3.follower_clients():
        assert _wait_until(lambda c=fc: c.get("it:repl:k1") == "v1", timeout=10), \
            f"follower (redis port={node.redis_port}) 未复制到 k1"
        assert fc.mget(["it:repl:k2", "it:repl:k3"]) == ["v2", "v3"], \
            f"follower (redis port={node.redis_port}) 未复制到 MSET 数据"
        fc.close()


# ═══════════════════════════════════════════════════════════
# 用例 2：leader 崩溃后，新 leader 继续接受写入并复制
# ═══════════════════════════════════════════════════════════

def test_leader_crash_then_write(cluster3):
    """杀死 leader → 等新 leader → 再写 SET/MSET → follower GET 能读到."""
    old_leader = cluster3.leader
    assert old_leader is not None

    # 先写一批，确认基线可用
    lc = old_leader.client()
    assert lc.set("it:failover:before", "b") is True
    lc.close()

    # 杀掉 leader，模拟崩溃
    old_leader.proc.kill()
    old_leader.proc.wait(timeout=10)

    # 等剩余节点选出新 leader
    new_leader = cluster3.find_leader(timeout=25)
    assert new_leader is not None, "旧 leader 崩溃后集群未能选出新 leader"
    assert new_leader is not old_leader

    # 新 leader 写 SET/MSET
    nc = new_leader.client()
    assert nc.set("it:failover:after1", "a1") is True
    assert nc.mset({"it:failover:after2": "a2",
                    "it:failover:after3": "a3"}) is True
    nc.close()

    # 在存活的 follower 上 GET 确认复制（注意 followers() 已排除新 leader）
    remaining = cluster3.followers()
    assert remaining, "崩溃后没有存活的 follower 可验证"
    for node in remaining:
        fc = node.client()
        assert _wait_until(lambda c=fc: c.get("it:failover:after1") == "a1",
                           timeout=10), \
            f"新 leader 写入的数据未复制到 follower (redis port={node.redis_port})"
        assert fc.mget(["it:failover:after2", "it:failover:after3"]) == \
            ["a2", "a3"], f"MSET 数据未复制到 follower (redis port={node.redis_port})"
        # 崩溃前已提交的数据也应还在
        assert fc.get("it:failover:before") == "b", \
            "崩溃前已提交的数据在新 leader 下丢失"
        fc.close()
