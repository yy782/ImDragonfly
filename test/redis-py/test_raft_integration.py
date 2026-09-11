"""Raft 集成测试：单节点持久化 / 三节点复制 / leader 崩溃后继续写入.

对应 /home/yy/programs/测试.md 的三条用例：
  1. 单节点：SET/MSET 后重启进程，GET 仍能读到（验证日志落盘 + 恢复重放）。
  2. 三节点：向 leader 写 SET/MSET，在 T1/T2 上 GET 能读到（验证多数派复制）。
  3. 在 2 的基础上杀死 leader，等新 leader 选出，再写 SET/MSET，
     在 follower 上 GET 能读到（验证选主 + 复制在新 term 下继续）。

与 test_all.py 不同，这里不连外部已启动的服务，而是自己拉起独立端口的
imdragonfly 进程（每个用例一份临时目录，互不干扰）。

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
import tempfile
import time

import pytest
import redis

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(HERE, "../.."))

SHARDS = 2


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


def _free_port():
    """绑定 0 获取一个随机空闲端口（释放后给子进程用，竞态可接受）."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _alloc_ports(n):
    """一次性申请 n 个互不相同的空闲端口."""
    ports = set()
    while len(ports) < n:
        ports.add(_free_port())
    return list(ports)


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


def _write_conf(tmpdir, *, node_id, redis_port, raft_ports, log_path,
                seed_raft_port=None, joining=False):
    """生成一份 raft 配置 JSON，返回文件路径."""
    conf = {
        "shards": SHARDS,
        "port": redis_port,
        "use_raft": True,
        "raft_node_id": node_id,
        "raft_is_leader": False,
        "raft_peers": ",".join(f"127.0.0.1:{p}" for p in raft_ports),
        "raft_log_path": log_path,
        "raft_rpc_timeout_ms": 500,
        "election_timeout_ms": 300,
        "registered_buf_count": 256,
        "registered_buf_size": 16384,
        "task_queue_size": 16384,
    }
    if joining:
        conf["raft_joining"] = True
        conf["raft_seed"] = f"127.0.0.1:{seed_raft_port}"
    path = os.path.join(tmpdir, f"raft_n{node_id}.conf")
    with open(path, "w") as f:
        json.dump(conf, f, indent=2)
    return path


def _start_node(binary, conf_path, tmpdir, port, proc_list):
    """启动一个节点进程并等待其端口可连，返回 NodeProc."""
    proc = subprocess.Popen(
        [binary, "--config", conf_path],
        cwd=tmpdir,  # 日志/raft 落盘都放临时目录
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
# 用例 1：单节点持久化（重启后数据还在）
# ═══════════════════════════════════════════════════════════

def test_single_node_persistence_after_restart(pytestconfig):
    """单节点：SET/MSET → 重启进程 → GET 仍能读到."""
    binary = _binary_path(pytestconfig)
    nodes = []
    with tempfile.TemporaryDirectory() as tmp:
        redis_port, raft_port = _alloc_ports(2)
        conf = _write_conf(
            tmp,
            node_id=0,
            redis_port=redis_port,
            raft_ports=[raft_port],
            log_path=os.path.join(tmp, "raft_n0.log"),
        )
        try:
            node = _start_node(binary, conf, tmp, redis_port, nodes)

            # 等它自选为单节点 leader（单节点应立即成为 leader）
            leader = _wait_leader([node], timeout=15)
            assert leader is not None, "单节点未成为 leader，无法写入"
            c = leader.client()
            assert c.set("it:single:k1", "v1") is True
            assert c.mset({"it:single:k2": "v2", "it:single:k3": "v3"}) is True
            assert c.get("it:single:k1") == "v1"
            assert c.mget(["it:single:k2", "it:single:k3"]) == ["v2", "v3"]
            c.close()

            # 重启：杀进程，用同一份配置（同一 raft 日志文件）重新拉起
            node.proc.kill()
            node.proc.wait(timeout=10)
            time.sleep(0.3)

            node2 = _start_node(binary, conf, tmp, redis_port, nodes)
            leader2 = _wait_leader([node2], timeout=15)
            assert leader2 is not None, "重启后单节点未成为 leader"
            c2 = leader2.client()
            assert c2.get("it:single:k1") == "v1", "重启后 SET 的数据丢失"
            assert c2.mget(["it:single:k2", "it:single:k3"]) == ["v2", "v3"], \
                "重启后 MSET 的数据丢失"
            c2.close()
        finally:
            _kill_all(nodes)


# ═══════════════════════════════════════════════════════════
# 三节点公共 fixture：拉起 3 个节点，选出一个 leader
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
    """拉起 3 节点集群并选出 leader，测试结束回收所有进程."""
    binary = _binary_path(pytestconfig)
    nodes = []
    tmp = tempfile.mkdtemp(prefix="dfly_raft_")
    redis_ports = _alloc_ports(3)
    raft_ports = _alloc_ports(3)
    confs = []
    for i in range(3):
        confs.append(
            _write_conf(
                tmp,
                node_id=i,
                redis_port=redis_ports[i],
                raft_ports=raft_ports,
                log_path=os.path.join(tmp, f"raft_n{i}.log"),
            )
        )
    try:
        for i in range(3):
            _start_node(binary, confs[i], tmp, redis_ports[i], nodes)
        leader = _wait_leader(nodes, timeout=20)
        if leader is None:
            logs = "\n".join(
                f"--- node {i} ---\n{n.stdout()}" for i, n in enumerate(nodes)
            )
            pytest.fail(f"3 节点集群未选出 leader:\n{logs}")
        yield Cluster(nodes, leader)
    finally:
        _kill_all(nodes)
        # 临时目录不强制删除，便于失败后排查；由 TemporaryDirectory 语义
        # 之外，这里手动尽力清理。
        try:
            for f in os.listdir(tmp):
                os.remove(os.path.join(tmp, f))
            os.rmdir(tmp)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════
# 用例 2：三节点复制（leader 写入，follower 能读到）
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
# 用例 3：leader 崩溃后，新 leader 继续接受写入并复制
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
