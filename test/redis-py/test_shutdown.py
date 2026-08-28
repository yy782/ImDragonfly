"""SHUTDOWN 集成测试：验证服务端能被 SHUTDOWN 命令干净地停机.

与其它测试不同，本测试不连接外部已启动的服务，而是自己拉起一个
imdragonfly 进程（独立随机端口），因为 SHUTDOWN 会把服务停掉。

运行（从项目根 ImDragonfly 目录）:
    python3 -m pytest test/redis-py/test_shutdown.py -v

可选指定二进制路径:
    python3 -m pytest test/redis-py/test_shutdown.py -v \\
        --imdragonfly-bin=./build/imdragonfly
"""

import os
import socket
import subprocess
import time

import pytest
import redis

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(HERE, "../.."))
BIN_DEFAULT = os.path.normpath(os.path.join(HERE, "../../build/imdragonfly"))

SHARDS = "2"  # 分片数，越少启动越快


def _wait_until(cond, timeout=15.0, interval=0.1):
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


def _free_port():
    """绑定 0 获取一个随机空闲端口（释放后立即给子进程用，竞态可接受）."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def imdragonfly(pytestconfig):
    """启动独立 imdragonfly 进程，返回 (proc, port)。测试结束后兜底回收."""
    raw = pytestconfig.getoption("--imdragonfly-bin")
    cand_here = os.path.normpath(os.path.join(HERE, raw))
    cand_cwd = os.path.abspath(raw)
    binary = cand_here if os.path.exists(cand_here) else cand_cwd
    if not os.path.exists(binary):
        pytest.fail(f"找不到 imdragonfly 二进制: {binary} (尝试了 {cand_here} 和 {cand_cwd})")

    port = _free_port()
    proc = subprocess.Popen(
        [binary, SHARDS, str(port)],
        cwd=PROJECT_ROOT,  # main.cpp 会在 cwd 下建 ./logs 目录
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if not _wait_until(lambda: _tcp_connectable("127.0.0.1", port)):
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        out, _ = proc.communicate(timeout=5)
        pytest.fail(f"imdragonfly 启动失败 (rc={proc.returncode}):\n{out}")

    yield proc, port

    if proc.poll() is None:  # 测试失败导致未停机时兜底
        proc.kill()
        proc.wait(timeout=5)


@pytest.fixture(scope="module")
def client(imdragonfly):
    """连到独立实例的 redis 客户端."""
    _, port = imdragonfly
    c = redis.Redis(
        host="127.0.0.1",
        port=port,
        protocol=2,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    assert c.ping()
    yield c
    try:
        c.close()
    except Exception:
        pass


def test_server_ready_before_shutdown(client):
    """停机前服务正常：SET/GET/PING 都能用."""
    assert client.set("shutdown_test_key", "v1") is True
    assert client.get("shutdown_test_key") == "v1"


def test_shutdown_clean_exit(imdragonfly, client):
    """SHUTDOWN 后：服务端断连、进程自行退出且 exit code 为 0、端口关闭."""
    proc, port = imdragonfly

    try:
        # 服务端 SHUTDOWN 不回包直接停机，redis-py 会读到连接关闭
        client.shutdown()
    except redis.RedisError:
        pass

    # 进程应在无外力干预下自行退出，且返回码为 0
    assert _wait_until(lambda: proc.poll() is not None), "进程未在超时内退出"
    assert proc.returncode == 0, f"SHUTDOWN 后进程返回码 = {proc.returncode}"

    # 端口不再监听
    assert _wait_until(
        lambda: not _tcp_connectable("127.0.0.1", port)
    ), "SHUTDOWN 后端口仍可连接"


def test_connection_closed_after_shutdown(imdragonfly):
    """SHUTDOWN 后：新连接直接拒绝（服务端已彻底停止监听）."""
    _, port = imdragonfly
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=2)
