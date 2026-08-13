"""命令名大小写不敏感集成测试：

服务端命令按大写注册（SET），但客户端可能发小写（set）。
验证 ToUpperIfNeeded 归一化逻辑：
  1. 先发小写 set key1 val1，等回复 -> +OK
  2. 再发大写 SET key1 val1，等回复 -> +OK

运行（需先启动 ImDragonfly 服务，占用 6379 端口）:
    ./build/imdragonfly 4
    python3 -m pytest test/redis-py/test_case_insensitive.py -v \\
        --redis-host=127.0.0.1 --redis-port=6379
"""

import socket

import pytest

# RESP 编码: set key1 val1 / SET key1 val1
LOWER_CMD = b"*3\r\n$3\r\nset\r\n$4\r\nkey1\r\n$4\r\nval1\r\n"
UPPER_CMD = b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n"
EXPECTED = b"+OK\r\n"


@pytest.fixture
def raw_socket(pytestconfig):
    host = pytestconfig.getoption("--redis-host")
    port = pytestconfig.getoption("--redis-port")
    s = socket.create_connection((host, port), timeout=5)
    s.settimeout(15)
    yield s
    s.close()


def _recv_exact(sock, n):
    """读取恰好 n 字节，连接关闭时提前返回."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            break
        data += chunk
    return data


def test_set_case_insensitive(raw_socket):
    """先发小写 set，等回复；再发大写 SET，等回复."""
    raw_socket.sendall(LOWER_CMD)
    resp = _recv_exact(raw_socket, len(EXPECTED))
    assert resp == EXPECTED, f"unexpected reply: {resp!r}"

    raw_socket.sendall(UPPER_CMD)
    resp = _recv_exact(raw_socket, len(EXPECTED))
    assert resp == EXPECTED, f"unexpected reply: {resp!r}"
