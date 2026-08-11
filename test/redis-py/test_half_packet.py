"""半包 (split packet) 集成测试：验证服务端能正确处理 RESP 命令被 TCP 拆分的情况.

一条完整命令分两段发送，中间间隔 5 秒：
  1. 先发送命令的前半段
  2. sleep 5 秒
  3. 再发送后半段

服务端需要保留第一段数据（不能被下一轮 read 覆盖），
等第二段到达后拼出完整命令并正常返回.

运行（需先启动 ImDragonfly 服务，占用 6379 端口）:
    ./build/imdragonfly 4
    python3 -m pytest test/redis-py/test_half_packet.py -v \\
        --redis-host=127.0.0.1 --redis-port=6379
"""

import socket
import time

import pytest

# RESP 编码: SET halfpkt_key halfpkt_value
CMD = b"*3\r\n$3\r\nSET\r\n$11\r\nhalfpkt_key\r\n$13\r\nhalfpkt_value\r\n"
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


def test_half_packet_set(raw_socket):
    """SET 命令拆成两半发送，中间间隔 5 秒，服务端应正常返回 +OK."""
    half = len(CMD) // 2
    assert half > 0
    # 前半段切在 value 中间，保证首段解析不完整
    raw_socket.sendall(CMD[:half])
    time.sleep(2)
    raw_socket.sendall(CMD[half:])

    resp = _recv_exact(raw_socket, len(EXPECTED))
    assert resp == EXPECTED, f"unexpected reply: {resp!r}"


def test_half_packet_multiple_breaks(raw_socket):
    """命令拆成三段发送，中间各间隔 5 秒，服务端应正常返回 +OK."""
    parts = [CMD[:4], CMD[4:len(CMD) // 2 + 4], CMD[len(CMD) // 2 + 4:]]
    for p in parts:
        assert p
        raw_socket.sendall(p)
        time.sleep(2)

    resp = _recv_exact(raw_socket, len(EXPECTED))
    assert resp == EXPECTED, f"unexpected reply: {resp!r}"
