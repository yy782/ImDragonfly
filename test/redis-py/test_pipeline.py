"""管道 (pipeline) 集成测试：

在同一个 socket 连接上，一次性发送多条命令（SET/GET/MSET/MGET），
中间不等待任何回复，验证服务端能在一次/多次 recv 中批量解析多条
RESP 命令，并按顺序返回全部回复。

运行（需先启动 ImDragonfly 服务，占用 6379 端口）:
    ./build/imdragonfly 4
    python3 -m pytest test/redis-py/test_pipeline.py -v \\
        --redis-host=127.0.0.1 --redis-port=6379
"""

import socket

import pytest


# 一次性写入的全部命令（RESP 编码）
#   SET pip_key1 val1
#   GET pip_key1
#   MSET pip_key2 val2 pip_key3 val3
#   MGET pip_key1 pip_key2 pip_key3
CMDS = (
    b"*3\r\n$3\r\nSET\r\n$8\r\npip_key1\r\n$4\r\nval1\r\n"
    b"*2\r\n$3\r\nGET\r\n$8\r\npip_key1\r\n"
    b"*5\r\n$4\r\nMSET\r\n$8\r\npip_key2\r\n$4\r\nval2\r\n$8\r\npip_key3\r\n$4\r\nval3\r\n"
    b"*4\r\n$4\r\nMGET\r\n$8\r\npip_key1\r\n$8\r\npip_key2\r\n$8\r\npip_key3\r\n"
)

# 期望的 RESP 回复（与上面命令一一对应）
EXPECTED = (
    b"+OK\r\n"                          # SET
    b"$4\r\nval1\r\n"                    # GET
    b"+OK\r\n"                          # MSET
    b"*3\r\n$4\r\nval1\r\n$4\r\nval2\r\n$4\r\nval3\r\n"  # MGET
)


class RespReader:
    """基于 socket 的 RESP 回复流式解析器，逐条读取完整回复."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def _read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError("connection closed before full reply")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError("connection closed before full reply")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def read(self):
        """读取一个完整的 RESP 值，返回其原始字节（含前缀与换行）."""
        prefix = self._read_exact(1)
        if prefix == b"+":  # 简单字符串
            return b"+" + self._read_line() + b"\r\n"
        if prefix == b"-":  # 错误
            return b"-" + self._read_line() + b"\r\n"
        if prefix == b":":  # 整数
            return b":" + self._read_line() + b"\r\n"
        if prefix == b"$":  # 批量字符串
            length = int(self._read_line())
            if length == -1:
                return b"$-1\r\n"
            data = self._read_exact(length)
            self._read_exact(2)  # 尾部 \r\n
            return b"$" + str(length).encode() + b"\r\n" + data + b"\r\n"
        if prefix == b"*":  # 数组
            count = int(self._read_line())
            if count == -1:
                return b"*-1\r\n"
            parts = [b"*" + str(count).encode() + b"\r\n"]
            for _ in range(count):
                parts.append(self.read())
            return b"".join(parts)
        raise AssertionError(f"unknown RESP prefix: {prefix!r}")


@pytest.fixture
def raw_socket(pytestconfig):
    host = pytestconfig.getoption("--redis-host")
    port = pytestconfig.getoption("--redis-port")
    s = socket.create_connection((host, port), timeout=5)
    s.settimeout(15)
    yield s
    s.close()


def test_pipeline_set_get_mset_mget(raw_socket):
    """一次性发送 SET/GET/MSET/MGET，按顺序读取全部回复并比对."""
    raw_socket.sendall(CMDS)
    reader = RespReader(raw_socket)
    replies = b"".join(reader.read() for _ in range(4))
    assert replies == EXPECTED, f"unexpected reply: {replies!r}"
