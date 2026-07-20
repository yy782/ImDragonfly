"""pytest 共享配置：Redis 连接 fixture 和测试辅助工具."""

import pytest
import redis


def pytest_addoption(parser):
    """添加自定义命令行选项."""
    parser.addoption(
        "--redis-host",
        action="store",
        default="localhost",
        help="Redis 服务器地址 (默认: localhost)",
    )
    parser.addoption(
        "--redis-port",
        action="store",
        type=int,
        default=6379,
        help="Redis 服务器端口 (默认: 6379)",
    )
    parser.addoption(
        "--redis-db",
        action="store",
        type=int,
        default=0,
        help="Redis 数据库编号 (默认: 0)",
    )
    parser.addoption(
        "--benchmark",
        action="store_true",
        default=False,
        help="运行 benchmark 测试",
    )


def pytest_configure(config):
    """动态注册 benchmark marker，并在 --benchmark 时自动选中."""
    config.addinivalue_line(
        "markers", "benchmark: 性能压测 (默认跳过，使用 --benchmark 开启)"
    )
    if not config.getoption("--benchmark"):
        # 默认排除 benchmark 测试
        mark_expr = config.getoption("markexpr", "")
        if "benchmark" not in mark_expr:
            config.option.markexpr = (
                f"not benchmark and ({mark_expr})" if mark_expr else "not benchmark"
            )


@pytest.fixture(scope="session")
def redis_client(request):
    """session 级别 Redis 连接，整个测试会话复用同一个连接."""
    host = request.config.getoption("--redis-host")
    port = request.config.getoption("--redis-port")
    db = request.config.getoption("--redis-db")

    client = redis.Redis(
        host=host,
        port=port,
        protocol=2,
        db=db,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.exit(f"无法连接到 Redis ({host}:{port})，请确认服务器正在运行")

    yield client
    client.close()


@pytest.fixture
def clean_redis(redis_client):
    """每个测试函数独立使用的 fixture，自动记录并清理测试 key."""
    keys = []

    def _track(key: str):
        """标记一个 key，测试结束后自动删除."""
        keys.append(key)

    yield redis_client, _track

    # 自动清理：批量删除，避免逐个 DELETE 大量 key 时阻塞
    if keys:
        try:
            redis_client.delete(*keys)
        except Exception:
            pass
