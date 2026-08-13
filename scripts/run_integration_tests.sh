#!/bin/bash
# 启动 ImDragonfly 服务并运行 Redis 协议集成测试
# 用法: bash scripts/run_integration_tests.sh [二进制路径] [端口]
#
# 示例:
#   bash scripts/run_integration_tests.sh
#   bash scripts/run_integration_tests.sh ./build/imdragonfly 6379

set -euo pipefail

IMDRAGONFLY_BIN="${1:-./build/imdragonfly}"
PORT="${2:-6379}"
TEST_DIR="${3:-test/redis-py}"
LOG_DIR="./logs"

# ── 检查端口占用 ────────────────────────────────────────
if ss -tlnp | grep -q ":${PORT} "; then
    echo "ERROR: Port $PORT is already in use."
    echo "Please free port $PORT before running tests."
    exit 1
fi

# ── 自动检测 CPU 核心数 ────────────────────────────────────
THREADS=$(nproc)
echo "Detected CPU cores: $THREADS"

# ── 检查二进制是否存在 ────────────────────────────────────
if [ ! -x "$IMDRAGONFLY_BIN" ]; then
    echo "ERROR: ImDragonfly binary not found or not executable: $IMDRAGONFLY_BIN"
    exit 1
fi

# ── 启动 ImDragonfly ──────────────────────────────────────
mkdir -p "$LOG_DIR"
echo "Starting ImDragonfly ($IMDRAGONFLY_BIN $THREADS)..."
"$IMDRAGONFLY_BIN" "$THREADS" &
IMDRAGONFLY_PID=$!

cleanup() {
    echo "Stopping ImDragonfly (pid=$IMDRAGONFLY_PID)..."
    kill "$IMDRAGONFLY_PID" 2>/dev/null || true
    wait "$IMDRAGONFLY_PID" 2>/dev/null || true
    echo "ImDragonfly stopped."
}

trap cleanup EXIT

# ── 等待服务就绪 ─────────────────────────────────────────
echo "Waiting for ImDragonfly to be ready on port $PORT..."
READY=0
for i in $(seq 1 30); do
    if python3 -c "
import socket
s = socket.socket()
s.settimeout(1)
try:
    s.connect(('127.0.0.1', $PORT))
    s.close()
    exit(0)
except Exception:
    exit(1)
" 2>/dev/null; then
        echo "ImDragonfly is ready (pid=$IMDRAGONFLY_PID, port=$PORT)"
        READY=1
        break
    fi
    echo "  waiting... ($i/30)"
    sleep 1
done

if [ "$READY" -ne 1 ]; then
    echo "ERROR: ImDragonfly failed to start within 30 seconds"
    exit 1
fi

# ── 创建虚拟环境并安装依赖 ───────────────────────────────
VENV_DIR="/tmp/imdragonfly_integration_venv"
echo "Setting up Python virtual environment..."
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install -q redis pytest pytest-timeout

# ── 运行集成测试 ─────────────────────────────────────────
echo ""
echo "=== Running integration tests ==="
"$VENV_DIR/bin/python" -m pytest \
    "$TEST_DIR/test_all.py" \
    "$TEST_DIR/test_half_packet.py" \
    -v \
    --redis-host=127.0.0.1 \
    --redis-port="$PORT" \
    --tb=short \
    --timeout=120

TEST_EXIT_CODE=$?

# ── 清理 ─────────────────────────────────────────────────
rm -rf "$VENV_DIR"

if [ "$TEST_EXIT_CODE" -eq 0 ]; then
    echo ""
    echo "=== All integration tests passed ==="
else
    echo ""
    echo "=== Integration tests FAILED (exit code: $TEST_EXIT_CODE) ==="
fi

exit $TEST_EXIT_CODE
