#!/bin/bash
# 启动 ImDragonfly 并运行 memtier_benchmark 压测
# 用法: bash scripts/run_benchmark.sh [二进制路径] [端口]
#
# 示例:
#   bash scripts/run_benchmark.sh
#   bash scripts/run_benchmark.sh ./build/imdragonfly 6379

set -euo pipefail

IMDRAGONFLY_BIN="${1:-./build/imdragonfly}"
PORT="${2:-6379}"
BENCH_DURATION="${BENCH_DURATION:-60}"

# ── 自动检测 CPU 核心数 ────────────────────────────────────
THREADS=$(nproc)
echo "Detected CPU cores: $THREADS"
RESULT_DIR="./benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MEMTIER_PID=""

# ═══════════════════════════════════════════════════════════════
# 测试用例定义 — 新增测试在这里加一个函数即可
# ═══════════════════════════════════════════════════════════════

run_bench_set_get() {
    local NAME="SET_GET"
    local RESULT_FILE="$RESULT_DIR/${NAME}_${TIMESTAMP}.log"

    echo ""
    echo "================================================"
    echo "  Test: $NAME"
    echo "  Description: 50% SET + 50% GET, 4 threads, 100 conns, 100B payload, ${BENCH_DURATION}s"
    echo "  Result: $RESULT_FILE"
    echo "================================================"

    # 后台启动 memtier_benchmark，test-time 设长一些，由脚本控制停止时间
    memtier_benchmark -s 127.0.0.1 -p "$PORT" \
        --command="SET __key__ __data__" --command-key-pattern=R --command-ratio=1 \
        --command="GET __key__" --command-key-pattern=R --command-ratio=1 \
        -t 4 -c 100 \
        -d 100 \
        --test-time=30 \
        --hide-histogram \
        > "$RESULT_FILE" 2>&1 &
    MEMTIER_PID=$!
    echo "memtier_benchmark started (pid=$MEMTIER_PID), waiting ${BENCH_DURATION}s..."

    sleep "$BENCH_DURATION"

    # 检查 ImDragonfly 是否还活着
    echo ""
    echo "=== Checking ImDragonfly process ==="
    if kill -0 "$IMDRAGONFLY_PID" 2>/dev/null; then
        echo "ImDragonfly is still running (pid=$IMDRAGONFLY_PID)."
    else
        echo "ERROR: ImDragonfly process (pid=$IMDRAGONFLY_PID) is DEAD!"
        echo "This likely indicates a crash during the stress test."
        kill "$MEMTIER_PID" 2>/dev/null || true
        wait "$MEMTIER_PID" 2>/dev/null || true
        MEMTIER_PID=""
        return 1
    fi

    # 如果 memtier_benchmark 还在跑，Ctrl+C 中断 开24核不能正常关闭连接，但是4核却可以，有问题，留意一下
    if kill -0 "$MEMTIER_PID" 2>/dev/null; then
        echo "memtier_benchmark (pid=$MEMTIER_PID) still running, interrupting..."
        kill -INT "$MEMTIER_PID" 2>/dev/null || true
        wait "$MEMTIER_PID" 2>/dev/null || true
        echo "memtier_benchmark stopped."
    else
        echo "memtier_benchmark finished on its own."
    fi
    MEMTIER_PID=""

    return 0
}

# TODO: 将来在这里添加更多测试用例
# run_bench_get_only() { ... }
# run_bench_set_only() { ... }

# ── 选择要运行的测试 ───────────────────────────────────────
# 可通过环境变量 BENCH_TESTS 指定，默认跑全部
DEFAULT_TESTS="set_get"
TESTS="${BENCH_TESTS:-$DEFAULT_TESTS}"

# ── 检查依赖 ───────────────────────────────────────────────
if ! command -v memtier_benchmark &>/dev/null; then
    echo "ERROR: memtier_benchmark not found. Install it with:"
    echo "  wget https://github.com/redis/memtier_benchmark/releases/download/2.5.0/memtier-benchmark_2.5.0.jammy_amd64.deb"
    echo "  sudo dpkg -i memtier-benchmark_2.5.0.jammy_amd64.deb"
    exit 1
fi

if [ ! -x "$IMDRAGONFLY_BIN" ]; then
    echo "ERROR: ImDragonfly binary not found or not executable: $IMDRAGONFLY_BIN"
    exit 1
fi

# ── 启动 ImDragonfly ──────────────────────────────────────
echo "Starting ImDragonfly ($IMDRAGONFLY_BIN $THREADS)..."
"$IMDRAGONFLY_BIN" "$THREADS" &
IMDRAGONFLY_PID=$!

cleanup() {
    echo ""
    if [ -n "$MEMTIER_PID" ] && kill -0 "$MEMTIER_PID" 2>/dev/null; then
        echo "Stopping memtier_benchmark (pid=$MEMTIER_PID)..."
        kill "$MEMTIER_PID" 2>/dev/null || true
        wait "$MEMTIER_PID" 2>/dev/null || true
    fi
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

# ── 运行测试 ──────────────────────────────────────────────
mkdir -p "$RESULT_DIR"
FAILED_TESTS=""

for test in $TESTS; do
    case "$test" in
        set_get)
            set +e
            run_bench_set_get
            BENCH_EXIT=$?
            set -e
            RESULT_FILE="$RESULT_DIR/SET_GET_${TIMESTAMP}.log"
            ;;
        # TODO: 新增的测试在这里加一个 case 分支
        # get_only)
        #     set +e
        #     run_bench_get_only
        #     BENCH_EXIT=$?
        #     set -e
        #     RESULT_FILE="$RESULT_DIR/GET_ONLY_${TIMESTAMP}.log"
        #     ;;
        *)
            echo "ERROR: Unknown test: $test"
            echo "Available: $DEFAULT_TESTS"
            exit 1
            ;;
    esac

    if [ "$BENCH_EXIT" -eq 0 ]; then
        echo ""
        echo "=== Test [$test] PASSED, result saved to $RESULT_FILE ==="
    else
        echo ""
        echo "=== Test [$test] FAILED (exit code: $BENCH_EXIT) ==="
        FAILED_TESTS="$FAILED_TESTS $test"
    fi
done

if [ -n "$FAILED_TESTS" ]; then
    echo ""
    echo "=== Some tests FAILED:$FAILED_TESTS ==="
    exit 1
fi

echo ""
echo "=== All benchmark tests passed ==="
