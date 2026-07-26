#!/bin/bash
# 运行 C++ 单元测试 (GTest + CTest)
# 用法: bash scripts/run_unit_tests.sh [构建目录]
#
# 示例:
#   bash scripts/run_unit_tests.sh                         # 默认 build 目录
#   bash scripts/run_unit_tests.sh ./build-release         # 指定构建目录

set -euo pipefail

# ── 检查 6379 端口占用 ────────────────────────────────────
if ss -tlnp | grep -q ":6379 "; then
    echo "ERROR: Port 6379 is already in use."
    echo "Please free port 6379 before running tests."
    exit 1
fi

BUILD_DIR="${1:-build}"

if [ ! -f "$BUILD_DIR/CTestTestfile.cmake" ]; then
    echo "ERROR: CTest test file not found in $BUILD_DIR"
    echo "Run 'bash scripts/build.sh' first to build tests."
    exit 1
fi

echo "=== Running C++ unit tests ==="
echo ""

ctest --test-dir "$BUILD_DIR" --output-on-failure
TEST_EXIT=$?

if [ "$TEST_EXIT" -eq 0 ]; then
    echo ""
    echo "=== All tests passed ==="
else
    echo ""
    echo "=== Tests FAILED (exit code: $TEST_EXIT) ==="
    exit $TEST_EXIT
fi
