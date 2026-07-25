#!/bin/bash
# 运行 C++ 单元测试 (GTest)
# 用法: bash scripts/run_unit_tests.sh [测试二进制路径]
#
# 示例:
#   bash scripts/run_unit_tests.sh                         # 默认路径
#   bash scripts/run_unit_tests.sh ./build/test/db_table_test

set -euo pipefail

TEST_EXE="${1:-./build/test/db_table_test}"

if [ ! -x "$TEST_EXE" ]; then
    echo "ERROR: Test executable not found or not executable: $TEST_EXE"
    echo "Run 'bash scripts/build.sh' first to build tests."
    exit 1
fi

echo "=== Running C++ unit tests: $TEST_EXE ==="
echo ""

"$TEST_EXE"
TEST_EXIT=$?

if [ "$TEST_EXIT" -eq 0 ]; then
    echo ""
    echo "=== All tests passed ==="
else
    echo ""
    echo "=== Tests FAILED (exit code: $TEST_EXIT) ==="
    exit $TEST_EXIT
fi
