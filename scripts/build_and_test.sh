#!/bin/bash
# 构建 C++ 单元测试并运行
# 用法: bash scripts/build_and_test.sh [选项]
#
# 选项:
#   -d, --debug          Debug 构建 (默认)
#   -r, --release        Release 构建
#   -a, --asan           启用 AddressSanitizer
#   -b, --build-dir DIR  指定构建目录 (默认: build)
#   -j, --jobs N         并行编译线程数 (默认: nproc)
#   -t, --targets LIST   指定构建目标 (默认: imdragonfly db_table_test)
#   --run                构建后运行测试
#   --no-build           跳过构建，直接运行已有的测试
#   -h, --help           显示帮助
#
# 示例:
#   bash scripts/build_and_test.sh                        # Debug 构建
#   bash scripts/build_and_test.sh -r                     # Release 构建
#   bash scripts/build_and_test.sh -d --asan --run         # Debug + ASan 构建并运行
#   bash scripts/build_and_test.sh -t "db_table_test" --run  # 只构建并运行测试

set -euo pipefail

# ── 默认值 ─────────────────────────────────────────────────
BUILD_TYPE="Debug"
USE_ASAN="OFF"
BUILD_DIR="build"
JOBS=$(nproc)
TARGETS="imdragonfly db_table_test"
DO_RUN=0
DO_BUILD=1

# ── 解析参数 ───────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        -r|--release)
            BUILD_TYPE="Release"
            shift
            ;;
        -a|--asan)
            USE_ASAN="ON"
            shift
            ;;
        -b|--build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        -j|--jobs)
            JOBS="$2"
            shift 2
            ;;
        -t|--targets)
            TARGETS="$2"
            shift 2
            ;;
        --run)
            DO_RUN=1
            shift
            ;;
        --no-build)
            DO_BUILD=0
            shift
            ;;
        -h|--help)
            head -18 "$0" | tail -15
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h for help"
            exit 1
            ;;
    esac
done

# ── 显示配置 ───────────────────────────────────────────────
echo "======================================"
echo "  Build Type:       $BUILD_TYPE"
echo "  AddressSanitizer: $USE_ASAN"
echo "  Build Directory:  $BUILD_DIR"
echo "  Targets:          $TARGETS"
echo "  Parallel Jobs:    $JOBS"
echo "  Run Tests After:  $([ "$DO_RUN" -eq 1 ] && echo "YES" || echo "NO")"
echo "======================================"

# ── CMake 配置 ─────────────────────────────────────────────
if [ "$DO_BUILD" -eq 1 ]; then
    echo ""
    echo "=== Configuring CMake ==="
    cmake \
        -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
        -DBUILD_TESTS=ON \
        -DUSE_ASAN="$USE_ASAN" \
        -S . -B "$BUILD_DIR"

    echo ""
    echo "=== Building ==="
    # 将空格分隔的 targets 转为 --target 参数
    TARGET_ARGS=()
    for t in $TARGETS; do
        TARGET_ARGS+=(--target "$t")
    done
    cmake --build "$BUILD_DIR" "${TARGET_ARGS[@]}" -j"$JOBS"

    echo ""
    echo "=== Build complete ==="
fi

# ── 运行测试 ───────────────────────────────────────────────
if [ "$DO_RUN" -eq 1 ]; then
    echo ""
    echo "=== Running C++ unit tests ==="

    TEST_EXE="$BUILD_DIR/test/db_table_test"
    if [ ! -x "$TEST_EXE" ]; then
        echo "ERROR: Test executable not found: $TEST_EXE"
        exit 1
    fi

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
fi
