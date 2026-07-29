#!/bin/bash
# 构建编译 ImDragonfly 及 C++ 单元测试
# 用法: bash scripts/build.sh [选项]
#
# 选项:
#   -d, --debug          Debug 构建 (默认)
#   -r, --release        Release 构建
#   -a, --asan           启用 AddressSanitizer
#   -b, --build-dir DIR  指定构建目录 (默认: build)
#   -j, --jobs N         并行编译线程数 (默认: nproc)
#   -t, --targets LIST   指定构建目标 (默认: imdragonfly unit_tests)
#   -h, --help           显示帮助
#
# 示例:
#   bash scripts/build.sh                          # Debug 构建所有目标
#   bash scripts/build.sh -r                       # Release 构建
#   bash scripts/build.sh -d --asan                # Debug + ASan
#            # 只构建 ImDragonfly
#   bash scripts/build.sh -b build-release -j 8    # 自定义目录和并行数
# bash scripts/build.sh -t "unit_tests"
set -euo pipefail

# ── 默认值 ─────────────────────────────────────────────────
BUILD_TYPE="Debug"
USE_ASAN="OFF"
BUILD_DIR="build"
JOBS=$(nproc)
TARGETS="imdragonfly unit_tests"

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
echo "======================================"

# ── CMake 配置 ─────────────────────────────────────────────
echo ""
echo "=== Configuring CMake ==="
cmake \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DBUILD_TESTS=ON \
    -DUSE_ASAN="$USE_ASAN" \
    -S . -B "$BUILD_DIR"

# ── 编译 ──────────────────────────────────────────────────
echo ""
echo "=== Building ==="
TARGET_ARGS=()
for t in $TARGETS; do
    TARGET_ARGS+=(--target "$t")
done
cmake --build "$BUILD_DIR" "${TARGET_ARGS[@]}" -j"$JOBS"

echo ""
echo "=== Build complete ==="
