#!/bin/bash
# 检查 C++ 代码格式化是否符合 .clang-format 规范
# 用法: bash scripts/check_format.sh

set -euo pipefail

echo "=== Checking C++ code formatting ==="

# 需要检查的目录和文件
DIRS=("src" "test")
FILES=("main.cpp")

# 收集所有 C++ 源文件
SOURCE_FILES=()

for dir in "${DIRS[@]}"; do
    if [ -d "$dir" ]; then
        while IFS= read -r -d '' f; do
            SOURCE_FILES+=("$f")
        done < <(find "$dir" -type f \( -name '*.cpp' -o -name '*.hpp' -o -name '*.h' \) -print0)
    fi
done

for f in "${FILES[@]}"; do
    if [ -f "$f" ]; then
        SOURCE_FILES+=("$f")
    fi
done

if [ ${#SOURCE_FILES[@]} -eq 0 ]; then
    echo "No C++ source files found to check."
    exit 0
fi

echo "Checking ${#SOURCE_FILES[@]} file(s)..."

# 优先使用 clang-format-18 (与本地开发环境一致)
if command -v clang-format-18 &>/dev/null; then
    CLANG_FORMAT="clang-format-18"
else
    CLANG_FORMAT="clang-format"
fi

# 运行 clang-format 检查
if ! $CLANG_FORMAT --dry-run --Werror --style=file "${SOURCE_FILES[@]}" 2>&1; then
    echo ""
    echo "ERROR: Some files are not properly formatted."
    echo "Run the following command to fix:"
    echo "  $CLANG_FORMAT -i --style=file \$(find src test -type f \\( -name '*.cpp' -o -name '*.hpp' -o -name '*.h' \\)) main.cpp"
    exit 1
fi

echo "All files are properly formatted."
