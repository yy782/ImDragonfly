# =============================================================================
# ImDragonfly - 多阶段构建 Dockerfile
# =============================================================================

# ── Stage 1: 构建阶段 ─────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# 安装构建依赖（与 CI 保持一致）
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    cmake \
    make \
    ninja-build \
    liburing-dev \
    libmimalloc-dev \
    libboost-dev \
    libgoogle-glog-dev \
    libabsl-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libgtest-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# 先复制 YY 子模块和 cmake 配置（依赖层，利用 Docker 缓存）
COPY YY/ ./YY/
COPY cmake/ ./cmake/
COPY CMakeLists.txt ./

# 复制源代码
COPY src/ ./src/
COPY main.cpp ./

# CMake 配置（Release 模式，不编译测试）
RUN cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTS=OFF \
    -S . -B build \
    && cmake --build build --target imdragonfly -j$(nproc)

# ── Stage 2: 运行阶段 ─────────────────────────────────────────────────────
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# 只安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    liburing2 \
    libmimalloc2.0 \
    libgoogle-glog0v6t64 \
    libprotobuf32t64 \
    libabsl-dev \
    && rm -rf /var/lib/apt/lists/*

# 创建非 root 用户
RUN useradd --create-home --shell /bin/bash imdragonfly

# 工作目录
WORKDIR /app

# 从构建阶段复制二进制
COPY --from=builder /build/build/imdragonfly /app/imdragonfly

# 确保 logs 目录存在
RUN mkdir -p /app/logs && chown -R imdragonfly:imdragonfly /app

USER imdragonfly

# 暴露 Redis 默认端口
EXPOSE 6379

# 启动服务，默认 4 个分片，日志输出到 stderr
ENTRYPOINT ["./imdragonfly"]
CMD ["4"]
ENV GLOG_logtostderr=1
