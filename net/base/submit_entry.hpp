// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <cstdint>
#include <fcntl.h>
#include <liburing/io_uring.h>


// struct io_uring_sqe {
//     __u8    opcode;         /* 操作类型（IORING_OP_READ/WRITE/NOP等） */
//     __u8    flags;          /* 提交标志（IOSQE_*） */
//     __u16   ioprio;         /* I/O 优先级 */
//     __s32   fd;             /* 目标文件描述符 */
//     union {
//         __u64   off;        /* 文件偏移量 */
//         __u64   addr2;      /* 第二个地址字段（某些操作复用） */
//     };
//     union {
//         __u64   addr;       /* 缓冲区地址/iovec数组地址/路径名地址 */
//         __u64   splice_off_in;
//     };
//     __u32   len;            /* 缓冲区大小 或 iovec 数量 */
//     union {
//         __kernel_rwf_t  rw_flags;      /* 读写标志 */
//         __u32           fsync_flags;   /* fsync 标志 */
//         __u16           poll_events;   /* poll 事件（兼容旧版） */
//         __u32           poll32_events; /* poll 事件（32位，大端需转换） */
//         __u32           sync_range_flags;
//         __u32           msg_flags;     /* send/recv 消息标志 */
//         __u32           timeout_flags; /* 超时标志 */
//         __u32           accept_flags;  /* accept4 标志 */
//         __u32           cancel_flags;  /* 取消操作标志 */
//         __u32           open_flags;    /* open/openat 标志 */
//         __u32           statx_flags;
//         __u32           fadvise_advice;/* fadvise/madvise 建议值 */
//         __u32           splice_flags;
//     };
//     __u64   user_data;      /* 用户数据，完成时原样返回 */
//     union {
//         struct {
//             union {
//                 __u16   buf_index;     /* 固定缓冲区索引 */
//                 __u16   buf_group;     /* 缓冲区组 ID（自动选择） */
//             };
//             __u16   personality;       /* 个性/凭证 ID */
//             __s32   splice_fd_in;      /* splice 输入 fd */
//         };
//         __u64   __pad2[3];   /* 填充到 64 字节 */
//     };
// };

// struct io_uring_cqe {
//     __u64   user_data;  /* 原样返回 SQE 中的 user_data */
//     __s32   res;        /* 操作结果（成功返回正数，失败返回 -errno） */
//     __u32   flags;      /* 完成标志 */
// };

namespace util {
namespace fb2 {
class UringProactor;

class SubmitEntry {
  io_uring_sqe* sqe_;

public:
  SubmitEntry() : sqe_(nullptr) {
  }

  explicit SubmitEntry(io_uring_sqe* sqe) : sqe_(sqe) {
  }


  void PrepOpenAt(int dfd /*目录文件描述符 */, const char* path /*文件路径*/, int flags/*打开标志*/, mode_t mode/*创建文件时的权限*/) {
    PrepFd(IORING_OP_OPENAT /*打开文件*/, dfd);
    sqe_->addr = reinterpret_cast<__u64>(path);
    sqe_->open_flags = flags;
    sqe_->len = mode;
  }

  // mask is a bit-OR of POLLXXX flags.
  void PrepPollAdd(int fd, unsigned mask) {
    PrepFd(IORING_OP_POLL_ADD, fd);
    sqe_->poll32_events = mask;
  }

  void PrepPollRemove(uint64_t uid) {
    PrepFd(IORING_OP_POLL_REMOVE, -1);
    sqe_->addr = uid;
  }

  void PrepAccept(int listen_fd, struct sockaddr* addr, unsigned addrlen, unsigned flags) {
    PrepFd(IORING_OP_ACCEPT, listen_fd);
    sqe_->addr = (__u64)addr;
    sqe_->len = addrlen;
    sqe_->accept_flags = flags;
  }

  void PrepRecv(int fd, void* buf, size_t len, unsigned flags) {
    PrepFd(IORING_OP_RECV, fd);
    sqe_->addr = (__u64)buf;
    sqe_->len = len;
    sqe_->msg_flags = flags;
  }

  void PrepRecvMsg(int fd, const struct msghdr* msg, unsigned flags) {
    PrepFd(IORING_OP_RECVMSG, fd);
    sqe_->addr = (__u64)msg;
    sqe_->len = 1;
    sqe_->msg_flags = flags;
  }

  void PrepRead(int fd, void* buf, unsigned size, size_t offset) {
    PrepFd(IORING_OP_READ, fd);
    sqe_->addr = (__u64)buf;
    sqe_->len = size;
    sqe_->off = offset;
  }

  void PrepReadFixed(int fd, void* buf, unsigned size, size_t offset, uint16_t buf_index) {
    PrepFd(IORING_OP_READ_FIXED, fd);
    sqe_->addr = (__u64)buf;
    sqe_->len = size;
    sqe_->off = offset;
    sqe_->buf_index = buf_index;
  }

  void PrepReadV(int fd, const struct iovec* vec, unsigned nr_vecs, size_t offset,
                 unsigned flags = 0) {
    PrepFd(IORING_OP_READV, fd);
    sqe_->addr = (__u64)vec;
    sqe_->len = nr_vecs;
    sqe_->off = offset;
    sqe_->rw_flags = flags;
  }

  void PrepWrite(int fd, const void* buf, unsigned size, size_t offset) {
    PrepFd(IORING_OP_WRITE, fd);
    sqe_->addr = (__u64)buf;
    sqe_->len = size;
    sqe_->off = offset;
  }

  void PrepWriteFixed(int fd, const void* buf, unsigned size, size_t offset, uint16_t buf_index) {
    PrepFd(IORING_OP_WRITE_FIXED, fd);
    sqe_->addr = (__u64)buf;
    sqe_->len = size;
    sqe_->off = offset;
    sqe_->buf_index = buf_index;
  }

  void PrepWriteV(int fd, const struct iovec* vec, unsigned nr_vecs, size_t offset,
                  unsigned flags = 0) {
    PrepFd(IORING_OP_WRITEV, fd);
    sqe_->addr = (__u64)vec;
    sqe_->len = nr_vecs;
    sqe_->off = offset;
    sqe_->rw_flags = flags;
  }

  void PrepFallocate(int fd, int mode, off_t offset, off_t len) {
    PrepFd(IORING_OP_FALLOCATE, fd);
    sqe_->off = offset;
    sqe_->len = mode;
    sqe_->addr = uint64_t(len);
  }

  void PrepFadvise(int fd, off_t offset, off_t len, int advice) {
    PrepFd(IORING_OP_FADVISE, fd);
    sqe_->fadvise_advice = advice;
    sqe_->len = len;
    sqe_->off = offset;
  }

  void PrepFSync(int fd, unsigned flags) {
    PrepFd(IORING_OP_FSYNC, fd);
    sqe_->fsync_flags = flags;
  }

  void PrepStatX(const char* filepath, struct statx *stat) {
    // AT_FDCWD is ignored when addr is an absolute path
	  PrepFd(IORING_OP_STATX, AT_FDCWD);
    sqe_->off = reinterpret_cast<uint64_t>(stat);
    sqe_->addr = reinterpret_cast<unsigned long>(filepath);
    // mask
    sqe_->len = STATX_BASIC_STATS;
    sqe_->statx_flags = 0;
  }


  void PrepSend(int fd, const void* buf, size_t len, unsigned flags) {
    PrepFd(IORING_OP_SEND, fd);
    sqe_->addr = (__u64)buf;
    sqe_->len = len;
    sqe_->msg_flags = flags;
  }

  void PrepSendMsg(int fd, const struct msghdr* msg, unsigned flags) {
    PrepFd(IORING_OP_SENDMSG, fd);
    sqe_->addr = (__u64)msg;
    sqe_->len = 1;
    sqe_->msg_flags = flags;
  }

  void PrepConnect(int fd, const struct sockaddr* addr, socklen_t addrlen) {
    PrepFd(IORING_OP_CONNECT, fd);
    sqe_->addr = (__u64)addr;
    sqe_->len = 0;
    sqe_->off = addrlen;
  }

  void PrepClose(int fd) {
    PrepFd(IORING_OP_CLOSE, fd);
  }

  void PrepTimeout(const timespec* ts, bool is_abs = true) {
    PrepFd(IORING_OP_TIMEOUT, -1);
    sqe_->addr = (__u64)ts;
    sqe_->len = 1;
    sqe_->timeout_flags = (is_abs ? IORING_TIMEOUT_ABS : 0);
  }

  void PrepTimeoutRemove(unsigned long long userdata) {
    PrepFd(IORING_OP_TIMEOUT_REMOVE, -1);
    sqe_->addr = userdata;
    sqe_->timeout_flags = 0;
  }

  // Sets up link timeout with relative timespec.
  void PrepLinkTimeout(const timespec* ts) {
    PrepFd(IORING_OP_LINK_TIMEOUT, -1);
    sqe_->addr = (__u64)ts;
    sqe_->len = 1;
    sqe_->timeout_flags = 0;
  }

  // how is either: SHUT_RD, SHUT_WR or SHUT_RDWR.
  void PrepShutdown(int fd, int how) {
    PrepFd(IORING_OP_SHUTDOWN, fd);
    sqe_->len = how;
  }

  void PrepMsgRing(int fd, unsigned int len, uint64_t data) {
    PrepFd(IORING_OP_MSG_RING, fd);
    sqe_->len = len;
    sqe_->off = data;
    sqe_->rw_flags = 0;
  }

  void PrepCancelFd(int fd, unsigned flags) {
    PrepFd(IORING_OP_ASYNC_CANCEL, fd);
    sqe_->cancel_flags = flags | IORING_ASYNC_CANCEL_FD;
  }

  void PrepMadvise(void* addr, off_t len, int advice) {
    PrepFd(IORING_OP_MADVISE, -1);
    sqe_->addr = (__u64)addr;
    sqe_->len = len;
    sqe_->fadvise_advice = advice;
  }

  void PrepNOP() {
    PrepFd(IORING_OP_NOP, -1);
  }

  io_uring_sqe* sqe() {
    return sqe_;
  }



 private:
  void PrepFd(int op, int fd) {
    sqe_->opcode = op;
    sqe_->fd = fd;
  }

};

}  // namespace base
