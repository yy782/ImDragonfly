

#include "socket.hpp"
#include "uring_proactor.hpp"
#include <coroutine>



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

namespace base{


UringSocket::UringSocket(UringProactorPtr proactor, int fd)
    : proactor_(proactor), fd_(fd) {}

UringSocket::~UringSocket() {
    if (fd_ != -1) {
        close(fd_);
    }
}

auto UringSocket::AsyncAccept() -> AcceptAwaitable {
    return AcceptAwaitable{this, -1, {}, 0};
}

auto UringSocket::AsyncRead(char* buf, size_t len, off_t offset) -> ReadAwaitable {
    return ReadAwaitable{this, buf, len, offset, 0};
}

auto UringSocket::AsyncWrite(const char* buf, size_t len, off_t offset) -> WriteAwaitable {
    return WriteAwaitable{this, buf, len, offset, 0};
}

// ============== Awaitable 方法实现 ==============

void UringSocket::AcceptAwaitable::await_suspend(std::coroutine_handle<> h) noexcept {
    addrlen_ = sizeof(addr_);
    socket_->Proactor()->SubmitAccept(socket_->fd(), h, this);
}

void UringSocket::ReadAwaitable::await_suspend(std::coroutine_handle<> h) noexcept {
    socket_->Proactor()->SubmitRead(socket_->fd(), buf_, len_, offset_, h, this);
}

void UringSocket::WriteAwaitable::await_suspend(std::coroutine_handle<> h) noexcept {
    socket_->Proactor()->SubmitWrite(socket_->fd(), buf_, len_, offset_, h, this);
}






}