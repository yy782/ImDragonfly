// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include <string_view>

namespace facade {

// class Connection;

// class ConnectionContext {
// public:
//     explicit ConnectionContext(Connection* owner) :
//     conn_closing_(false),
//     async_dispatch_(false),
//     sync_dispatch_(false),
//     blocked_(false),     
//     owner_(owner) {

//     }

//     virtual ~ConnectionContext() {
//     }

//     Connection* conn() {
//         return owner_;
//     }

//     const Connection* conn() const {
//         return owner_;
//     }
// protected:

//     bool conn_closing_ : 1;
//     bool async_dispatch_ : 1;  // whether this connection is amid an async dispatch // 是否正在异步分发命令
//     bool sync_dispatch_ : 1;   // whether this connection is amid a sync dispatch // 是否正在同步分发命令
//     bool blocked_ = false; // 是否被阻塞（如 BLPOP 等待中）
// private:
//     Connection* owner_; // 指向底层 Connection 对象的指针
// };

}  // namespace facade
