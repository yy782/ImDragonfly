// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "detail/common.hpp"

namespace dfly {

class Connection;
class Transaction;


struct ConnectionContext{    
RedisSessionPtr owner_;
};

class CommandId;

class  CommandContext{
public:
    CommandContext() = default;

    CommandContext(ConnectionContext* conn_cntx, Transaction* transaction, CommandId* cid) : 
    conn_cntx_(conn_cntx), 
    transaction_(transaction),
    cid_(cid) 
    {

    }

    ConnectionContext* conn_cntx() const { return conn_cntx_; }

    const CommandId* cid() const {
      return cid_;
    }
    Transaction* tx() const { 
        return transaction_;
    }
private:
    ConnectionContext* conn_cntx_;
    Transaction* transaction_;
    const CommandId* cid_ = nullptr;
};



}  // namespace dfly

