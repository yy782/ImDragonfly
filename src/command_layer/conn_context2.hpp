// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

namespace dfly {

class Connection;

struct ConnextionContext{

Connection* owner_;

};

class CommandId;

class  CommandContext{
public:
    CommandContext(ConnextionContext* conn_cntx, CommandId* cid) : 
    conn_cntx_(conn_cntx), 
    cid_(cid) 
    {

    }

    ConnextionContext* conn_cntx() const { return conn_cntx_; }

    const CommandId* cid() const {
      return cid_;
    }


private:
    ConnextionContext* conn_cntx_;
    const CommandId* cid_ = nullptr;
};



}  // namespace dfly

