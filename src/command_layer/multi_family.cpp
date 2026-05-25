#include "multi_family.hpp"

#include "command_layer/command_registry.hpp"
#include "command_layer/conn_context.hpp"
#include "transaction_layer/transaction.hpp"
#include "network/redis_server.hpp"

namespace dfly {

using facade::kInvalidKeysStart;
using facade::kInvalidKeysNum;
using facade::kInvalidKeysOffset;

void MultiFamily::Multi(CommandContext* cmd_cntx, CmdArgList args) {
    (void)args;
    auto conn = cmd_cntx->conn_cntx()->owner_;
    auto tx = cmd_cntx->tx();
    
    if (tx->GetState() == Transaction::State::MULTI) {
        conn->SendERROR("MULTI calls can not be nested");
        return;
    }
    
    tx->startMulti();
    conn->SendStatus("OK");
}

void MultiFamily::Exec(CommandContext* cmd_cntx, CmdArgList args) {
    (void)args;
    auto conn = cmd_cntx->conn_cntx()->owner_;
    auto tx = cmd_cntx->tx();
    
    if (tx->GetState() != Transaction::State::MULTI) {
        conn->SendERROR("EXEC without MULTI");
        return;
    }
    
    if (tx->IsDirty()) {
        conn->SendERROR("EXECABORT Transaction discarded because of previous errors.");
        tx->SetState(Transaction::State::DISCARDED);
        tx->ClearQueuedCommands();
        tx->ClearDirtyKeys();
        return;
    }
    
    auto& queued = tx->GetQueuedCommands();
    if (queued.empty()) {
        conn->SendVec({});
        tx->SetState(Transaction::State::IDLE);
        return;
    }
    
    tx->SetState(Transaction::State::EXEC);
    
    DbContext db = tx->GetDbContext();

    for (const auto& cmd : queued) {
        Transaction*& t = tx->CreateSubTransaction(cmd.cid);
        t->setConnectionContext(tx->GetConnectionContext());
        t->InitByArgs(db.ns_, db.db_index_, cmd.GetCmdArgList());
        cmd.cid->Invoke(&t->GetCommandContext(), cmd.GetCmdArgList());
    }
}

void MultiFamily::Discard(CommandContext* cmd_cntx, CmdArgList args) {
    (void)args;
    auto conn = cmd_cntx->conn_cntx()->owner_;
    auto tx = cmd_cntx->tx();
    
    if (tx->GetState() != Transaction::State::MULTI) {
        conn->SendERROR("DISCARD without MULTI");
        return;
    }
    
    tx->FinishOrDiscardMulti();
    conn->SendStatus("OK");
}

void MultiFamily::Watch(CommandContext* cmd_cntx, CmdArgList args) {
    auto conn = cmd_cntx->conn_cntx()->owner_;
    auto tx = cmd_cntx->tx();
    
    if (tx->GetState() == Transaction::State::MULTI) {
        conn->SendERROR("WATCH inside MULTI is not allowed");
        return;
    }
    
    for (size_t i = 1; i < args.size(); ++i) {
        tx->AddWatchKey(args[i]);
    }
    
    tx->SetState(Transaction::State::WATCH);
    conn->SendStatus("OK");
}

void MultiFamily::Unwatch(CommandContext* cmd_cntx, CmdArgList args) {
    (void)args;
    auto conn = cmd_cntx->conn_cntx()->owner_;
    auto tx = cmd_cntx->tx();
    
    tx->ClearWatchKeys();
    tx->SetState(Transaction::State::IDLE);
    conn->SendStatus("OK");
}

using CI = CommandId;
void MultiFamily::Register(CommandRegistry* registry) {
    registry->StartFamily();
    *registry
        << CI{"MULTI", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Multi)
        << CI{"EXEC", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Exec)
        << CI{"DISCARD", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Discard)
        << CI{"WATCH", 1, kInvalidKeysNum, 1}.SetHandler(&MultiFamily::Watch)
        << CI{"UNWATCH", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Unwatch)
        ;
}

void RegisterMulti(CommandRegistry* registry) {
    MultiFamily::Register(registry);
}

}  // namespace dfly