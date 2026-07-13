// #include "multi_family.hpp"

// #include "command_layer/command_registry.hpp"
// #include "detail/conn_context.hpp"
// #include "transaction_layer/transaction.hpp"
// #include "network/redis_server.hpp"
// #include "cmd_support.hpp"
// #include "util/synchronization.hpp"
// #include "cppcoro/async_task.hpp"
// #include <assert.h>
// namespace dfly {
// using namespace cmd;
// using facade::kInvalidKeysStart;
// using facade::kInvalidKeysNum;
// using facade::kInvalidKeysOffset;

// void MultiFamily::Multi(CommandContext* cmd_cntx, CmdArgList args) {
//     (void)args;
//     auto conn = cmd_cntx->conn_cntx()->owner(); 
//     auto tx = cmd_cntx->tx();
    
//     if (tx->GetState() == Transaction::State::MULTI) {
//         conn->SendERROR("MULTI calls can not be nested");
//         return;
//     }
    
//     tx->startMulti();
//     conn->SendStatus("OK");
// }

// void MultiFamily::Exec(CommandContext* cmd_cntx, CmdArgList args) {
//     (void)args;
//     auto conn = cmd_cntx->conn_cntx()->owner(); 
//     auto tx = cmd_cntx->tx();
    
//     if (tx->GetState() != Transaction::State::MULTI) {
//         conn->SendERROR("EXEC without MULTI");
//         return;
//     }
    
//     if (tx->IsDirty()) {
//         conn->Send(std::string(""));
//         //tx->FinishOrDiscardMulti();
//         return;
//     }
    
//     auto& queued = tx->GetQueuedCommands();
//     if (queued.empty()) {
//         conn->SendVec({});
//         tx->SetState(Transaction::State::IDLE);
//         return;
//     }
    
//     tx->SetState(Transaction::State::EXEC);

//     for (const auto& cmd : queued) {
//         // Transaction*& t = tx->CreateSubTransaction(cmd.cid);
//         // t->InitByArgs(tx->GetConnectionContext(), cmd.GetCmdArgList());
//         // cmd.cid->Invoke(&t->GetCommandContext(), cmd.GetCmdArgList());
//     }
// }

// void MultiFamily::Discard(CommandContext* cmd_cntx, CmdArgList args) {
//     (void)args;
//     auto conn = cmd_cntx->conn_cntx()->owner(); 
//     auto tx = cmd_cntx->tx();
    
//     if (tx->GetState() != Transaction::State::MULTI) {
//         conn->SendERROR("DISCARD without MULTI");
//         return;
//     }
    
//     //tx->FinishOrDiscardMulti();
//     conn->SendStatus("OK");
// }


// cppcoro::AsyncTask CmdWatch(CommandContext* cmd_cntx, CmdArgList args) {
//     auto conn = cmd_cntx->conn_cntx()->owner(); 
//     auto tx = cmd_cntx->tx();
    
//     if (tx->GetState() == Transaction::State::MULTI) {
//         conn->SendERROR("WATCH inside MULTI is not allowed");
//         co_return;
//     }
//     size_t keyNum = args.size() - 1;
//     util::EmbeddedBlockingCounter eb(keyNum);
//     for (size_t i = 1; i < args.size(); ++i) {
//         tx->AddWatchKey(args[i], [&eb](){
//             eb.Dec();
//     });
//     }
//     bool isSuccess = co_await eb.Wait();
//     assert(isSuccess);
//     conn->SendStatus("OK");   
//     co_return; 
// }

// void MultiFamily::Watch(CommandContext* cmd_cntx, CmdArgList args) {
//     CmdWatch(cmd_cntx, args);
// }


// void MultiFamily::Unwatch(CommandContext* cmd_cntx, CmdArgList args) {
//     (void)args;
//     auto conn = cmd_cntx->conn_cntx()->owner(); 
//     auto tx = cmd_cntx->tx();
    
//     tx->ClearWatchKeys();
//     conn->SendStatus("OK");   
// }

// using CI = CommandId;
// void MultiFamily::Register(CommandRegistry* registry) {
//     registry->StartFamily();
//     *registry
//         << CI{"MULTI", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Multi)
//         << CI{"EXEC", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Exec)
//         << CI{"DISCARD", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Discard)
//         << CI{"WATCH", 1, kInvalidKeysNum, 1}.SetHandler(&MultiFamily::Watch)
//         << CI{"UNWATCH", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&MultiFamily::Unwatch)
//         ;
// }

// void RegisterMulti(CommandRegistry* registry) {
//     MultiFamily::Register(registry);
// }

// }  // namespace dfly