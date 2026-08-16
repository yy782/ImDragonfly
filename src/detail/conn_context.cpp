#include "conn_context.hpp"

#include "transaction_layer/transaction.hpp"

namespace dfly {

ConnectionContext::~ConnectionContext() {}

CommandContext::CommandContext(util::intrusive_ptr<Transaction> transaction,
                               const CommandId* cid,
                               ReplyBuilder* reply_builder)
    : transaction_(std::move(transaction)),
      cid_(cid),
      reply_builder_(reply_builder) {
  DCHECK(reply_builder_);
}

CommandContext::~CommandContext() = default;

util::intrusive_ptr<Transaction> CommandContext::tx() const {
  return transaction_;
}

}  // namespace dfly