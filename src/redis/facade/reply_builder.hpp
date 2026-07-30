// reply_builder.h
#pragma once
#include <glog/logging.h>

#include <exception>
#include <functional>
#include <string>
#include <vector>
namespace dfly {

class ReplyBuilder {
 public:
  using SendCallback = std::function<void(std::string&&)>;

  void SetSendCallback(SendCallback cb) { send_cb_ = std::move(cb); }
  ~ReplyBuilder() { DCHECK_EQ(std::uncaught_exceptions(), 0); }

  // ---- 每个 Build 末尾自动调 SendCallback ----

  void BuildNull() {
    reply_ = "$-1\r\n";
    DoSend();
  }

  void BuildSimpleString(std::string_view s) {
    reply_.reserve(s.size() + 5);
    reply_.clear();
    reply_.append("+");
    reply_.append(s);
    reply_.append("\r\n");
    DoSend();
  }

  void BuildError(std::string_view err) {
    reply_.reserve(err.size() + 9);
    reply_.clear();
    reply_.append("-ERR ");
    reply_.append(err);
    reply_.append("\r\n");
    DoSend();
  }

  void BuildInteger(long n) {
    reply_.clear();
    reply_.append(":");
    reply_.append(std::to_string(n));
    reply_.append("\r\n");
    DoSend();
  }

  void BuildBulkString(std::string_view s) {
    reply_.clear();
    AppendBulkStringRaw(s);
    DoSend();
  }

  void BuildNullBulkString() {
    reply_ = "$-1\r\n";
    DoSend();
  }

  void BuildArray(const std::vector<std::string>& items) {
    reply_.clear();
    reply_.append("*");
    reply_.append(std::to_string(items.size()));
    reply_.append("\r\n");
    for (const auto& item : items) {
      AppendBulkStringRaw(item);  // 不触发 send，拼完统一发
    }
    DoSend();
  }

  void BuildMultiArray(const std::vector<std::string>& items) {
    reply_.clear();
    reply_.append("*");
    reply_.append(std::to_string(items.size()));
    reply_.append("\r\n");
    for (const auto& item : items) {
      reply_.append(item);  // 各 item 已是编好的 RESP 片段
    }
    DoSend();
  }

 private:
  void AppendBulkStringRaw(std::string_view s) {
    reply_.append("$");
    reply_.append(std::to_string(s.size()));
    reply_.append("\r\n");
    reply_.append(s);
    reply_.append("\r\n");
  }

  void DoSend() {
    if (send_cb_) send_cb_(std::move(reply_));  // TODO try 捕捉
  }

  std::string reply_;
  SendCallback send_cb_;
};

}  // namespace dfly
