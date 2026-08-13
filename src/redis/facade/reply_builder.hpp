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

  // 发送一个已构建好的原始 RESP 字符串（回放用）。该字符串直接作为
  // 整条回复发出，不再包装。
  void SendRaw(std::string s) {
    reply_ = std::move(s);
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
      AppendBulkStringRaw(item);
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
    if (send_cb_) {
      try {
        send_cb_(std::move(reply_));
      } catch (std::exception& e) {
        LOG(WARNING) << "ReplyBuilder::DoSend exception: " << e.what();
      }
    }
  }

  std::string reply_;
  SendCallback send_cb_;
};

}  // namespace dfly
