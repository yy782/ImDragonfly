// reply_builder.h
#pragma once
#include <glog/logging.h>

#include <charconv>
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include "util/function.hpp"
namespace dfly {

class ReplyBuilder {
 public:
  using SendCallback = util::function<void(std::vector<std::string>&&)>;

  ReplyBuilder() = default;
  ReplyBuilder(const ReplyBuilder&) = default;
  ReplyBuilder& operator=(const ReplyBuilder&) = default;
  ReplyBuilder(ReplyBuilder&&) = default;
  ReplyBuilder& operator=(ReplyBuilder&&) = default;

  void SetSendCallback(SendCallback cb) { send_cb_ = std::move(cb); }
  ~ReplyBuilder() { DCHECK_EQ(std::uncaught_exceptions(), 0); }

  void SendRaw(std::string s) { pending_.push_back(std::move(s)); }

  void BuildSimpleString(std::string_view s) {
    std::string r;
    r.reserve(s.size() + 5);
    r.append("+");
    r.append(s);
    r.append("\r\n");
    pending_.push_back(std::move(r));
  }

  void BuildError(std::string_view err) {
    std::string r;
    r.reserve(err.size() + 9);
    r.append("-ERR ");
    r.append(err);
    r.append("\r\n");
    pending_.push_back(std::move(r));
  }

  void BuildInteger(long n) {
    char buf[24];
    auto res = std::to_chars(buf, buf + sizeof(buf), n);
    std::string r;
    r.reserve(res.ptr - buf + 2);
    r.append(":");
    r.append(buf, res.ptr - buf);
    r.append("\r\n");
    pending_.push_back(std::move(r));
  }

  void BuildBulkString(std::string_view s) {
    std::string r;
    r.reserve(s.size() + 32);
    AppendBulkStringRaw(r, s);
    pending_.push_back(std::move(r));
  }

  void BuildNullBulkString() { pending_.emplace_back("$-1\r\n"); }

  void BuildDouble(double d) {
    char buf[64];
    int len = std::snprintf(buf, sizeof(buf), "%.17g", d);
    std::string r;
    r.append("$");
    AppendInt(r, static_cast<size_t>(len));
    r.append("\r\n");
    r.append(buf, static_cast<size_t>(len));
    r.append("\r\n");
    pending_.push_back(std::move(r));
  }

  void StartArray(size_t n) {
    std::string r;
    r.append("*");
    AppendInt(r, n);
    r.append("\r\n");
    pending_.push_back(std::move(r));
  }

  void BuildOk() { pending_.emplace_back("+OK\r\n"); }

  void BuildArray(const std::vector<std::string>& items) {
    std::string r;
    r.append("*");
    AppendInt(r, items.size());
    r.append("\r\n");
    for (const auto& item : items) {
      AppendBulkStringRaw(r, item);
    }
    pending_.push_back(std::move(r));
  }

  void Flush() {
    if (pending_.empty()) return;
    if (send_cb_) send_cb_(std::move(pending_));
    pending_.clear();
  }

  bool empty() const { return pending_.empty(); }

 private:
  static void AppendInt(std::string& dst, size_t n) {
    char buf[24];
    auto res = std::to_chars(buf, buf + sizeof(buf), n);
    dst.append(buf, res.ptr - buf);
  }

  void AppendBulkStringRaw(std::string& dst, std::string_view s) {
    dst.append("$");
    AppendInt(dst, s.size());
    dst.append("\r\n");
    dst.append(s);
    dst.append("\r\n");
  }

  std::vector<std::string> pending_;
  SendCallback send_cb_;
};

}  // namespace dfly
