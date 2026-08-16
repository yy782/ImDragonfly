// reply_builder.h
#pragma once
#include <glog/logging.h>

#include <charconv>
#include <exception>
#include <string>
#include <vector>

#include "util/function.hpp"
namespace dfly {

class ReplyBuilder {
 public:
  // 回调契约：接收一批已编码回复（而非单条）。ReplyBuilder 只缓冲，
  // 何时把一批交给上层（发送/收集）由调用方通过 Flush() 决定。
  using SendCallback = util::function<void(std::vector<std::string>&&)>;

  ReplyBuilder() = default;
  ReplyBuilder(const ReplyBuilder&) = default;
  ReplyBuilder& operator=(const ReplyBuilder&) = default;
  ReplyBuilder(ReplyBuilder&&) = default;
  ReplyBuilder& operator=(ReplyBuilder&&) = default;

  void SetSendCallback(SendCallback cb) { send_cb_ = std::move(cb); }
  ~ReplyBuilder() { DCHECK_EQ(std::uncaught_exceptions(), 0); }

  // 提交一条已构建好的原始 RESP 字符串（回放用）。仅缓冲，Flush 时才交给回调。
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
    // 预留 "$" + len + "\r\n" + data + "\r\n" 的空间，避免 append 中途扩容。
    r.reserve(s.size() + 32);
    AppendBulkStringRaw(r, s);
    pending_.push_back(std::move(r));
  }

  void BuildNullBulkString() { pending_.emplace_back("$-1\r\n"); }

  // 直接产出完整 "+OK\r\n" 常量回复（SSO 内联，无堆分配、无逐字节拼装），
  // 用于 SET 等高频成功路径。
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

  // 把当前缓冲的一批回复一次性交给回调。空批不回调。
  void Flush() {
    if (pending_.empty()) return;
    if (send_cb_) send_cb_(std::move(pending_));
    pending_.clear();
  }

  bool empty() const { return pending_.empty(); }

 private:
  // 将整数写进 dst，避免 std::to_string 产生临时 string 分配。
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
