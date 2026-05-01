// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <span.h>
#include <sys/uio.h>

#include <string_view>

#include "io/io_buf.h"

namespace io {

using MutableBytes = std::span<uint8_t>;
using Bytes = std::span<const uint8_t>;

inline Bytes Buffer(std::string_view str) {
  return Bytes{reinterpret_cast<const uint8_t*>(str.data()), str.size()};
}

template <size_t N> inline MutableBytes MutableBuffer(char (&buf)[N]) {
  return MutableBytes{reinterpret_cast<uint8_t*>(buf), N};
}

inline std::string_view View(Bytes bytes) {
  return std::string_view{reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

inline std::string_view View(MutableBytes bytes) {
  return View(Bytes{bytes});
}

/// Similar to Rust std::io::Result.
template <typename T, typename E = ::std::error_code> 
using Result = utils::expected<T, E>;

class Source {
 public:
  virtual ~Source() {
  }

  Result<size_t> ReadSome(const MutableBytes& dest) {
    iovec v{.iov_base = dest.data(), .iov_len = dest.size()};
    return ReadSome(&v, 1);
  }

  virtual Result<size_t> ReadSome(const iovec* v, uint32_t len) = 0;

  Result<size_t> ReadAtLeast(const MutableBytes& dest, size_t min_size);

  Result<size_t> Read(const MutableBytes& dest) {
    return ReadAtLeast(dest, dest.size());
  }
};

class Sink {
 public:
  virtual ~Sink() {
  }

  Result<size_t> WriteSome(Bytes buf) {
    iovec v{.iov_base = const_cast<uint8_t*>(buf.data()), .iov_len = buf.size()};
    return WriteSome(&v, 1);
  }

  virtual Result<size_t> WriteSome(const iovec* v, uint32_t len) = 0;

  std::error_code Write(Bytes buf) {
    iovec v{.iov_base = const_cast<uint8_t*>(buf.data()), .iov_len = buf.size()};
    return Write(&v, 1);
  }

  std::error_code Write(const iovec* vec, uint32_t len);
};


using AsyncProgressCb = std::function<void(Result<size_t>)>;
using AsyncResultCb = std::function<void(std::error_code)>;

class AsyncSink {
 public:
  virtual void AsyncWriteSome(const iovec* v, uint32_t len, AsyncProgressCb cb) = 0;
  void AsyncWrite(const iovec* v, uint32_t len, AsyncResultCb cb);

  void AsyncWrite(Bytes buf, AsyncResultCb cb) {
    iovec v{const_cast<uint8_t*>(buf.data()), buf.size()};
    AsyncWrite(&v, 1, std::move(cb));
  }
};

class AsyncSource {
 public:
  virtual void AsyncReadSome(const iovec* v, uint32_t len, AsyncProgressCb cb) = 0;
  void AsyncRead(const iovec* v, uint32_t len, AsyncResultCb cb);
  void AsyncRead(MutableBytes buf, AsyncResultCb cb) {
    iovec v{buf.data(), buf.size()};
    AsyncRead(&v, 1, std::move(cb));
  }
};

class PrefixSource : public Source {
 public:
  PrefixSource(Bytes prefix, Source* upstream) : prefix_(prefix), upstream_(upstream) {
  }

  Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

  Bytes UnusedPrefix() const {
    return offs_ >= prefix_.size() ? Bytes{} : prefix_.subspan(offs_);
  }

 private:
  Bytes prefix_;
  Source* upstream_;
  size_t offs_ = 0;
};

class BytesSource : public Source {
 public:
  BytesSource(Bytes buf) : buf_(buf) {
  }
  BytesSource(std::string_view view) : buf_{Buffer(view)} {
  }

  Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

 protected:
  Bytes buf_;
  off_t offs_ = 0;
};

class BufSource : public Source {
 public:
  BufSource(IoBuf* source) : buf_{source} {
  }

  Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

 protected:
  IoBuf* buf_;
};

class NullSink final : public Sink {
 public:
  Result<size_t> WriteSome(const iovec* v, uint32_t len);
};

class BufSink : public Sink {
 public:
  BufSink(IoBuf* sink) : buf_{sink} {
  }

  Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

 protected:
  IoBuf* buf_;
};

class StringSink final : public Sink {
 public:
  Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  const std::string& str() const& {
    return str_;
  }

  std::string str() && {
    return std::move(str_);
  }

  void Clear() {
    str_.clear();
  }

 private:
  std::string str_;
};

template <typename SomeFunc>
std::error_code ApplyExactly(const iovec* v, uint32_t len, SomeFunc&& func) {
  const iovec* endv = v + len;
  while (v != endv) {
    Result<size_t> res = func(v, endv - v);
    if (!res) {
      return res.error();
    }

    size_t done = *res;

    while (v != endv && done >= v->iov_len) {
      done -= v->iov_len;
      ++v;
    }

    if (done == 0)
      continue;

    uint8_t* next = reinterpret_cast<uint8_t*>(v->iov_base) + done;
    uint8_t* base_end = reinterpret_cast<uint8_t*>(v->iov_base) + v->iov_len;
    do {
      iovec iovv{next, size_t(base_end - next)};
      res = func(&iovv, 1);
      if (!res) {
        return res.error();
      }
      next += *res;
    } while (next != base_end);
    ++v;
  }
  return std::error_code{};
}

}  // namespace io
