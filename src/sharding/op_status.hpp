#pragma once

namespace dfly {

enum class OpStatus : uint16_t {
    OK,
    KEY_NOTFOUND,
    WRONG_TYPE,
    OUT_OF_MEMORY,
};

class OpResultBase {
 public:
    OpResultBase(OpStatus st = OpStatus::OK) : st_(st) {
    }

    constexpr explicit operator bool() const {
        return st_ == OpStatus::OK;
    }

    OpStatus status() const {
        return st_;
    }

    bool operator==(OpStatus st) const {
        return st_ == st;
    }

    bool ok() const {
        return st_ == OpStatus::OK;
    }

    const char* DebugFormat() const;

private:
    OpStatus st_;
};

template <typename V> 
class OpResult : public OpResultBase {
public:
    using Type = V;

    OpResult(V&& v) : v_(std::move(v)) {
    }

    OpResult(const V& v) : v_(v) {
    }

    using OpResultBase::OpResultBase;

    const V& value() const {
        return v_;
    }

    V& value() {
        return v_;
    }

    V value_or(V v) const {
        return status() == OpStatus::OK ? v_ : v;
    }

    V* operator->() {
        return &v_;
    }

    V& operator*() & {
        return v_;
    }

    V&& operator*() && {
        return std::move(v_);
    }

    const V* operator->() const {
        return &v_;
    }

    const V& operator*() const& {
        return v_;
    }

private:
    V v_{};
};

template <>
class OpResult<void> : public OpResultBase {
public:
    using OpResultBase::OpResultBase;
};

inline bool operator==(OpStatus st, const OpResultBase& ob) {
    return ob.operator==(st);
}

std::string_view StatusToMsg(OpStatus status);

}  // namespace facade