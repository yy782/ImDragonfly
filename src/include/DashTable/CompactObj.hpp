// simplified_compact_obj.h
#pragma once

#include <cstring>
#include <optional>
#include <string>
#include <string_view>
namespace detail
{

enum CompactObjType : uint8_t {
    OBJ_STRING = 0,
    OBJ_INT = 1,
    OBJ_NONE = 2,
};


class CompactObj {
public:
    CompactObj() : tag_(TAG_EMPTY), type_(OBJ_NONE) {
        memset(inline_data_, 0, sizeof(inline_data_));
    }
    
    explicit CompactObj(int64_t val) : tag_(TAG_INT), type_(OBJ_INT) {
        int_val_ = val;
    }
    
    explicit CompactObj(std::string_view str) : tag_(TAG_EMPTY), type_(OBJ_STRING) {
        SetString(str);
    }
    CompactObj(const CompactObj& other) : tag_(TAG_EMPTY), type_(OBJ_NONE) {
        CopyFrom(other);
    }
    
    CompactObj& operator=(const CompactObj& other) {
        if (this != &other) {
            Free();
            CopyFrom(other);
        }
        return *this;
    }
    CompactObj(CompactObj&& other) noexcept 
        : tag_(TAG_EMPTY), type_(OBJ_NONE), is_key_(other.is_key_) {
        MoveFrom(std::move(other));
    }
    
    CompactObj& operator=(CompactObj&& other) noexcept {
        if (this != &other) {
            Free();
            MoveFrom(std::move(other));
        }
        return *this;
    }
    
    ~CompactObj() {
        Free();
    }
    void SetString(std::string_view str) {
        Free();
        type_ = OBJ_STRING;
        
        if (str.size() <= kInlineSize) {
            // 内联存储：直接复制到 inline_data_
            tag_ = TAG_INLINE;
            memcpy(inline_data_, str.data(), str.size());
            inline_data_[str.size()] = '\0';
            inline_len_ = str.size();
        } else {
            // 堆存储：分配内存
            tag_ = TAG_HEAP;
            heap_len_ = str.size();
            heap_cap_ = str.size() + 1;
            heap_ptr_ = new char[heap_cap_];
            memcpy(heap_ptr_, str.data(), str.size());
            heap_ptr_[str.size()] = '\0';
        }
    }
    void SetInt(int64_t val) {
        Free();
        type_ = OBJ_INT;
        tag_ = TAG_INT;
        int_val_ = val;
    }
    std::string_view GetString() const {
        if (type_ != OBJ_STRING) return {};
        
        if (tag_ == TAG_INLINE) {
            return std::string_view(inline_data_, inline_len_);
        } else if (tag_ == TAG_HEAP) {
            return std::string_view(heap_ptr_, heap_len_);
        }
        return {};
    }
    std::optional<int64_t> GetInt() const {
        if (type_ == OBJ_INT && tag_ == TAG_INT) {
            return int_val_;
        }
        return std::nullopt;
    }
    CompactObjType GetType() const { return static_cast<CompactObjType>(type_); }
    bool IsEmpty() const { return type_ == OBJ_NONE; }
    size_t MemoryUsage() const {
        if (tag_ == TAG_HEAP) {
            return heap_cap_;
        }
        return 0;
    }
    bool operator==(const CompactObj& other) const {
        if (type_ != other.type_) return false;
        
        if (type_ == OBJ_STRING) {
            return GetString() == other.GetString();
        } else if (type_ == OBJ_INT) {
            return int_val_ == other.int_val_;
        }
        return true;
    }
    
    bool operator!=(const CompactObj& other) const { return !(*this == other); }
    bool IsKey() const { return is_key_; }
    
protected:
    void SetIsKey(bool is_key) { is_key_ = is_key; }
private:
    enum Tag : uint8_t {
        TAG_EMPTY = 0,   // 空
        TAG_INLINE = 1,  // 内联字符串（≤15字节）
        TAG_HEAP = 2,    // 堆字符串
        TAG_INT = 3,     // 整数
    };
    
    static constexpr size_t kInlineSize = 15;  // 内联字符串最大长度
    
    // 释放堆内存
    void Free() {
        if (tag_ == TAG_HEAP && heap_ptr_) {
            delete[] heap_ptr_;
            heap_ptr_ = nullptr;
        }
        tag_ = TAG_EMPTY;
    }
    void CopyFrom(const CompactObj& other) {
        is_key_ = other.is_key_;
        type_ = other.type_;
        tag_ = other.tag_;
        
        if (tag_ == TAG_INLINE) {
            memcpy(inline_data_, other.inline_data_, sizeof(inline_data_));
            inline_len_ = other.inline_len_;
        } else if (tag_ == TAG_HEAP) {
            heap_len_ = other.heap_len_;
            heap_cap_ = other.heap_cap_;
            heap_ptr_ = new char[heap_cap_];
            memcpy(heap_ptr_, other.heap_ptr_, heap_len_ + 1);
        } else if (tag_ == TAG_INT) {
            int_val_ = other.int_val_;
        }
    }
    void MoveFrom(CompactObj&& other) {
        is_key_ = other.is_key_;
        type_ = other.type_;
        tag_ = other.tag_;
        
        if (tag_ == TAG_INLINE) {
            memcpy(inline_data_, other.inline_data_, sizeof(inline_data_));
            inline_len_ = other.inline_len_;
        } else if (tag_ == TAG_HEAP) {
            heap_ptr_ = other.heap_ptr_;
            heap_len_ = other.heap_len_;
            heap_cap_ = other.heap_cap_;
            other.heap_ptr_ = nullptr;
        } else if (tag_ == TAG_INT) {
            int_val_ = other.int_val_;
        }
        
        other.tag_ = TAG_EMPTY;
        other.type_ = OBJ_NONE;
    }
    
    // 联合体（16 字节对齐）
    union {
        struct {
            char inline_data_[kInlineSize + 1];  // +1 for null terminator
            uint8_t inline_len_;
        };
        struct {
            char* heap_ptr_;
            size_t heap_len_;
            size_t heap_cap_;
        };
        int64_t int_val_;
    };
    
    uint8_t tag_ : 2;      // 存储方式标签
    uint8_t type_ : 2;     // 数据类型（string/int）
    uint8_t is_key_ : 1;   // 是否为 key（true for PrimeKey）
    // 剩余 3 位保留，可用于未来的扩展
};
static_assert(sizeof(CompactObj) <= 32, "CompactObj should be small");

class CompactKey : public CompactObj {
public:
    CompactKey() : CompactObj() {
        SetIsKey(true);
    }
    
    explicit CompactKey(std::string_view str) : CompactObj(str) {
        SetIsKey(true);
    }
    
    explicit CompactKey(int64_t val) : CompactObj(val) {
        SetIsKey(true);
    }
    CompactKey(const CompactKey& other) : CompactObj(other) {}
    CompactKey(CompactKey&& other) noexcept : CompactObj(std::move(other)) {}
    
    CompactKey& operator=(const CompactKey& other) {
        CompactObj::operator=(other);
        return *this;
    }
    
    CompactKey& operator=(CompactKey&& other) noexcept {
        CompactObj::operator=(std::move(other));
        return *this;
    }
    size_t Hash() const {
        if (GetType() == OBJ_STRING) {
            std::string_view sv = GetString();
            size_t hash = 0x811c9dc5;
            for (char c : sv) {
                hash ^= c;
                hash *= 0x01000193;
            }
            return hash;
        } else if (GetType() == OBJ_INT) {
            auto val = GetInt();
            return val.value_or(0);
        }
        return 0;
    }
    
    bool Equals(const CompactKey& other) const {
        return *this == other;
    }
};
struct CompactValue : public CompactObj {
  CompactValue() : CompactObj(false) {
  }

  explicit CompactValue(std::string_view str) : CompactObj(str) {
  }
};
using PrimeKey = CompactKey;
using PrimeValue = CompactValue;
}