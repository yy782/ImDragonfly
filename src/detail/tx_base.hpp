#pragma once
#include "common_types.hpp"

#include "namespaces.hpp"
#include <span>
namespace dfly {

using ArgSlice = std::span<const std::string_view>; // from arg_range.h
using IndexSlice = std::pair<uint32_t, uint32_t>;


struct DbContext {
    Namespace* ns_ = nullptr; 
    DbIndex db_index_ = 0;
    uint64_t time_now_ms = 0;
    DbSlice& GetDbSlice(ShardId shard_id) const{
        return ns->GetDbSlice(shard_id);        
    }  
};

struct OpArgs {
    EngineShard* shard_ = nullptr;
    const Transaction* tx_ = nullptr;
    DbContext db_cntx_;

    // Convenience method.
    DbSlice& GetDbSlice() const;
};


class ShardArgs {
public:
    class Iterator {
        ArgSlice arglist_;
        std::span<const IndexSlice>::const_iterator index_it_;
        uint32_t delta_ = 0;

    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::string_view;
        using difference_type = ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        // First version, corresponds to spans over arguments.
        Iterator(cmn::ArgSlice list, absl::Span<const IndexSlice>::const_iterator it)
            : arglist_(list), index_it_(it) {
        }

        bool operator==(const Iterator& o) const {
            return index_it_ == o.index_it_ && delta_ == o.delta_ && arglist_.data() == o.arglist_.data();
        }

        bool operator!=(const Iterator& o) const {
            return !(*this == o);
        }

        std::string_view operator*() const {
            return arglist_[index()];
        }

        Iterator& operator++() {
            ++delta_;
            if (index() >= index_it_->second) {
                ++index_it_;
                ++delta_ = 0;
            }
            return *this;
        }

        Iterator operator++(int) {
            Iterator copy = *this;
            operator++();
            return copy;
        }

        size_t index() const {
            return index_it_->first + delta_;
        }
    };

    using const_iterator = Iterator;

    ShardArgs(ArgSlice fa, std::span<const IndexSlice> s) : slice_(ArgsIndexPair(fa, s)) {
    }

    ShardArgs() : slice_(ArgsIndexPair{}) {
    }

    size_t Size() const;

    Iterator cbegin() const {
        return Iterator{slice_.first, slice_.second.begin()};
    }

    Iterator cend() const {
        return Iterator{slice_.first, slice_.second.end()};
    }

    Iterator begin() const {
        return cbegin();
    }

    Iterator end() const {
        return cend();
    }

    bool Empty() const {
        return slice_.second.empty();
    }

    std::string_view Front() const {
        return *cbegin();
    }
private:
    using ArgsIndexPair = std::pair<ArgSlice, std::span<const IndexSlice>>;
    ArgsIndexPair slice_;
};






}  // namespace dfly