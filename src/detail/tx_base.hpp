#pragma once
#include "detail/common_types.hpp"
#include "command_layer/cmn_types.hpp"
#include "sharding/namespaces.hpp"
#include <span>
#include <iostream>
namespace dfly {




struct DbContext {
    Namespace* ns_ = nullptr; 
    DbIndex db_index_ = 0;
    uint64_t time_now_ms_ = 0;
    DbSlice& GetDbSlice(ShardId shard_id) const{
        return ns_->GetDbSlice(shard_id);        
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
        std::vector<IndexSlice>::const_iterator index_it_; // not same
        uint32_t delta_ = 0;

    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::string_view;
        using difference_type = ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;
        Iterator(::cmn::ArgSlice list, std::vector<IndexSlice>::const_iterator it)
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
            if (index() >= *index_it_) {
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
            return *index_it_ + delta_;
        }
    };

    using const_iterator = Iterator;

    ShardArgs(::cmn::ArgSlice fa, std::vector<IndexSlice> s) 
    : 
    slice_(fa), 
    index_(s)
    {
    }

    ShardArgs()  {
    }

    size_t Size() const;

    Iterator cbegin() const {

        return Iterator{slice_, index_.begin()};
    }

    Iterator cend() const {
        return Iterator{slice_, index_.end()};
    }

    Iterator begin() const {
        return cbegin();
    }

    Iterator end() const {
        return cend();
    }

    bool Empty() const {
        return index_.empty();
    }

    std::string_view Front() const {
        return *cbegin();
    }
private:

    ::cmn::ArgSlice slice_;
    std::vector<IndexSlice> index_;
};






}  // namespace dfly