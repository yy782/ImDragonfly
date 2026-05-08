// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once


#include <cstring>
#include <vector>
#include <string>
#include <assert.h>
namespace base {

class TcpBuffer;
using IoBuf = TcpBuffer;


class TcpBuffer // TODO 不能拷贝
{
public: 

    typedef std::vector<char> CharContainer;

    explicit TcpBuffer(size_t initial_size=1024,size_t prepend_size=8);
    void reset()///////////////////////////////////////////////////////////////////
    {
        read_index_=8;
        write_index_=8;
    }

    ~TcpBuffer()=default;
    
    void swap(TcpBuffer& other) noexcept;
    
    template<typename T>
    void append(T&& value);
    

    void append(const char* data,size_t size);
    

    void append(const char* data){append(data, strlen(data));}
    

    ssize_t appendFormFd(int fd);
    

    template<typename T>
    TcpBuffer& FluentAppend(T&& value);
    

    TcpBuffer& FluentAppend(const char* data,size_t size);
    

    TcpBuffer& FluentAppend(const char*)
    {
        assert(false&&"请指明长度");
        return *this;
    };
    
    void consume(size_t size);

    char* BeginWrite();
    

    char* retrieve(size_t size);

    char* retrieveAll();
    
    std::string retrieveAllToString();
    
    const char* peek() const noexcept{return begin()+read_index_;}

    
    std::string_view readView()const noexcept{return std::string_view(peek(),readable_size());}
    
    char* ModifyData(){return begin()+read_index_;}

    void shrink(size_t reserve);

    size_t readable_size() const noexcept{return write_index_-read_index_;}
    
    size_t writable_size() const noexcept{return buffer_.size()-write_index_;}
    
    size_t prependable_size()const noexcept{return read_index_;}

    char* findBorder(const char* border) noexcept
    {
        return std::search(begin_read(),begin_write(),border,border+strlen(border));
    }
    
    char* findBorder(const char* border,size_t size) noexcept
    {
        return std::search(begin_read(),begin_write(),border,border+size);
    }

    char* findBorder(const char* border,size_t size,size_t& len) noexcept
    {
        char* last=findBorder(border,size);
        len=last-begin_read();
        return last;
    }
    
    void prepend(const char* data,size_t size) noexcept
    {
        assert(size <= prependable_size());
        read_index_ -= size;
        std::copy(data, data + size, begin_read());
    }
    
    void prepend(const void* data,size_t size) noexcept
    {
        prepend(static_cast<const char*>(data), size);
    }

    void ensureWritableBytes(size_t len);
    
    void hasWritten(size_t len);
    
    void unwrite(size_t len);

    void clear() noexcept
    {
        read_index_=8;
        write_index_=8;
    }
protected:
    void move_write_index(size_t size);
    
    void move_read_index(size_t size);
    
    void check_index_validity()const;
    
    void ensure_appendable(size_t size);

    char* begin_write(){return begin()+write_index_;}
    
    char* begin_read(){return begin()+read_index_;}
    
    char* begin(){return &*buffer_.begin();}
    
    const char* begin()const{return &*buffer_.begin();}
    
    void expend(size_t size);
    
    void reuse_prependable_space();

    void appendImp(const char* data,size_t size);

    CharContainer buffer_;// inOne
    
    const size_t prepend_size_;
    
    size_t read_index_;
    

    size_t write_index_;
    



};
/**
 * @brief 添加数据
 * 
 * @tparam T 数据类型
 * @param value 要添加的数据
 */
template<typename T>
void TcpBuffer::append(T&& value)
{
    using DecayedT = std::decay_t<T>;
    static_assert(!std::is_same_v<DecayedT,std::string>);
    if constexpr(std::is_same_v<DecayedT,std::string_view>)
    {
        appendImp(value.data(),value.size());
    }
    else
        appendImp(reinterpret_cast<const char*>(&value), sizeof(T));
    
}

template<typename T>
TcpBuffer& TcpBuffer::FluentAppend(T&& value)
{
    append(std::forward<T>(value));
    return *this;
}

}  // namespace base
