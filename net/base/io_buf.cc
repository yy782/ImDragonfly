// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/io_buf.h"
#include <assert.h>
#include <sys/uio.h>
namespace base {    
TcpBuffer::TcpBuffer(size_t initial_size,size_t prepend_size):
buffer_(prepend_size+initial_size),
prepend_size_(prepend_size),
read_index_(prepend_size),
write_index_(prepend_size)
{}
void TcpBuffer::swap(TcpBuffer& other) noexcept
{
    buffer_.swap(other.buffer_);
    std::swap(read_index_,other.read_index_);
    std::swap(write_index_,other.write_index_);
}
void TcpBuffer::append(const char* data,size_t size)
{
    appendImp(data,size);
}

TcpBuffer& TcpBuffer::FluentAppend(const char* data,size_t size)
{
    appendImp(data,size);
    return *this;
}
ssize_t TcpBuffer::appendFormFd(int fd)
{
    
    char extrabuf[65536];
    struct iovec vec[2];
    const size_t writable=writable_size();
    vec[0].iov_base=begin_write();
    vec[0].iov_len=writable;
    vec[1].iov_base=extrabuf;
    vec[1].iov_len=sizeof extrabuf;

    const int iovcnt = (writable < sizeof extrabuf) ? 2 : 1;
    const ssize_t n = ::readv(fd, vec, iovcnt);
    if(n>0)
    {
        if(static_cast<size_t>(n)<=writable_size())
        {
            move_write_index(n);
        }
        else 
        {
            write_index_=buffer_.size();
            append(extrabuf, n - writable);
        }
    }
    return n;

}
void TcpBuffer::appendImp(const char* data,size_t size)
{
    ensure_appendable(size);
    std::copy(data,data+size,begin_write());
    move_write_index(size);
}
void TcpBuffer::consume(size_t size)
{
    move_read_index(size);
}
char* TcpBuffer::BeginWrite()
{
    return begin_write();
}
char* TcpBuffer::retrieve(size_t size)
{
    if(size<=readable_size())
    {
        char* r=begin_read();
        move_read_index(size);
        return r;
    }
    return nullptr;
}
char* TcpBuffer::retrieveAll()
{
    auto size=readable_size();
    return retrieve(size);
}

std::string TcpBuffer::retrieveAllToString()
{
    
    return std::string(retrieveAll());
}    
void TcpBuffer::ensureWritableBytes(size_t len)
{
    if (writable_size()<len)
    {
        expend(len);
    }
    assert(writable_size() >= len&&"可写尺寸小于需写尺寸");
    check_index_validity();
}
void TcpBuffer::hasWritten(size_t len)
{
    assert(len<=writable_size());
    write_index_+=len;
    check_index_validity();
}
void TcpBuffer::unwrite(size_t len)
{
    assert(len<=readable_size());
    write_index_-=len;
    check_index_validity();
}
void TcpBuffer::shrink(size_t reserve)
{
    buffer_.resize(readable_size()+reserve+prepend_size_);
    buffer_.shrink_to_fit();
}

void TcpBuffer::check_index_validity()const
{
#ifndef NDEBUG        
        auto read_index=std::to_string(read_index_);
        auto prepend_size=std::to_string(prepend_size_);
    
        auto check_read_ptr=" 可读指针是 "+read_index+" 预留大小是 "+prepend_size;
        assert(read_index_>=prepend_size_&&check_read_ptr.c_str());
        
        auto write_index=std::to_string(write_index_);
        auto check_write_ptr=" 可写指针是 "+write_index+" 可读指针是 "+read_index;
        assert(write_index_>=read_index_&&check_write_ptr.c_str());
#endif    
}
void TcpBuffer::ensure_appendable(size_t size)
{
    if(writable_size()<size)
    {
        expend(size);
    }
    assert(writable_size()>=size&&"可写尺寸小于需写尺寸");
    check_index_validity();
}
void TcpBuffer::move_read_index(size_t size)
{
    assert(size<=readable_size());
    if(size<readable_size())
    {
        read_index_+=size;
        check_index_validity();            
    }
    else 
    {
        check_index_validity();
        read_index_=prepend_size_;
        write_index_=prepend_size_;
        check_index_validity();
    }
}
void TcpBuffer::move_write_index(size_t size)
{
    assert(size<=writable_size()&&"可写尺寸不够");
    write_index_+=size;
}
void TcpBuffer::expend(size_t size)
{
    check_index_validity();
    if(writable_size()+prependable_size()>size+prepend_size_)
    {
        //复用
        reuse_prependable_space();
    }
    else
    {
        reuse_prependable_space();
        // @brief muduo没有选择进行优化，但是提出了建议。我选择进行优化
        buffer_.resize(write_index_+size);
        check_index_validity();
    }
}
void TcpBuffer::reuse_prependable_space()
{
    size_t readable=readable_size();
    std::copy(begin()+read_index_,
                begin()+write_index_,
                begin()+prepend_size_);
    read_index_=prepend_size_;
    write_index_=read_index_+readable;
    check_index_validity();        
}    



}  // namespace base

