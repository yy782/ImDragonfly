// network/redis_server.h
#pragma once

#include <boost/asio.hpp>
#include <thread>
#include <vector>
#include <functional>
#include "engine_shard_set.hpp"
#include "cluster_support.hpp"
#include "facade/redis_parser.hpp"
#include "facade/reply_builder.hpp"
#include "src/include/namespaces.hpp"


#include "dispatch_command.hpp"

using namespace boost;

namespace dfly{


class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(asio::ip::tcp::socket socket )
        : socket_(std::move(socket)), common_({Namespaces->GetDefaultNamespace(), 0}) {}
    
    void Start() {
        DoRead();
    }
    
private:
    void DoRead() {
        auto self = shared_from_this();
        asio::async_read_until(socket_, buffer_, "\r\n",
            [this, self](std::error_code ec, size_t bytes) {
                if (!ec) {
                    ProcessCommand();
                    DoRead();
                }
            });
    }
    
    void ProcessCommand() {
        std::istream is(&buffer_);
        std::string line;
        std::getline(is, line);
        
        // 解析 RESP 协议
        auto args = ParseRESP(line);
        if (args.empty()) return;
        
        // 执行命令
        std::string response = ExecuteCommand(args);
        
        // 发送响应
        asio::async_write(socket_, asio::buffer(response),
            [](std::error_code ec, size_t) {});
    }
    
   std::string ExecuteCommand(const std::vector<std::string>& args) {
        if (args.empty()) {
            return common_.BuildError("empty command");
        }
        
        const std::string& cmd = args[0];
        
        // 转换为大写
        std::string upper_cmd = cmd;
        for (auto& c : upper_cmd) c = toupper(c);
        
        if (upper_cmd == "SET" && args.size() >= 3) {
            return common_.HandleSet(args[1], args[2]);
        }
        else if (upper_cmd == "GET" && args.size() >= 2) {
            return common_.HandleGet(args[1]);
        }
        else if (upper_cmd == "DEL" && args.size() >= 2) {
            return common_.HandleDel(args);
        }
        
        return common_.BuildError("unknown command '" + cmd + "'");
    }
    

    

      

    asio::ip::tcp::socket socket_;
    asio::streambuf buffer_;

    Common common_;
};
class RedisServer {
public:
    RedisServer(asio::io_context& io_context, short port)
        : acceptor_(io_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
          socket_(io_context)
           {
    }
    
    void Start() {
        DoAccept();
    }
    
    void Stop() {
        acceptor_.close();
    }
    
private:
    void DoAccept() {
        acceptor_.async_accept(socket_, [this](std::error_code ec) {
            if (!ec) {
                std::make_shared<RedisSession>(std::move(socket_))->Start();
            }
            DoAccept();
        });
    }
    
    asio::ip::tcp::acceptor acceptor_;
    asio::ip::tcp::socket socket_;
};



}