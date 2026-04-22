// network/redis_server.h
#pragma once

#include <boost/asio.hpp>
#include <thread>
#include <vector>
#include <functional>
#include "EngineShardSet.hpp"
#include "cluster_support.hpp"
#include "facade/redis_parser.hpp"
#include "facade/reply_builder.hpp"
#include "src/include/namespaces.hpp"
using namespace boost;

namespace dfly{


class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(asio::ip::tcp::socket socket, EngineShardSet* shard_set)
        : socket_(std::move(socket)), shard_set_(shard_set) {}
    
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
            return BuildError("empty command");
        }
        
        const std::string& cmd = args[0];
        
        // 转换为大写
        std::string upper_cmd = cmd;
        for (auto& c : upper_cmd) c = toupper(c);
        
        // ==================== 字符串命令 ====================
        if (upper_cmd == "SET" && args.size() >= 3) {
            return HandleSet(args[1], args[2]);
        }
        else if (upper_cmd == "GET" && args.size() >= 2) {
            return HandleGet(args[1]);
        }
        else if (upper_cmd == "DEL" && args.size() >= 2) {
            return HandleDel(args);
        }
        
        return BuildError("unknown command '" + cmd + "'");
    }
    
    // ==================== 命令实现 ====================
    
    std::string HandleSet(const std::string& key, const std::string& value) {
        ShardId sid = KeySlot(std::string_view(key));
        
        // 使用 BlockingCounter 等待完成
        util::fb2::BlockingCounter bc(1);

        
        shard_set_->Add(sid, [&]() {
            auto* shard = EngineShard::tlocal();

            Namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id())
                                            .AddOrFind();
            bc.Dec();
        });
        
        bc.Wait();
        return BuildString("OK");
    }
    
    std::string HandleGet(const std::string& key) {
        ShardId sid = Shard(key, shard_set_->size());
        
        std::optional<std::string> result;
        util::fb2::BlockingCounter bc(1);
        
        shard_set_->Add(sid, [&]() {
            auto* shard = EngineShard::tlocal();
            result = shard->db_slice().Get(key);
            bc.Dec();
        });
        
        bc.Wait();
        
        if (result.has_value()) {
            return BuildBulkString(*result);
        } else {
            return BuildNull();
        }
    }
    
    std::string HandleDel(const std::vector<std::string>& args) {
        std::atomic<int> deleted{0};
        util::fb2::BlockingCounter bc(args.size() - 1);
        
        for (size_t i = 1; i < args.size(); i++) {
            ShardId sid = Shard(args[i], shard_set_->size());
            shard_set_->Add(sid, [&, key = args[i]]() {
                auto* shard = EngineShard::tlocal();
                if (shard->db_slice().Del(key)) {
                    deleted++;
                }
                bc.Dec();
            });
        }
        
        bc.Wait();
        return BuildInteger(deleted.load());
    }
      

    asio::ip::tcp::socket socket_;
    asio::streambuf buffer_;
    EngineShardSet* shard_set_;
};
class RedisServer {
public:
    RedisServer(asio::io_context& io_context, short port, EngineShardSet* shard_set)
        : acceptor_(io_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
          socket_(io_context),
          shard_set_(shard_set) {
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
                std::make_shared<RedisSession>(std::move(socket_), shard_set_)->Start();
            }
            DoAccept();
        });
    }
    
    asio::ip::tcp::acceptor acceptor_;
    asio::ip::tcp::socket socket_;
    EngineShardSet* shard_set_;
};



}