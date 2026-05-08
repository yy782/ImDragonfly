// main.cpp
// ./imdragonfly

#include <iostream>
#include <signal.h>
#include "src/network/redis_server.hpp"

#include <memory>

using namespace dfly;
std::unique_ptr<RedisServer> g_server;
std::unique_ptr<asio::io_context> g_io_context;
using namespace boost;
void SignalHandler(int signum) {
    std::cout << "Shutting down..." << std::endl;
    if (g_server) g_server->Stop();
    if (shard_set) shard_set->Shutdown();
    if (g_io_context) g_io_context->stop();
}

int main(int argc, char* argv[]) {
    //  初始化信号处理
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    auto pp = std::unique_ptr<util::fb2::Pool>(util::fb2::Pool::IOUring(256, 4));
    pp->Run();
    //  创建 EngineShardSet
    shard_set = new EngineShardSet(pp.get());
    
    //  初始化分片
    shard_set->Init(4, []() {
        std::cout << "Shard initialized" << std::endl;
    });
    
    //  创建 ASIO 网络层
    g_io_context = std::make_unique<asio::io_context>();
    g_server = std::make_unique<RedisServer>(*g_io_context, 6379);
    g_server->Start();
    std::cout << "Redis server started on port 6379" << std::endl;
    //  运行事件循环
    g_io_context->run();

    delete shard_set;
    return 0;
}