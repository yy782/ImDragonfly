// main.cpp
// ./imdragonfly

#include <iostream>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "src/network/redis_server.hpp"

#include <memory>

using namespace dfly;


int main(int argc, char* argv[]) {
    
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        perror("socket");
        return -1;
    }

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("setsockopt");
        close(listen_fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY; 
    addr.sin_port = htons(6379);       

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(listen_fd);
        return -1;
    }
    int backlog = 128;  
    if (listen(listen_fd, backlog) == -1) {
        perror("listen");
        close(listen_fd);
        return -1;
    }
    RedisServer server(listen_fd, 4); // 监听6379端口，使用4个分片
    server.Start();

    return 0;
}