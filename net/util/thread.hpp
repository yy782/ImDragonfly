#pragma once
#include <string>
namespace util{


#include <pthread.h>
#include <functional>
#include <utility>
#include <stdexcept>

class Thread {
public:
    Thread(const char* name = nullptr) :  tid_(0), joined_(false) {
        name_ = name ? name : "unnamed_thread";
    }
    
    template<typename Func, typename... Args>
    explicit Thread(const char* name, Func&& func, Args&&... args) : Thread(name) {
        auto* wrapper = new std::function<void()>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
        );
        
        if (pthread_create(&tid_, nullptr, thread_func, wrapper) != 0) {
            delete wrapper;
            assert(false && "Failed to create thread");
        }
    }
    
    Thread(const Thread&) = delete;
    Thread& operator=(const Thread&) = delete;
    
    Thread(Thread&& other) noexcept 
        : tid_(other.tid_), joined_(other.joined_) {
        other.tid_ = 0;
        other.joined_ = false;
    }
    
    Thread& operator=(Thread&& other) noexcept {
        if (this != &other) {
            if (joinable()) {
                std::terminate();  
            }
            tid_ = other.tid_;
            joined_ = other.joined_;
            other.tid_ = 0;
            other.joined_ = false;
        }
        return *this;
    }
    
    ~Thread() {
        if (joinable()) {
            std::terminate();  
        }
    }
    
    void join() {
        if (!joinable()) {
            throw std::runtime_error("Thread not joinable");
        }
        pthread_join(tid_, nullptr);
        joined_ = true;
    }
    
    void detach() {
        if (!joinable()) {
            throw std::runtime_error("Thread not joinable");
        }
        pthread_detach(tid_);
        joined_ = true;
    }
    
    bool joinable() const noexcept {
        return tid_ != 0 && !joined_;
    }
    
    pthread_t native_handle() const {
        return tid_;
    }
    

    static void sleep_for(double seconds) {
        timespec ts;
        ts.tv_sec = static_cast<time_t>(seconds);
        ts.tv_nsec = static_cast<long>((seconds - ts.tv_sec) * 1e9);
        nanosleep(&ts, nullptr);
    }

    static void yield() {
        sched_yield();
    }
    
    static Thread current_thread() {
        Thread t;
        t.tid_ = pthread_self();
        t.joined_ = false;
        return t;
    }

private:

    std::string name_;
    pthread_t tid_;
    bool joined_;
    
    static void* thread_func(void* arg) {
        std::unique_ptr<std::function<void()>> func(
            static_cast<std::function<void()>*>(arg)
        );
        (*func)();
        return nullptr;
    }

    
};

}