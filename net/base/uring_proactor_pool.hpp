

#pragma once
#include "uring_proactor.hpp"
#include "util/thread.hpp"

#include <memory>
#include <vector>
#include <latch>

namespace base{

using UringProactorPtr = std::shared_ptr<UringProactor>;


class UringProactorPool{
public:
    UringProactorPool(uint32_t size) : proactors_(size) {
        for(std::size_t i = 0;i < proactors_.size(); ++i){
            proactors_[i] = std::make_shared<UringProactor>(i, 4096);
        }


        for(std::size_t i = 0;i < proactors_.size(); ++i){
            threads_.emplace_back();
        }
    }

    void AsyncLoop() {

        for(std::size_t i = 0;i < proactors_.size(); ++i){
            threads_[i] = std::make_unique<util::Thread>([this, i]{
                proactors_[i]->loop();
            });            
        }
    }

    void stop() {


        DispatchBrief([this](UringProactorPtr p){
            p->stop();
        });

        for(std::size_t i = 0;i < proactors_.size(); ++i){
            threads_[i]->join();           
        }        
    }

    size_t size() const { return proactors_.size(); }

    template <typename Func> 
    void DispatchBrief(Func&& f){
        for (std::size_t i = 0; i < size(); ++i) {
            auto& p = proactors_[i];

            p->DispatchBrief([p, f]() mutable { f(p); });
        }        
    }    
    template <typename Func>
    void AwaitOnAll(Func&& func) {
        std::latch latch(size());
        auto cb = [func = std::forward<Func>(func), &latch](UringProactorPtr p) mutable {
            func(p);
            latch.count_down();
        };
        DispatchBrief(std::move(cb));
        latch.wait();
    }


    auto at(size_t index) const { return proactors_[index]; }

    auto operator[](size_t index) const { return at(index); }

private:
    std::vector<std::shared_ptr<UringProactor>> proactors_;
    std::vector<std::unique_ptr<util::Thread>> threads_;
};


}