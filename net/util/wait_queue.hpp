// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/list.hpp>
#include <functional>
#include <variant>
#include <coroutine>
namespace util {
namespace detail {


struct Waiter{
    std::coroutine_handle<> handler;
    using ListHookType =
    boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;
    ListHookType wait_hook;

    bool IsLinked() const {
        return wait_hook.is_linked();
    }

};

class WaitQueue {
public:
    bool empty() const {
      return wait_list_.empty();
    }

    void Link(Waiter* waiter){
        wait_list_.push_back(*waiter);
    }


    void Unlink(Waiter* waiter) {
      auto it = WaitList::s_iterator_to(*waiter);
      wait_list_.erase(it);
    }

    // Return true if a waiter exitsted and was notified
    bool NotifyOne(){
      if (wait_list_.empty())
        return false;

      Waiter* waiter = &wait_list_.front();

      wait_list_.pop_front();

      waiter->handler.resume();
      return true;
    }

    // Return true if any waiter was notified
    bool NotifyAll(){
        bool notified = false;
        auto it = wait_list_.begin();


        while (it != wait_list_.end()) {
            Waiter& waiter = *it;
            it = wait_list_.erase(it);
            waiter.handler.resume();
            notified = true;
        }
        return notified;
    }

private:
    using WaitList = boost::intrusive::list<
        Waiter, boost::intrusive::member_hook<Waiter, Waiter::ListHookType, &Waiter::wait_hook>,
        boost::intrusive::constant_time_size<false>>;

    WaitList wait_list_;
};

}  // namespace detail
}  // namespace util
