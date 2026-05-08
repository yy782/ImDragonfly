// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once


#include <optional>
#include <string_view>
#include <utility>
#include <charconv>

#include "util/Strings.hpp"
#include "cmn_types.hpp"
namespace cmd {
using ::cmn::ArgSlice;
class CmdArgParser {
public:
    CmdArgParser(ArgSlice args) : args_{args} {
    }
    ~CmdArgParser()=default;

    std::string_view Peek() {
      return SafeSV(cur_i_);
    }


    template <class T = std::string_view, class... Ts> 
    auto Next() {
        if (cur_i_ + sizeof...(Ts) >= args_.size()) {
            Report();
            return std::conditional_t<sizeof...(Ts) == 0, T, std::tuple<T, Ts...>>();
        }

        if constexpr (sizeof...(Ts) == 0) {
            auto idx = cur_i_++;
            return Convert<T>(idx);
        } else {
            std::tuple<T, Ts...> res;
            NextImpl<0>(&res);
            cur_i_ += sizeof...(Ts) + 1;
          return res;
        }
    }


    template <class T = std::string_view> 
    auto NextOrDefault(T default_value = {}) {
        return HasNext() ? Next<T>() : default_value;
    }


    // void ExpectTag(std::string_view tag);


    template <class... Cases> 
    auto MapNext(Cases&&... cases) {
      if (cur_i_ >= args_.size()) {
        Report();
        return typename decltype(MapImpl(std::string_view(),
                                        std::forward<Cases>(cases)...))::value_type{};
      }

      auto idx = cur_i_++;
      auto res = MapImpl(SafeSV(idx), std::forward<Cases>(cases)...);
      if (!res) {
        Report();
        return typename decltype(res)::value_type{};
      }
      return *res;
    }


    template <class... Cases>
    auto TryMapNext(Cases&&... cases)
        -> std::optional<std::tuple_element_t<1, std::tuple<Cases...>>> {
      if (cur_i_ >= args_.size()) {
        return std::nullopt;
      }

      auto res = MapImpl(SafeSV(cur_i_), std::forward<Cases>(cases)...);
      cur_i_ = res ? cur_i_ + 1 : cur_i_;
      return res;
    }

    bool Check(std::string_view tag) {
        if (cur_i_  >= args_.size())
            return false;

        std::string_view arg = SafeSV(cur_i_);
        if (!util::EqualsIgnoreCaseStd(arg, tag))
            return false;

        ++cur_i_;

        return true;
    }

    // Skip specified number of arguments
    CmdArgParser& Skip(size_t n) {
      if (cur_i_ + n > args_.size()) {
        Report();
      } else {
        cur_i_ += n;
      }
      return *this;
    }


  // Return remaining arguments
    ::cmn::ArgSlice Tail() const {
      return args_.subspan(cur_i_);
    }

    bool HasNext() {
      return cur_i_ < args_.size() && !error_;
    }

    bool HasError() const {
      return error_==HAS_ERROR;
    }


    bool HasAtLeast(size_t i) const {
      return !error_ && i <= args_.size() - cur_i_;
    }

    size_t GetCurrentIndex() const {
      return cur_i_;
    }

    void Report() { 
        error_=HAS_ERROR;
        cur_i_=args_.size();
    }

private:

    template <class T, class... Cases>
    std::optional<std::decay_t<T>> MapImpl(std::string_view arg, std::string_view tag, T&& value,
                                          Cases&&... cases) {
      if (util::EqualsIgnoreCaseStd(arg, tag))
        return std::forward<T>(value);

      if constexpr (sizeof...(cases) > 0)
        return MapImpl(arg, cases...);

      return std::nullopt;
    }

    template <size_t shift, class Tuple> 
    void NextImpl(Tuple* t) {
      std::get<shift>(*t) = Convert<std::tuple_element_t<shift, Tuple>>(cur_i_ + shift);
      if constexpr (constexpr auto next = shift + 1; next < std::tuple_size_v<Tuple>)
        NextImpl<next>(t);
    }

    template <class T> 
    T Convert(size_t idx) {
      if constexpr (std::is_arithmetic_v<T>) {
        return Num<T>(idx);
      } else if constexpr (std::is_constructible_v<T, std::string_view>) {
        return static_cast<T>(SafeSV(idx));
      } 
    }

    std::string_view SafeSV(size_t i) const {
      using namespace std::literals::string_view_literals;
      if (i >= args_.size())
        return ""sv;
      return args_[i].empty() ? ""sv : args_[i];
    }

    
    int64_t Num(size_t idx) {
      std::string_view arg = SafeSV(idx);
      int64_t out;

      auto result = std::from_chars(arg.data(), arg.data()+arg.size(), out);

      if(result.ec == std::errc() && result.ptr == arg.data() + arg.size()){
        return out;
      }
      Report();
      return {};
    }

private:
    enum ErrorType {
        NO_ERROR,
        HAS_ERROR
    }; 
    size_t cur_i_ = 0;
    ArgSlice args_;
    ErrorType error_=NO_ERROR;
};


}  // namespace facade
