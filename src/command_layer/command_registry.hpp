// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <functional>
#include <optional>


#include "command_layer/command_id.hpp"
#include "command_layer/cmn_types.hpp"
#include "util/function.hpp"


namespace dfly {

namespace CO {

enum CommandOpt : uint32_t { // 命令选项枚举

};


} // namespace CO



class CommandId;
class CommandContext;


class CommandId : public facade::CommandId {
public:
    using CmdArgList = ::cmn::CmdArgList;

    CommandId(const char* name, size_t keys_start, size_t keys_nums, size_t keys_offset);

    CommandId(CommandId&& o) = default;

    ~CommandId();

    [[nodiscard]] CommandId Clone(std::string_view name) const;


    using Handler = util::function_base<true, true, fu2::capacity_default, false, false,
                                      void(CommandContext*, CmdArgList) const>;
    void Invoke(CommandContext* cmd_cntx, CmdArgList args) const {
        handler_(cmd_cntx, args);
    }


    CommandId&& SetHandler(Handler f) && {
        handler_ = std::move(f);
        return std::move(*this);
    }


 private:

    Handler handler_;
};

class CommandRegistry {
public:
    CommandRegistry();

    CommandRegistry& operator<<(CommandId cmd);

    const CommandId* Find(std::string_view cmd) const {
        auto it = cmd_map_.find(std::string(cmd));
        return it == cmd_map_.end() ? nullptr : &it->second;
    }

    CommandId* Find(std::string_view cmd) {
        auto it = cmd_map_.find(std::string(cmd));
        return it == cmd_map_.end() ? nullptr : &it->second;
    }

    using TraverseCb = std::function<void(std::string_view, const CommandId&)>;

    void Traverse(TraverseCb cb) {
        for (const auto& k_v : cmd_map_) {
          cb(k_v.first, k_v.second);
        }
    }





    void StartFamily();


    using FamiliesVec = std::vector<std::vector<std::string>>;
    FamiliesVec GetFamilies();

 private:
    std::unordered_map<std::string, CommandId> cmd_map_;

    FamiliesVec family_of_commands_;
    size_t bit_index_;
};

}  // namespace dfly
