// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "command_registry.hpp"

#include <absl/container/inlined_vector.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <hdr/hdr_histogram.h>

#include "base/bits.h"
#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"


namespace dfly {

using namespace facade;

using absl::AsciiStrToUpper;
using absl::GetFlag;
using absl::StrCat;
using absl::StrSplit;

namespace {

// uint32_t ImplicitCategories(uint32_t mask) {
//   if (mask & CO::ADMIN)
//     mask |= CO::NOSCRIPT;
//   return mask;
// }

// uint32_t ImplicitAclCategories(uint32_t mask) {
//   mask = ImplicitCategories(mask);
//   uint32_t out = 0;

//   if (mask & CO::JOURNALED)
//     out |= acl::WRITE;

//   if ((mask & CO::READONLY) && ((mask & CO::NOSCRIPT) == 0))
//     out |= acl::READ;

//   if (mask & CO::ADMIN)
//     out |= acl::ADMIN | acl::DANGEROUS;

//   // todo pubsub

//   if (mask & CO::FAST)
//     out |= acl::FAST;

//   if (mask & CO::BLOCKING)
//     out |= acl::BLOCKING;

//   if ((out & acl::FAST) == 0)
//     out |= acl::SLOW;

//   return out;
// }

using CmdLineMapping = absl::flat_hash_map<std::string, std::string>;

CmdLineMapping ParseCmdlineArgMap(const absl::Flag<std::vector<std::string>>& flag) {
  const auto& mappings = absl::GetFlag(flag);
  CmdLineMapping parsed_mappings;
  parsed_mappings.reserve(mappings.size());

  for (const std::string& mapping : mappings) {
    absl::InlinedVector<std::string_view, 2> kv = absl::StrSplit(mapping, '=');
    if (kv.size() != 2) {
      LOG(ERROR) << "Malformed command '" << mapping << "' for " << flag.Name()
                 << ", expected key=value";
      exit(1);
    }

    std::string key = absl::AsciiStrToUpper(kv[0]);
    std::string value = absl::AsciiStrToUpper(kv[1]);

    if (key == value) {
      LOG(ERROR) << "Invalid attempt to map " << key << " to itself in " << flag.Name();
      exit(1);
    }

    if (!parsed_mappings.emplace(std::move(key), std::move(value)).second) {
      LOG(ERROR) << "Duplicate insert to " << flag.Name() << " not allowed";
      exit(1);
    }
  }
  return parsed_mappings;
}

CmdLineMapping OriginalToAliasMap() {
  CmdLineMapping original_to_alias;
  CmdLineMapping alias_to_original = ParseCmdlineArgMap(FLAGS_command_alias);
  original_to_alias.reserve(alias_to_original.size());
  std::for_each(std::make_move_iterator(alias_to_original.begin()),
                std::make_move_iterator(alias_to_original.end()),
                [&original_to_alias](auto&& pair) {
                  original_to_alias.emplace(std::move(pair.second), std::move(pair.first));
                });

  return original_to_alias;
}

constexpr int64_t kLatencyHistogramMinValue = 1;        // Minimum value in usec
constexpr int64_t kLatencyHistogramMaxValue = 1000000;  // Maximum value in usec (1s)
constexpr int32_t kLatencyHistogramPrecision = 2;

}  // namespace

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key)
    : facade::CommandId(name, mask, arity, first_key, last_key) {
}

CommandId::~CommandId() {
}

CommandId CommandId::Clone(const std::string_view name) const {
    CommandId cloned =
        CommandId{name.data(), opt_mask_, arity_, first_key_, last_key_};
    cloned.handler_ = handler_;
    return cloned;
}


CommandRegistry::CommandRegistry() {
}



CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
    string k = string(cmd.name());
    cmd.SetFamily(family_of_commands_.size() - 1);
    cmd_map_.emplace(k, std::move(cmd));
    return *this;
}

void CommandRegistry::StartFamily(std::optional<uint32_t> acl_category) {
  family_of_commands_.emplace_back();
  bit_index_ = 0;
  acl_category_ = acl_category;
}

std::string_view CommandRegistry::RenamedOrOriginal(std::string_view orig) const {
  if (!cmd_rename_map_.empty() && cmd_rename_map_.contains(orig)) {
    return cmd_rename_map_.find(orig)->second;
  }
  return orig;
}

CommandRegistry::FamiliesVec CommandRegistry::GetFamilies() {
  return std::move(family_of_commands_);
}

std::pair<const CommandId*, ParsedArgs> CommandRegistry::FindExtended(ParsedArgs args) const {
  DCHECK(!args.empty());

  string cmd = absl::AsciiStrToUpper(args.Front());// 提取并转大写
  auto tail_args = args.Tail();

  if (cmd == RenamedOrOriginal("ACL"sv)) { // 特殊处理：ACL 命令
    if (tail_args.empty()) {
      return {Find(cmd), {}};
    }

    auto second_cmd = absl::AsciiStrToUpper(tail_args.Front());
    string full_cmd = StrCat(cmd, " ", second_cmd);

    return {Find(full_cmd), tail_args.Tail()};
  }

  const CommandId* res = Find(cmd);
  if (!res)
    return {nullptr, {}};

  // A workaround for XGROUP HELP that does not fit our static taxonomy of commands.
  if (tail_args.size() == 1 && res->name() == "XGROUP") {
    if (absl::EqualsIgnoreCase(tail_args.Front(), "HELP")) {
      res = Find("_XGROUP_HELP");
    }
  }
  return {res, tail_args};
}

absl::flat_hash_map<std::string, hdr_histogram*> CommandRegistry::LatencyMap() const {
  absl::flat_hash_map<std::string, hdr_histogram*> cmd_latencies;
  cmd_latencies.reserve(cmd_map_.size());
  for (const auto& [cmd_name, cmd] : cmd_map_) {
    cmd_latencies.insert({absl::AsciiStrToLower(cmd_name), cmd.GetLatencyHist()});
  }
  return cmd_latencies;
}

}  // namespace dfly
