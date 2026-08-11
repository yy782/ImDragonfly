// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "engine_shard_set.hpp"

#include <glog/logging.h>

#include <functional>

#include "detail/common.hpp"
#include "namespaces.hpp"
#include "util/maths.hpp"

namespace dfly {

EngineShardSet* shard_set = nullptr;

void EngineShardSet::Init(uint32_t sz) {
  LOG(INFO) << "Initializing EngineShardSet with " << sz << " shards";
  shards_.reset(new EngineShard*[sz]);
  size_ = sz;

  pp_->AwaitOnAll([this](std::shared_ptr<base::UringProactor> pb) {
    InitThreadLocal(pb.get());
  });

  namespaces = new Namespaces();
  namespaces->init();
  LOG(INFO) << "EngineShardSet initialized with " << sz
            << " shards and namespace support";
}

void EngineShardSet::PreShutdown() {}

void EngineShardSet::Shutdown() {
  namespaces->Clear();
  RunBlockingInParallel(
      [](EngineShard*) { EngineShard::DestroyThreadLocal(); });
  delete namespaces;
  namespaces = nullptr;
}

void EngineShardSet::InitThreadLocal(base::UringProactor* pb) {
  EngineShard::InitThreadLocal(pb);
  EngineShard* es = EngineShard::tlocal();
  shards_[es->shard_id()] = es;
  util::Thread::set_cpu_affinity(es->shard_id());
}

}  // namespace dfly