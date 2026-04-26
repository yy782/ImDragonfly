#pragma once

namespace dfly::cmd {

struct CmdR {
    struct Coro;
    using promise_type = Coro;
};


}

