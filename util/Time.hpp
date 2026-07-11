#pragma  once

#include <time.h>

namespace util{

inline uint64_t GetCurrentTimeMs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;
}


}