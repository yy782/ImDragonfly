#include <stdlib.h>
#include <string.h>
#include "zmalloc.hpp"

void *zmalloc(size_t size) {
    return malloc(size);
}

void *zcalloc(size_t size) {
    void *ptr = malloc(size);
    if (ptr) memset(ptr, 0, size);
    return ptr;
}

void *zrealloc(void *ptr, size_t size) {
    return realloc(ptr, size);
}

void zfree(void *ptr) {
    free(ptr);
}

char *zstrdup(const char *s) {
    size_t len = strlen(s) + 1;
    char *dup = static_cast<char*>(malloc(len));
    if (dup) memcpy(dup, s, len);
    return dup;
}

size_t zmalloc_usable_size(const void *p) {
    (void)p;
    return 0;
}