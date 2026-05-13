#ifndef __ZMALLOC_HPP
#define __ZMALLOC_HPP

#include <stddef.h>
#include <stdint.h>

void *zmalloc(size_t size);
void *zcalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void zfree(void *ptr);
char *zstrdup(const char *s);
size_t zmalloc_usable_size(const void *p);

#endif /* __ZMALLOC_HPP */