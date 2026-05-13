#ifndef __DICT_HPP
#define __DICT_HPP

#include <stdint.h>
#include <stdlib.h>

#define DICT_OK 0
#define DICT_ERR 1

typedef struct dictEntry {
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictEntry *next;
} dictEntry;

typedef struct dict dict;

typedef struct dictType {
    uint64_t (*hashFunction)(const void *key);
    void *(*keyDup)(dict *d, const void *key);
    void *(*valDup)(dict *d, const void *obj);
    int (*keyCompare)(dict *d, const void *key1, const void *key2);
    void (*keyDestructor)(dict *d, void *key);
    void (*valDestructor)(dict *d, void *obj);
} dictType;

#define DICTHT_SIZE(exp) ((exp) == -1 ? 0 : (unsigned long)1<<(exp))
#define DICTHT_SIZE_MASK(exp) ((exp) == -1 ? 0 : (DICTHT_SIZE(exp))-1)

struct dict {
    dictType *type;
    dictEntry **ht_table[2];
    unsigned long ht_used[2];
    long rehashidx;
    int16_t pauserehash;
    signed char ht_size_exp[2];
};

typedef struct dictIterator {
    dict *d;
    long index;
    int table, safe;
    dictEntry *entry, *nextEntry;
} dictIterator;

#define DICT_HT_INITIAL_EXP 2
#define DICT_HT_INITIAL_SIZE (1<<(DICT_HT_INITIAL_EXP))

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetKey(he) ((he)->key)
#define dictSize(d) ((d)->ht_used[0]+(d)->ht_used[1])

uint64_t dictGenHashFunction(const void *key, size_t len);

dict *dictCreate(dictType *type);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictFind(dict *d, const void *key);
int dictDelete(dict *d, const void *key);
void dictRelease(dict *d);
void dictEmpty(dict *d, void(callback)(dict*));

#endif /* __DICT_HPP */