#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include "dict.hpp"
#include "zmalloc.hpp"

extern uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);

static uint8_t dict_hash_function_seed[16] = {0};

uint64_t dictGenHashFunction(const void *key, size_t len) {
    return siphash(reinterpret_cast<const uint8_t*>(key), len, dict_hash_function_seed);
}

static int _dictExpandIfNeeded(dict *d);
static signed char _dictNextExp(unsigned long size);
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing);
static int _dictInit(dict *d, dictType *type);

static void _dictReset(dict *d, int htidx) {
    d->ht_table[htidx] = NULL;
    d->ht_used[htidx] = 0;
    d->ht_size_exp[htidx] = -1;
}

int _dictInit(dict *d, dictType *type) {
    _dictReset(d, 0);
    _dictReset(d, 1);
    d->type = type;
    d->rehashidx = -1;
    d->pauserehash = 0;
    return DICT_OK;
}

dict *dictCreate(dictType *type) {
    dict *d = static_cast<dict*>(zmalloc(sizeof(*d)));
    if (!d) return NULL;
    _dictInit(d, type);
    return d;
}

static signed char _dictNextExp(unsigned long size) {
    unsigned long i = DICT_HT_INITIAL_SIZE;
    if (size >= LONG_MAX) return LONG_MAX;
    while (1) {
        if (i >= size) return i;
        i *= 2;
    }
}

int dictExpand(dict *d, unsigned long size) {
    unsigned long realsize = _dictNextExp(size);
    if (dictSize(d) > 0 || realsize == DICTHT_SIZE(d->ht_size_exp[0])) {
        return DICT_ERR;
    }

    dictEntry **new_ht = static_cast<dictEntry**>(zcalloc(realsize * sizeof(dictEntry *)));
    if (!new_ht) return DICT_ERR;

    d->ht_table[0] = new_ht;
    d->ht_size_exp[0] = realsize == 0 ? -1 : __builtin_ctzll(realsize);
    d->ht_used[0] = 0;
    return DICT_OK;
}

static int _dictExpandIfNeeded(dict *d) {
    if (d->ht_table[0] == NULL) {
        return dictExpand(d, DICT_HT_INITIAL_SIZE);
    }
    if ((double)d->ht_used[0] / DICTHT_SIZE(d->ht_size_exp[0]) > 1.0) {
        return dictExpand(d, d->ht_used[0] * 2);
    }
    return DICT_OK;
}

static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing) {
    unsigned long idx, table;
    dictEntry *he;

    if (_dictExpandIfNeeded(d) == DICT_ERR) return -1;

    idx = hash & DICTHT_SIZE_MASK(d->ht_size_exp[0]);
    table = 0;

    he = d->ht_table[table][idx];
    while (he) {
        if (d->type->keyCompare(d, key, he->key)) {
            if (existing) *existing = he;
            return -1;
        }
        he = he->next;
    }
    return idx;
}

int dictAdd(dict *d, void *key, void *val) {
    dictEntry *entry = static_cast<dictEntry*>(zmalloc(sizeof(*entry)));
    if (!entry) return DICT_ERR;

    if (d->type->keyDup) {
        key = d->type->keyDup(d, key);
        if (!key) {
            zfree(entry);
            return DICT_ERR;
        }
    }
    entry->key = key;

    if (d->type->valDup) {
        val = d->type->valDup(d, val);
        if (!val) {
            if (d->type->keyDestructor) d->type->keyDestructor(d, entry->key);
            zfree(entry);
            return DICT_ERR;
        }
    }
    entry->v.val = val;
    entry->next = NULL;

    long index = _dictKeyIndex(d, key, dictGenHashFunction(key, sizeof(uint64_t)), NULL);
    if (index == -1) {
        if (d->type->keyDestructor) d->type->keyDestructor(d, entry->key);
        if (d->type->valDestructor) d->type->valDestructor(d, entry->v.val);
        zfree(entry);
        return DICT_ERR;
    }

    entry->next = d->ht_table[0][index];
    d->ht_table[0][index] = entry;
    d->ht_used[0]++;
    return DICT_OK;
}

dictEntry *dictFind(dict *d, const void *key) {
    dictEntry *he;
    unsigned long idx, hash;

    if (d->ht_used[0] == 0) return NULL;

    hash = dictGenHashFunction(key, sizeof(uint64_t));
    idx = hash & DICTHT_SIZE_MASK(d->ht_size_exp[0]);
    he = d->ht_table[0][idx];
    while (he) {
        if (d->type->keyCompare(d, key, he->key)) {
            return he;
        }
        he = he->next;
    }
    return NULL;
}

int dictDelete(dict *d, const void *key) {
    dictEntry *he, **prevHe;
    unsigned long idx, hash;

    if (d->ht_used[0] == 0) return DICT_ERR;

    hash = dictGenHashFunction(key, sizeof(uint64_t));
    idx = hash & DICTHT_SIZE_MASK(d->ht_size_exp[0]);
    he = d->ht_table[0][idx];
    prevHe = &d->ht_table[0][idx];

    while (he) {
        if (d->type->keyCompare(d, key, he->key)) {
            *prevHe = he->next;
            if (d->type->keyDestructor) d->type->keyDestructor(d, he->key);
            if (d->type->valDestructor) d->type->valDestructor(d, he->v.val);
            zfree(he);
            d->ht_used[0]--;
            return DICT_OK;
        }
        prevHe = &he->next;
        he = he->next;
    }
    return DICT_ERR;
}

void dictEmpty(dict *d, void(callback)(dict*)) {
    (void)callback;
    unsigned long i;
    dictEntry *he, *nextHe;

    for (i = 0; i < DICTHT_SIZE(d->ht_size_exp[0]); i++) {
        he = d->ht_table[0][i];
        while (he) {
            nextHe = he->next;
            if (d->type->keyDestructor) d->type->keyDestructor(d, he->key);
            if (d->type->valDestructor) d->type->valDestructor(d, he->v.val);
            zfree(he);
            he = nextHe;
        }
    }
    zfree(d->ht_table[0]);
    _dictReset(d, 0);
}

void dictRelease(dict *d) {
    dictEmpty(d, NULL);
    zfree(d);
}