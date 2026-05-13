#include <stdint.h>
#include <string.h>

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k) {
    uint64_t v0 = 0x736f6d6570736575ULL;
    uint64_t v1 = 0x646f72616e646f6dULL;
    uint64_t v2 = 0x6c7967656e657261ULL;
    uint64_t v3 = 0x7465646279746573ULL;

    uint64_t k0 = *(uint64_t *)k;
    uint64_t k1 = *(uint64_t *)(k + 8);

    v3 ^= k1;
    v2 ^= k0;
    v1 ^= k1;
    v0 ^= k0;

    const uint8_t *end = in + inlen - (inlen % sizeof(uint64_t));
    for (; in < end; in += 8) {
        uint64_t m = *(uint64_t *)in;
        v3 ^= m;

        for (int i = 0; i < 2; i++) {
            v0 += v1; v1 = v1 << 13; v1 ^= v0; v0 = v0 << 32;
            v2 += v3; v3 = v3 << 16; v3 ^= v2;
            v0 += v3; v3 = v3 << 21; v3 ^= v0;
            v2 += v1; v1 = v1 << 17; v1 ^= v2; v0 = v0 << 32;
        }

        v0 ^= m;
    }

    uint64_t b = ((uint64_t)inlen) << 56;
    switch (inlen & 7) {
        case 7: b |= (uint64_t)in[6] << 48; /* fallthrough */
        case 6: b |= (uint64_t)in[5] << 40; /* fallthrough */
        case 5: b |= (uint64_t)in[4] << 32; /* fallthrough */
        case 4: b |= (uint64_t)in[3] << 24; /* fallthrough */
        case 3: b |= (uint64_t)in[2] << 16; /* fallthrough */
        case 2: b |= (uint64_t)in[1] << 8;  /* fallthrough */
        case 1: b |= (uint64_t)in[0];
    }

    v3 ^= b;

    for (int i = 0; i < 2; i++) {
        v0 += v1; v1 = v1 << 13; v1 ^= v0; v0 = v0 << 32;
        v2 += v3; v3 = v3 << 16; v3 ^= v2;
        v0 += v3; v3 = v3 << 21; v3 ^= v0;
        v2 += v1; v1 = v1 << 17; v1 ^= v2; v0 = v0 << 32;
    }

    v0 ^= b;
    v2 ^= 0xff;

    for (int i = 0; i < 4; i++) {
        v0 += v1; v1 = v1 << 13; v1 ^= v0; v0 = v0 << 32;
        v2 += v3; v3 = v3 << 16; v3 ^= v2;
        v0 += v3; v3 = v3 << 21; v3 ^= v0;
        v2 += v1; v1 = v1 << 17; v1 ^= v2; v0 = v0 << 32;
    }

    return (v0 ^ v1) ^ (v2 ^ v3);
}