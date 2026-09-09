/* A canned engine so the Go shell compiles and its plumbing tests run before
 * rs/ffi lands. No execution: parse hashes the bytes, verify/accept/reject
 * succeed, build echoes the candidate list as the block, accounts are nonce
 * 0 with a fixed balance. The test preloads header/rpc/block bytes through
 * epochdb_stub_set. Build with -tags epochdb_stub. */
#include <stdlib.h>
#include <string.h>
#include "epochdb_engine.h"

#define MAXBLK 4096
typedef struct { uint8_t id[32], parent[32]; uint64_t height; uint8_t *raw; size_t len; } blk;

struct epochdb_engine {
  blk blocks[MAXBLK]; int nblocks;
  uint8_t accepted[MAXBLK][32]; uint64_t height; /* accepted[h] = id at height h */
  uint32_t state;
};

static uint8_t *g_header; static size_t g_header_len;
static uint8_t *g_rpc; static size_t g_rpc_len;

static void hash32(const uint8_t *p, size_t n, uint8_t out[32]) {
  for (int lane = 0; lane < 4; lane++) {
    uint64_t h = 1469598103934665603ULL ^ (uint64_t)(lane * 0x9E3779B97F4A7C15ULL);
    for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 1099511628211ULL; }
    for (int b = 0; b < 8; b++) out[lane * 8 + b] = (uint8_t)(h >> (56 - 8 * b));
  }
}
static epochdb_buf dup(const uint8_t *p, size_t n) {
  epochdb_buf b = { malloc(n ? n : 1), n }; if (n) memcpy(b.ptr, p, n); return b;
}
static blk *find(epochdb_engine *e, const uint8_t id[32]) {
  for (int i = 0; i < e->nblocks; i++) if (!memcmp(e->blocks[i].id, id, 32)) return &e->blocks[i];
  return NULL;
}
static blk *add(epochdb_engine *e, const uint8_t *raw, size_t len, const uint8_t parent[32], uint64_t height) {
  uint8_t id[32]; hash32(raw, len, id);
  blk *b = find(e, id); if (b) return b;
  if (e->nblocks >= MAXBLK) return NULL;
  b = &e->blocks[e->nblocks++];
  memcpy(b->id, id, 32); memcpy(b->parent, parent, 32); b->height = height;
  b->raw = malloc(len ? len : 1); memcpy(b->raw, raw, len); b->len = len;
  return b;
}

epochdb_engine *epochdb_open(const uint8_t *d, size_t dl, const uint8_t *g, size_t gl,
                             const uint8_t *u, size_t ul, const uint8_t *c, size_t cl,
                             const uint8_t chain_id[32], const uint8_t subnet_id[32],
                             uint32_t network_id, epochdb_buf *err) {
  (void)d; (void)dl; (void)u; (void)ul; (void)c; (void)cl; (void)subnet_id; (void)network_id;
  if (gl == 0) { *err = dup((const uint8_t *)"stub: empty genesis", 19); return NULL; }
  epochdb_engine *e = calloc(1, sizeof *e);
  memcpy(e->accepted[0], chain_id, 32); /* genesis id: canned = the chain id */
  return e;
}
void epochdb_close(epochdb_engine *e) {
  for (int i = 0; i < e->nblocks; i++) free(e->blocks[i].raw);
  free(e);
}
int epochdb_set_state(epochdb_engine *e, uint32_t s) { e->state = s; return 0; }
int epochdb_parse(epochdb_engine *e, const uint8_t *p, size_t n, epochdb_block_meta *out) {
  blk *b = add(e, p, n, e->accepted[e->height], e->height + 1);
  if (!b) return -1;
  memcpy(out->id, b->id, 32); memcpy(out->parent, b->parent, 32);
  out->height = b->height; out->timestamp = 0;
  return 0;
}
int epochdb_verify(epochdb_engine *e, const uint8_t id[32], uint64_t pch, epochdb_verify_out *out) {
  (void)pch; blk *b = find(e, id); if (!b) return -2;
  memset(out, 0, sizeof *out); out->tx_count = 0; return 0;
}
int epochdb_accept(epochdb_engine *e, const uint8_t id[32]) {
  blk *b = find(e, id); if (!b || e->height + 1 >= MAXBLK) return -2;
  e->height++; memcpy(e->accepted[e->height], id, 32); return 0;
}
int epochdb_reject(epochdb_engine *e, const uint8_t id[32]) { return find(e, id) ? 0 : -2; }
int epochdb_last_accepted(epochdb_engine *e, uint8_t id[32], uint64_t *h) {
  memcpy(id, e->accepted[e->height], 32); *h = e->height; return 0;
}
int epochdb_block_id_at_height(epochdb_engine *e, uint64_t h, uint8_t id[32]) {
  if (h > e->height) return -3; memcpy(id, e->accepted[h], 32); return 0;
}
int epochdb_get_block(epochdb_engine *e, const uint8_t id[32], epochdb_buf *out) {
  blk *b = find(e, id); if (!b) return -3; *out = dup(b->raw, b->len); return 0;
}
int epochdb_build(epochdb_engine *e, const uint8_t parent[32], uint64_t ts, const uint8_t cb[20],
                  uint64_t pch, const uint8_t *txs, size_t n, const uint8_t *senders, size_t senders_len, epochdb_build_out *out) {
  (void)ts; (void)cb; (void)pch; (void)senders; (void)senders_len;
  blk *p = find(e, parent); uint64_t h = p ? p->height + 1 : e->height + 1;
  blk *b = add(e, txs, n, parent, h); if (!b) return -1;
  memset(out, 0, sizeof *out);
  out->block_bytes = dup(b->raw, b->len); memcpy(out->id, b->id, 32);
  out->included_count = n > 2 ? 1 : 0; /* an empty RLP list is 1 byte */
  out->skipped = dup(NULL, 0);
  return 0;
}
int epochdb_account_state(epochdb_engine *e, const uint8_t *addrs, size_t n, const uint8_t bid[32], epochdb_buf *out) {
  (void)e; (void)addrs; (void)bid;
  out->len = 40 * n; out->ptr = calloc(out->len ? out->len : 1, 1);
  for (size_t i = 0; i < n; i++) { out->ptr[i * 40 + 8 + 31 - 12] = 0x01; } /* balance 2^96 wei, nonce 0 */
  return 0;
}
int epochdb_head_header(epochdb_engine *e, epochdb_buf *out) {
  (void)e; if (!g_header) return -4; *out = dup(g_header, g_header_len); return 0;
}
int epochdb_rpc(epochdb_engine *e, const uint8_t *body, size_t n, epochdb_buf *out) {
  (void)e; if (g_rpc) { *out = dup(g_rpc, g_rpc_len); return 0; }
  *out = dup(body, n); return 0; /* echo */
}
int epochdb_health(epochdb_engine *e, epochdb_buf *out) {
  (void)e; *out = dup((const uint8_t *)"{\"stub\":true}", 13); return 0;
}
int epochdb_last_error(epochdb_engine *e, epochdb_buf *out) {
  (void)e; *out = dup((const uint8_t *)"stub error", 10); return 0;
}
void epochdb_buf_free(epochdb_buf *b) { free(b->ptr); b->ptr = NULL; b->len = 0; }

/* test hooks (process-global, applied to every engine): kind 0 = head header RLP, 1 = canned rpc response */
void epochdb_stub_set(int kind, const uint8_t *p, size_t n) {
  uint8_t **dst = kind == 0 ? &g_header : &g_rpc; size_t *len = kind == 0 ? &g_header_len : &g_rpc_len;
  free(*dst); *dst = malloc(n ? n : 1); memcpy(*dst, p, n); *len = n;
}
