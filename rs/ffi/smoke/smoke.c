/* Smoke test of the epochdb engine C ABI: open on a genesis, parse + verify +
 * accept N inner blocks from an export file ([u32 LE len][inner block RLP]
 * records, vbench --export-inner), build block N+1 from its own txs and
 * compare with the real bytes, read an account, call eth_blockNumber, close,
 * reopen, check last_accepted.
 *
 *   smoke <data_dir> <genesis.json> <upgrade.json> <inner.bin> <n> <alloc_addr_hex40>
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include "../epochdb_engine.h"

static uint8_t *read_file(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); exit(2); }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *b = malloc(n > 0 ? n : 1);
    if (fread(b, 1, n, f) != (size_t)n) { perror("read"); exit(2); }
    fclose(f);
    *len = n;
    return b;
}

static void die(epochdb_engine *e, const char *what, int rc) {
    epochdb_buf err = {0};
    if (e) epochdb_last_error(e, &err);
    fprintf(stderr, "smoke: %s failed: rc=%d %.*s\n", what, rc, (int)err.len, err.ptr ? (char *)err.ptr : "");
    epochdb_buf_free(&err);
    exit(1);
}

/* RLP: the header length of the item at p (bytes of the length prefix) and its payload length. */
static size_t rlp_head(const uint8_t *p, size_t *payload, int *is_list) {
    uint8_t b = p[0];
    if (b < 0x80) { *payload = 1; *is_list = 0; return 0; }
    if (b < 0xb8) { *payload = b - 0x80; *is_list = 0; return 1; }
    if (b < 0xc0) { size_t n = b - 0xb7, l = 0; for (size_t i = 0; i < n; i++) l = (l << 8) | p[1 + i]; *payload = l; *is_list = 0; return 1 + n; }
    if (b < 0xf8) { *payload = b - 0xc0; *is_list = 1; return 1; }
    size_t n = b - 0xf7, l = 0; for (size_t i = 0; i < n; i++) l = (l << 8) | p[1 + i]; *payload = l; *is_list = 1; return 1 + n;
}

/* The tx list element of an inner block [header, txs, uncles]. */
static const uint8_t *block_txs(const uint8_t *blk, size_t *len) {
    size_t pl; int l;
    size_t h = rlp_head(blk, &pl, &l);
    const uint8_t *p = blk + h;               /* header element */
    h = rlp_head(p, &pl, &l);
    p += h + pl;                              /* txs element */
    h = rlp_head(p, &pl, &l);
    *len = h + pl;
    return p;
}

static int hexval(char c) { return c <= '9' ? c - '0' : (c | 32) - 'a' + 10; }

static double ms_since(struct timespec *t0) {
    struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
    return (t1.tv_sec - t0->tv_sec) * 1e3 + (t1.tv_nsec - t0->tv_nsec) / 1e6;
}

static epochdb_engine *open_engine(const char *data, uint8_t *g, size_t gl, uint8_t *u, size_t ul) {
    static const char *cfg = "{\"roll-budget-mb\":8}";
    uint8_t chain_id[32], subnet_id[32];
    memset(chain_id, 0xc4, 32); memset(subnet_id, 0x5b, 32);
    epochdb_buf err = {0};
    epochdb_engine *e = epochdb_open((const uint8_t *)data, strlen(data), g, gl, u, ul, (const uint8_t *)cfg, strlen(cfg), chain_id, subnet_id, 1, &err);
    if (!e) { fprintf(stderr, "smoke: open failed: %.*s\n", (int)err.len, (char *)err.ptr); epochdb_buf_free(&err); exit(1); }
    return e;
}

int main(int argc, char **argv) {
    if (argc < 7) { fprintf(stderr, "usage: smoke data genesis.json upgrade.json inner.bin n alloc_addr\n"); return 2; }
    const char *data = argv[1];
    size_t gl, ul, il;
    uint8_t *g = read_file(argv[2], &gl), *u = read_file(argv[3], &ul), *inner = read_file(argv[4], &il);
    uint64_t n = strtoull(argv[5], 0, 10);
    uint8_t addr[20];
    for (int i = 0; i < 20; i++) addr[i] = hexval(argv[6][2 * i]) * 16 + hexval(argv[6][2 * i + 1]);

    epochdb_engine *e = open_engine(data, g, gl, u, ul);
    int rc = epochdb_set_state(e, 2);
    if (rc) die(e, "set_state", rc);

    epochdb_buf hh = {0};
    if ((rc = epochdb_head_header(e, &hh))) die(e, "head_header", rc);
    printf("smoke: genesis header %zu bytes\n", hh.len);
    epochdb_buf_free(&hh);

    /* Walk the export: [u32 LE len][inner]. */
    size_t off = 0;
    uint64_t h = 0;
    double t_verify = 0, t_verify_max = 0;
    uint8_t last_id[32] = {0};
    const uint8_t *next_blk = 0; size_t next_len = 0;
    while (off + 4 <= il) {
        uint32_t len; memcpy(&len, inner + off, 4); off += 4;
        const uint8_t *blk = inner + off; off += len;
        if (h == n) { next_blk = blk; next_len = len; break; }
        epochdb_block_meta m;
        if ((rc = epochdb_parse(e, blk, len, &m))) die(e, "parse", rc);
        if (m.height != h + 1) { fprintf(stderr, "smoke: block %llu out of order (want %llu)\n", (unsigned long long)m.height, (unsigned long long)h + 1); return 1; }
        struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
        epochdb_verify_out vo;
        if ((rc = epochdb_verify(e, m.id, 0, &vo))) die(e, "verify", rc);
        double d = ms_since(&t0); t_verify += d; if (d > t_verify_max) t_verify_max = d;
        if ((rc = epochdb_accept(e, m.id))) die(e, "accept", rc);
        memcpy(last_id, m.id, 32);
        h = m.height;
    }
    printf("smoke: verified+accepted %llu blocks, verify mean %.3f ms max %.3f ms\n", (unsigned long long)h, t_verify / (h ? h : 1), t_verify_max);
    if (!next_blk) { fprintf(stderr, "smoke: export has no block %llu\n", (unsigned long long)n + 1); return 1; }

    /* Build block n+1 from its own txs on the head, compare with the real bytes. */
    epochdb_block_meta real;
    if ((rc = epochdb_parse(e, next_blk, next_len, &real))) die(e, "parse next", rc);
    size_t txs_len; const uint8_t *txs = block_txs(next_blk, &txs_len);
    /* coinbase and timestamp are the real header's: fields 3 (coinbase) and 12 (time) of the header list. */
    size_t pl; int l; size_t hh0 = rlp_head(next_blk, &pl, &l);
    const uint8_t *hp = next_blk + hh0; size_t hph = rlp_head(hp, &pl, &l); const uint8_t *f = hp + hph;
    uint8_t coinbase[20]; uint64_t ts = 0;
    for (int i = 0; i < 12; i++) {
        size_t fl; int fli; size_t fh = rlp_head(f, &fl, &fli);
        if (i == 2) memcpy(coinbase, f + fh, 20);
        if (i == 11) { ts = 0; if (fh == 0) ts = f[0]; else for (size_t k = 0; k < fl; k++) ts = (ts << 8) | f[fh + k]; }
        f += fh + fl;
    }
    epochdb_build_out bo;
    struct timespec tb; clock_gettime(CLOCK_MONOTONIC, &tb);
    if ((rc = epochdb_build(e, last_id, ts * 1000, coinbase, 0, txs, txs_len, &bo))) die(e, "build", rc);
    double build_ms = ms_since(&tb);
    int same = bo.block_bytes.len == next_len && memcmp(bo.block_bytes.ptr, next_blk, next_len) == 0 && memcmp(bo.id, real.id, 32) == 0;
    printf("smoke: built block %llu: %zu bytes, %llu txs included, %s, %.3f ms\n", (unsigned long long)real.height, bo.block_bytes.len, (unsigned long long)bo.included_count, same ? "byte-identical to the real block" : "DIFFERS from the real block", build_ms);
    epochdb_buf_free(&bo.block_bytes);
    epochdb_buf_free(&bo.skipped);
    if (!same) return 1;
    /* Its verify is a lookup now; accept it. */
    epochdb_verify_out vo;
    if ((rc = epochdb_verify(e, real.id, 0, &vo))) die(e, "verify built", rc);
    if ((rc = epochdb_accept(e, real.id))) die(e, "accept built", rc);
    h = real.height;

    epochdb_buf acct = {0};
    uint8_t zero[32] = {0};
    if ((rc = epochdb_account_state(e, addr, 1, zero, &acct))) die(e, "account_state", rc);
    uint64_t nonce; memcpy(&nonce, acct.ptr, 8);
    printf("smoke: alloc account nonce %llu balance 0x", (unsigned long long)nonce);
    for (int i = 8; i < 40; i++) printf("%02x", acct.ptr[i]);
    printf("\n");
    epochdb_buf_free(&acct);

    const char *req = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}";
    epochdb_buf resp = {0};
    if ((rc = epochdb_rpc(e, (const uint8_t *)req, strlen(req), &resp))) die(e, "rpc", rc);
    printf("smoke: eth_blockNumber -> %.*s\n", (int)resp.len, (char *)resp.ptr);
    epochdb_buf_free(&resp);

    epochdb_buf health = {0};
    if ((rc = epochdb_health(e, &health))) die(e, "health", rc);
    printf("smoke: health %.*s\n", (int)health.len, (char *)health.ptr);
    epochdb_buf_free(&health);

    /* An unknown id is ENOTFOUND, not a crash. */
    uint8_t bogus[32]; memset(bogus, 0xee, 32);
    epochdb_verify_out vo2;
    rc = epochdb_verify(e, bogus, 0, &vo2);
    if (rc != EPOCHDB_ENOTFOUND) { fprintf(stderr, "smoke: verify of an unknown id returned %d\n", rc); return 1; }

    epochdb_close(e);
    e = open_engine(data, g, gl, u, ul);
    uint8_t id[32]; uint64_t height = 0;
    if ((rc = epochdb_last_accepted(e, id, &height))) die(e, "last_accepted", rc);
    printf("smoke: reopened, last accepted height %llu (%s)\n", (unsigned long long)height, height == h && memcmp(id, real.id, 32) == 0 ? "ok" : "MISMATCH");
    if (height != h) return 1;
    epochdb_buf blk = {0};
    if ((rc = epochdb_get_block(e, id, &blk))) die(e, "get_block", rc);
    printf("smoke: get_block(head) %zu bytes %s\n", blk.len, blk.len == next_len && memcmp(blk.ptr, next_blk, next_len) == 0 ? "== real" : "DIFFERS");
    epochdb_buf_free(&blk);
    epochdb_close(e);
    free(g); free(u); free(inner);
    printf("smoke: ok\n");
    return 0;
}
