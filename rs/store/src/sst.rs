//! Pebblev2 row-block sstables, reader and writer, byte-for-byte as pebble
//! v2.1.6 writes them under store/format.go's writerOptions: restart interval
//! 16, CRC32c, zstd (DataDog/zstd = libzstd 1.5.7) with pebble's 12% minimum
//! reduction rule, table bloom filter, single or two-level index.

use crate::bloom;
use anyhow::{anyhow, bail, Result};
use std::io::Write;
use std::sync::Arc;

pub const TRAILER_LEN: usize = 5;
const FOOTER_LEN: usize = 53;
const MAGIC: &[u8; 8] = b"\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3";
const FORMAT_VERSION: u32 = 2;
const FILTER_META_NAME: &str = "fullfilter.rocksdb.BuiltinBloomFilter";
const PROPS_META_NAME: &str = "rocksdb.properties";
const FILTER_POLICY_NAME: &str = "rocksdb.BuiltinBloomFilter";
const COMPRESSION_OPTIONS: &str =
    "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ";
const RESTART_INTERVAL: usize = 16;
const BLOCK_SIZE_THRESHOLD: usize = 90;
const MIN_REDUCTION_PERCENT: usize = 12;
const ENCODED_BHP_ESTIMATED_SIZE: usize = 20;
const IND_NONE: u8 = 0;
const IND_ZSTD: u8 = 7;
/// InternalKey trailer of a Set at seqnum 0, little-endian.
const SET_TRAILER: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
/// What pebble encodes for InternalKey{} of an empty table's only index entry.
const INVALID_TRAILER: [u8; 8] = [255, 0, 0, 0, 0, 0, 0, 0];

// ---------------------------------------------------------------------------
// small codecs

pub fn put_uvarint(out: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}
pub fn uvarint(b: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, &c) in b.iter().enumerate() {
        if i == 10 {
            return None;
        }
        if c < 0x80 {
            if i == 9 && c > 1 {
                return None;
            }
            return Some((x | (c as u64) << s, i + 1));
        }
        x |= ((c & 0x7f) as u64) << s;
        s += 7;
    }
    None
}
fn uvarint_len(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

/// pebble/internal/crc: CRC-32C then rotate and add a constant.
pub fn block_checksum(data: &[u8], typ: u8) -> u32 {
    let c = crc32c::crc32c_append(crc32c::crc32c(data), &[typ]);
    (c >> 15 | c << 17).wrapping_add(0xa282ead8)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Handle {
    pub offset: u64,
    pub length: u64,
}
impl Handle {
    fn encode(&self, out: &mut Vec<u8>) {
        put_uvarint(out, self.offset);
        put_uvarint(out, self.length);
    }
    fn decode(b: &[u8]) -> Option<(Handle, usize)> {
        let (o, n) = uvarint(b)?;
        let (l, m) = uvarint(&b[n..])?;
        Some((Handle { offset: o, length: l }, n + m))
    }
}

// ---------------------------------------------------------------------------
// row block builder (sstable/rowblk.Writer)

struct BlockBuilder {
    restart_interval: usize,
    n: usize,
    next_restart: usize,
    buf: Vec<u8>,
    restarts: Vec<u32>,
    cur_key: Vec<u8>,
    prev_key: Vec<u8>,
}

impl BlockBuilder {
    fn new(restart_interval: usize) -> Self {
        BlockBuilder { restart_interval, n: 0, next_restart: 0, buf: Vec::new(), restarts: Vec::new(), cur_key: Vec::new(), prev_key: Vec::new() }
    }
    /// key is the ENCODED key (internal key for data/index blocks, raw for
    /// meta blocks); max_shared bounds the prefix compression.
    fn add(&mut self, key: &[u8], value: &[u8], max_shared: usize) {
        std::mem::swap(&mut self.cur_key, &mut self.prev_key);
        self.cur_key.clear();
        self.cur_key.extend_from_slice(key);
        let mut shared = 0usize;
        if self.n == self.next_restart {
            self.next_restart = self.n + self.restart_interval;
            self.restarts.push(self.buf.len() as u32);
        } else {
            let n = max_shared.min(self.prev_key.len());
            while shared < n && self.cur_key[shared] == self.prev_key[shared] {
                shared += 1;
            }
        }
        put_uvarint(&mut self.buf, shared as u64);
        put_uvarint(&mut self.buf, (key.len() - shared) as u64);
        put_uvarint(&mut self.buf, value.len() as u64);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.n += 1;
    }
    fn estimated_size(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4
    }
    fn finish(&mut self) -> Vec<u8> {
        if self.n == 0 {
            self.restarts.clear();
            self.restarts.push(0);
        }
        let mut out = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.n = 0;
        self.next_restart = 0;
        self.restarts.clear();
        out
    }
}

/// block.FlushGovernor without allocator size classes plus sstable.shouldFlush.
fn should_flush(key_len: usize, value_len: usize, restart_interval: usize, est: usize, n: usize, target: usize) -> bool {
    if n == 0 {
        return false;
    }
    let low = (target * BLOCK_SIZE_THRESHOLD + 99) / 100;
    if est < low {
        return false;
    }
    let mut new_size = est + key_len + value_len;
    if n % restart_interval == 0 {
        new_size += 4;
    }
    new_size += 4 + uvarint_len(key_len as u64) + uvarint_len(value_len as u64);
    if est >= new_size || est < low {
        return false;
    }
    new_size > target
}

// ---------------------------------------------------------------------------
// writer

pub struct Opts {
    pub block_size: usize,
    pub index_block_size: usize,
    pub filter: bool,
    pub zstd_level: i32,
    pub comparer: &'static str,
    pub merger: &'static str,
}

struct Partition {
    sep: Vec<u8>,
    block: Vec<u8>,
    entries: usize,
}

pub struct SstWriter<'a> {
    out: &'a mut dyn Write,
    offset: u64,
    opts: Opts,
    data: BlockBuilder,
    index: BlockBuilder,
    parts: Vec<Partition>,
    two_level: bool,
    filter: Option<bloom::FilterWriter>,
    cctx: zstd::zstd_safe::CCtx<'static>,
    cbuf: Vec<u8>,
    num_entries: u64,
    raw_key_size: u64,
    raw_value_size: u64,
    metaindex: Vec<(String, Vec<u8>)>,
    ikey: Vec<u8>,
    last_user_key: Vec<u8>,
    last_handle: Handle,
}

impl<'a> SstWriter<'a> {
    pub fn new(out: &'a mut dyn Write, opts: Opts) -> Self {
        let filter = opts.filter.then(|| bloom::FilterWriter::new(20));
        SstWriter {
            out,
            offset: 0,
            opts,
            data: BlockBuilder::new(RESTART_INTERVAL),
            index: BlockBuilder::new(1),
            parts: Vec::new(),
            two_level: false,
            filter,
            cctx: zstd::zstd_safe::CCtx::create(),
            cbuf: Vec::new(),
            num_entries: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            metaindex: Vec::new(),
            ikey: Vec::new(),
            last_user_key: Vec::new(),
            last_handle: Handle::default(),
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.num_entries > 0 && key <= self.last_user_key.as_slice() {
            bail!("sst: keys must be added in strictly increasing order");
        }
        if should_flush(key.len() + 8, value.len(), RESTART_INTERVAL, self.data.estimated_size(), self.data.n, self.opts.block_size) {
            self.flush_data()?;
        }
        if let Some(f) = self.filter.as_mut() {
            f.add_key(&key[..crate::format::split(key)]);
        }
        self.ikey.clear();
        self.ikey.extend_from_slice(key);
        self.ikey.extend_from_slice(&SET_TRAILER);
        let ikey = std::mem::take(&mut self.ikey);
        self.data.add(&ikey, value, key.len());
        self.ikey = ikey;
        self.num_entries += 1;
        self.raw_key_size += key.len() as u64 + 8;
        self.raw_value_size += value.len() as u64;
        self.last_user_key.clear();
        self.last_user_key.extend_from_slice(key);
        Ok(())
    }

    fn compress(&mut self, data: &[u8]) -> (u8, Vec<u8>) {
        let bound = zstd::zstd_safe::compress_bound(data.len());
        self.cbuf.clear();
        put_uvarint(&mut self.cbuf, data.len() as u64);
        let vl = self.cbuf.len();
        self.cbuf.resize(vl + bound, 0);
        let n = self.cctx.compress(&mut self.cbuf[vl..], data, self.opts.zstd_level).expect("zstd compress");
        self.cbuf.truncate(vl + n);
        if self.cbuf.len() * 100 > data.len() * (100 - MIN_REDUCTION_PERCENT) {
            return (IND_NONE, data.to_vec());
        }
        (IND_ZSTD, self.cbuf.clone())
    }

    fn write_physical(&mut self, ind: u8, body: &[u8]) -> Result<Handle> {
        let sum = block_checksum(body, ind);
        self.out.write_all(body)?;
        self.out.write_all(&[ind])?;
        self.out.write_all(&sum.to_le_bytes())?;
        let h = Handle { offset: self.offset, length: body.len() as u64 };
        self.offset += body.len() as u64 + TRAILER_LEN as u64;
        self.last_handle = h;
        Ok(h)
    }
    fn write_block(&mut self, data: &[u8]) -> Result<Handle> {
        let (ind, body) = self.compress(data);
        self.write_physical(ind, &body)
    }
    fn write_block_uncompressed(&mut self, data: &[u8]) -> Result<Handle> {
        self.write_physical(IND_NONE, data)
    }

    /// Finish the data block, write it and add its index entry (pebble's
    /// flush + writeQueue.performWrite + addIndexEntry, in that order).
    fn flush_data(&mut self) -> Result<()> {
        let sep = if self.data.n == 0 { INVALID_TRAILER.to_vec() } else { self.data.cur_key.clone() };
        let blk = self.data.finish();
        let h = self.write_block(&blk)?;
        self.add_index_entry(sep, h)
    }

    fn add_index_entry(&mut self, sep: Vec<u8>, h: Handle) -> Result<()> {
        let cut = should_flush(sep.len(), ENCODED_BHP_ESTIMATED_SIZE, 1, self.index.estimated_size(), self.index.n, self.opts.index_block_size);
        if cut {
            let psep = self.index.cur_key.clone();
            let entries = self.index.n;
            let block = self.index.finish();
            self.parts.push(Partition { sep: psep, block, entries });
            self.two_level = true;
        }
        let mut v = Vec::with_capacity(20);
        h.encode(&mut v);
        self.index.add(&sep, &v, sep.len());
        Ok(())
    }

    /// Writes filter, index, properties, metaindex and footer. Returns the
    /// section's byte length.
    pub fn finish(mut self) -> Result<u64> {
        if self.data.n > 0 || self.index.n == 0 {
            self.flush_data()?;
        }
        let data_size = self.offset;
        let mut filter_size = 0u64;
        if let Some(f) = self.filter.take() {
            let b = if f.count == 0 { Vec::new() } else { f.finish() };
            let h = self.write_block_uncompressed(&b)?;
            filter_size = h.length;
            let mut v = Vec::new();
            h.encode(&mut v);
            self.metaindex.push((FILTER_META_NAME.to_string(), v));
        }
        let (index_type, index_size, num_data_blocks, index_partitions, top_size);
        if self.two_level {
            let psep = self.index.cur_key.clone();
            let entries = self.index.n;
            let block = self.index.finish();
            self.parts.push(Partition { sep: psep, block, entries });
            let mut top = BlockBuilder::new(1);
            let mut isz = 0u64;
            let mut ndb = 0u64;
            let parts = std::mem::take(&mut self.parts);
            for p in &parts {
                ndb += p.entries as u64;
                isz += p.block.len() as u64;
                let h = self.write_block(&p.block)?;
                let mut v = Vec::new();
                h.encode(&mut v);
                top.add(&p.sep, &v, p.sep.len());
            }
            index_partitions = parts.len() as u64;
            top_size = top.estimated_size() as u64;
            isz += top_size + TRAILER_LEN as u64;
            let tb = top.finish();
            self.write_block(&tb)?;
            index_type = 2u32;
            index_size = isz;
            num_data_blocks = ndb;
        } else {
            index_type = 0;
            index_size = self.index.estimated_size() as u64 + TRAILER_LEN as u64;
            num_data_blocks = self.index.n as u64;
            index_partitions = 0;
            top_size = 0;
            let ib = self.index.finish();
            self.write_block(&ib)?;
        }
        // The last-written index block is what the footer names.
        let index_handle = self.last_handle;

        // properties (sorted keys, restart interval "infinite")
        let mut props: Vec<(String, Vec<u8>)> = Vec::new();
        let uv = |x: u64| {
            let mut v = Vec::new();
            put_uvarint(&mut v, x);
            v
        };
        props.push(("rocksdb.block.based.table.index.type".into(), index_type.to_le_bytes().to_vec()));
        props.push(("rocksdb.comparator".into(), self.opts.comparer.as_bytes().to_vec()));
        props.push(("rocksdb.compression".into(), format!("zstd{}", self.opts.zstd_level).into_bytes()));
        props.push(("rocksdb.compression_options".into(), COMPRESSION_OPTIONS.as_bytes().to_vec()));
        props.push(("rocksdb.data.size".into(), uv(data_size)));
        props.push(("rocksdb.deleted.keys".into(), uv(0)));
        if self.opts.filter {
            props.push(("rocksdb.filter.policy".into(), FILTER_POLICY_NAME.as_bytes().to_vec()));
        }
        props.push(("rocksdb.filter.size".into(), uv(filter_size)));
        if index_partitions != 0 {
            props.push(("rocksdb.index.partitions".into(), uv(index_partitions)));
            props.push(("rocksdb.top-level.index.size".into(), uv(top_size)));
        }
        props.push(("rocksdb.index.size".into(), uv(index_size)));
        props.push(("rocksdb.merge.operands".into(), uv(0)));
        props.push(("rocksdb.merge.operator".into(), self.opts.merger.as_bytes().to_vec()));
        props.push(("rocksdb.num.data.blocks".into(), uv(num_data_blocks)));
        props.push(("rocksdb.num.entries".into(), uv(self.num_entries)));
        props.push(("rocksdb.num.range-deletions".into(), uv(0)));
        props.push(("rocksdb.property.collectors".into(), b"[]".to_vec()));
        props.push(("rocksdb.raw.key.size".into(), uv(self.raw_key_size)));
        props.push(("rocksdb.raw.value.size".into(), uv(self.raw_value_size)));
        props.sort_by(|a, b| a.0.cmp(&b.0));
        let mut pb = BlockBuilder::new(i32::MAX as usize);
        for (k, v) in &props {
            pb.add(k.as_bytes(), v, k.len());
        }
        let pblk = pb.finish();
        let ph = self.write_block_uncompressed(&pblk)?;
        let mut v = Vec::new();
        ph.encode(&mut v);
        self.metaindex.push((PROPS_META_NAME.to_string(), v));

        self.metaindex.sort_by(|a, b| a.0.cmp(&b.0));
        let mut mb = BlockBuilder::new(1);
        let mi = std::mem::take(&mut self.metaindex);
        for (k, v) in &mi {
            mb.add(k.as_bytes(), v, k.len());
        }
        let mblk = mb.finish();
        let mh = self.write_block_uncompressed(&mblk)?;

        let mut footer = vec![0u8; FOOTER_LEN];
        footer[0] = 1; // crc32c
        let mut hb = Vec::new();
        mh.encode(&mut hb);
        index_handle.encode(&mut hb);
        footer[1..1 + hb.len()].copy_from_slice(&hb);
        footer[FOOTER_LEN - 12..FOOTER_LEN - 8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        footer[FOOTER_LEN - 8..].copy_from_slice(MAGIC);
        self.out.write_all(&footer)?;
        self.offset += FOOTER_LEN as u64;
        Ok(self.offset)
    }

}

// ---------------------------------------------------------------------------
// reader

/// Random-access bytes: a local mmap or the chunk cache over a bucket.
pub trait ReadAt: Send + Sync {
    fn size(&self) -> u64;
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<()>;
}

/// One SST section of a run: [off, off+len) of an artifact.
pub struct Sst {
    blob: Arc<dyn ReadAt>,
    off: u64,
    pub len: u64,
    /// Every data block's (last user key, handle), across all index partitions.
    index: Vec<(Vec<u8>, Handle)>,
    pub filter: Option<Vec<u8>>,
    pub props: Vec<(Vec<u8>, Vec<u8>)>,
    pub two_level: bool,
    /// Index partitions of a two-level index, for the layout report.
    pub index_blocks: Vec<Handle>,
}

fn decode_block_entries(blk: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if blk.len() < 4 {
        bail!("sst: block shorter than its restart count");
    }
    let nr = u32::from_le_bytes(blk[blk.len() - 4..].try_into().unwrap()) as usize;
    let end = blk.len().checked_sub(4 + 4 * nr).ok_or_else(|| anyhow!("sst: bad restart count"))?;
    let mut out = Vec::new();
    let mut key: Vec<u8> = Vec::new();
    let mut p = 0;
    while p < end {
        let (shared, a) = uvarint(&blk[p..]).ok_or_else(|| anyhow!("sst: bad entry"))?;
        let (unshared, b) = uvarint(&blk[p + a..]).ok_or_else(|| anyhow!("sst: bad entry"))?;
        let (vlen, c) = uvarint(&blk[p + a + b..]).ok_or_else(|| anyhow!("sst: bad entry"))?;
        p += a + b + c;
        key.truncate(shared as usize);
        key.extend_from_slice(&blk[p..p + unshared as usize]);
        p += unshared as usize;
        out.push((key.clone(), blk[p..p + vlen as usize].to_vec()));
        p += vlen as usize;
    }
    Ok(out)
}

fn strip_trailer(ikey: &[u8]) -> &[u8] {
    &ikey[..ikey.len().saturating_sub(8)]
}

impl Sst {
    pub fn open(blob: Arc<dyn ReadAt>, off: u64, len: u64) -> Result<Sst> {
        let mut s = Sst { blob, off, len, index: Vec::new(), filter: None, props: Vec::new(), two_level: false, index_blocks: Vec::new() };
        if len < FOOTER_LEN as u64 {
            bail!("sst: section is {len} bytes, too small for a footer");
        }
        let mut f = vec![0u8; FOOTER_LEN];
        s.blob.read_at(off + len - FOOTER_LEN as u64, &mut f)?;
        if &f[FOOTER_LEN - 8..] != MAGIC {
            bail!("sst: bad magic");
        }
        let ver = u32::from_le_bytes(f[FOOTER_LEN - 12..FOOTER_LEN - 8].try_into().unwrap());
        if ver != FORMAT_VERSION {
            bail!("sst: table format version {ver}, want {FORMAT_VERSION}");
        }
        if f[0] != 1 {
            bail!("sst: checksum type {}", f[0]);
        }
        let (mh, n) = Handle::decode(&f[1..]).ok_or_else(|| anyhow!("sst: bad metaindex handle"))?;
        let (ih, _) = Handle::decode(&f[1 + n..]).ok_or_else(|| anyhow!("sst: bad index handle"))?;
        let meta = decode_block_entries(&s.read_block(mh)?)?;
        let mut filter_h = None;
        for (k, v) in &meta {
            if k == PROPS_META_NAME.as_bytes() {
                let (h, _) = Handle::decode(v).ok_or_else(|| anyhow!("sst: bad props handle"))?;
                s.props = decode_block_entries(&s.read_block(h)?)?;
            } else if k == FILTER_META_NAME.as_bytes() {
                let (h, _) = Handle::decode(v).ok_or_else(|| anyhow!("sst: bad filter handle"))?;
                filter_h = Some(h);
            }
        }
        if let Some(h) = filter_h {
            s.filter = Some(s.read_block(h)?);
        }
        let index_type = s
            .props
            .iter()
            .find(|(k, _)| k == b"rocksdb.block.based.table.index.type")
            .map(|(_, v)| u32::from_le_bytes(v[..4].try_into().unwrap()))
            .unwrap_or(0);
        s.two_level = index_type == 2;
        let top = decode_block_entries(&s.read_block(ih)?)?;
        if s.two_level {
            for (_, v) in &top {
                let (h, _) = Handle::decode(v).ok_or_else(|| anyhow!("sst: bad index partition handle"))?;
                s.index_blocks.push(h);
                for (k, v) in decode_block_entries(&s.read_block(h)?)? {
                    let (dh, _) = Handle::decode(&v).ok_or_else(|| anyhow!("sst: bad data handle"))?;
                    s.index.push((strip_trailer(&k).to_vec(), dh));
                }
            }
        } else {
            s.index_blocks.push(ih);
            for (k, v) in top {
                let (dh, _) = Handle::decode(&v).ok_or_else(|| anyhow!("sst: bad data handle"))?;
                s.index.push((strip_trailer(&k).to_vec(), dh));
            }
        }
        Ok(s)
    }

    pub fn prop(&self, name: &str) -> Option<&[u8]> {
        self.props.iter().find(|(k, _)| k == name.as_bytes()).map(|(_, v)| v.as_slice())
    }
    pub fn prop_uvarint(&self, name: &str) -> u64 {
        self.prop(name).and_then(|v| uvarint(v)).map(|(x, _)| x).unwrap_or(0)
    }
    pub fn num_data_blocks(&self) -> usize {
        self.index.len()
    }
    pub fn data_handles(&self) -> impl Iterator<Item = Handle> + '_ {
        self.index.iter().map(|(_, h)| *h)
    }

    /// Reads and verifies one block, decompressing it.
    pub fn read_block(&self, h: Handle) -> Result<Vec<u8>> {
        let n = h.length as usize + TRAILER_LEN;
        let mut raw = vec![0u8; n];
        self.blob.read_at(self.off + h.offset, &mut raw)?;
        let ind = raw[h.length as usize];
        let want = u32::from_le_bytes(raw[h.length as usize + 1..].try_into().unwrap());
        let got = block_checksum(&raw[..h.length as usize], ind);
        if got != want {
            bail!("sst: block {}/{}: checksum {got:08x} != {want:08x}", h.offset, h.length);
        }
        raw.truncate(h.length as usize);
        match ind {
            IND_NONE => Ok(raw),
            IND_ZSTD => {
                let (dl, vl) = uvarint(&raw).ok_or_else(|| anyhow!("sst: bad zstd length prefix"))?;
                let mut out = vec![0u8; dl as usize];
                let n = zstd::bulk::decompress_to_buffer(&raw[vl..], &mut out)?;
                if n != dl as usize {
                    bail!("sst: zstd block decoded to {n} bytes, header says {dl}");
                }
                Ok(out)
            }
            _ => bail!("sst: unsupported block compression {ind}"),
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        match &self.filter {
            None => true,
            Some(f) => bloom::may_contain(f, &key[..crate::format::split(key)]),
        }
    }

    /// The whole block that could hold key, decoded (user keys only), or None
    /// past the end.
    fn block_index_ge(&self, key: &[u8]) -> Option<usize> {
        let i = self.index.partition_point(|(sep, _)| sep.as_slice() < key);
        (i < self.index.len()).then_some(i)
    }

    pub fn iter(&self) -> SstIter<'_> {
        SstIter { sst: self, blk: usize::MAX, entries: Vec::new(), pos: 0 }
    }

    /// Exact-key point read.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.may_contain(key) {
            return Ok(None);
        }
        let mut it = self.iter();
        if it.seek_ge(key)? && it.key() == key {
            return Ok(Some(it.value().to_vec()));
        }
        Ok(None)
    }

    /// The last row under prefix with TxNum suffix <= at (store's Latest).
    pub fn latest(&self, prefix: &[u8], at: u64) -> Result<Option<(Vec<u8>, u64)>> {
        if !self.may_contain(&crate::format::suffixed(prefix, 0)) {
            return Ok(None);
        }
        let mut it = self.iter();
        if !it.seek_lt(&crate::format::suffixed(prefix, at + 1))? {
            return Ok(None);
        }
        let k = it.key();
        if k.len() != prefix.len() + 8 || &k[..prefix.len()] != prefix {
            return Ok(None);
        }
        Ok(Some((it.value().to_vec(), crate::format::txnum_of(k))))
    }
}

/// A cursor over one section. Blocks are decoded whole on entry; every key
/// is a user key (the SET trailer is stripped).
pub struct SstIter<'a> {
    sst: &'a Sst,
    blk: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl<'a> SstIter<'a> {
    fn load(&mut self, blk: usize) -> Result<()> {
        if self.blk == blk {
            return Ok(());
        }
        let raw = self.sst.read_block(self.sst.index[blk].1)?;
        let mut ents = decode_block_entries(&raw)?;
        for e in &mut ents {
            let n = e.0.len() - 8;
            e.0.truncate(n);
        }
        self.entries = ents;
        self.blk = blk;
        Ok(())
    }
    pub fn valid(&self) -> bool {
        self.blk != usize::MAX && self.pos < self.entries.len()
    }
    pub fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }
    pub fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }
    pub fn first(&mut self) -> Result<bool> {
        if self.sst.index.is_empty() {
            return Ok(false);
        }
        self.load(0)?;
        self.pos = 0;
        while !self.valid() {
            if self.blk + 1 >= self.sst.index.len() {
                return Ok(false);
            }
            let b = self.blk + 1;
            self.load(b)?;
            self.pos = 0;
        }
        Ok(true)
    }
    pub fn seek_ge(&mut self, key: &[u8]) -> Result<bool> {
        let Some(b) = self.sst.block_index_ge(key) else {
            self.blk = usize::MAX;
            return Ok(false);
        };
        self.load(b)?;
        self.pos = self.entries.partition_point(|(k, _)| k.as_slice() < key);
        if self.pos >= self.entries.len() {
            return self.next();
        }
        Ok(true)
    }
    /// Positions on the last entry with key < target.
    pub fn seek_lt(&mut self, key: &[u8]) -> Result<bool> {
        let n = self.sst.index.len();
        if n == 0 {
            return Ok(false);
        }
        let mut b = self.sst.block_index_ge(key).unwrap_or(n - 1);
        loop {
            self.load(b)?;
            let p = self.entries.partition_point(|(k, _)| k.as_slice() < key);
            if p > 0 {
                self.pos = p - 1;
                return Ok(true);
            }
            if b == 0 {
                self.blk = usize::MAX;
                return Ok(false);
            }
            b -= 1;
        }
    }
    pub fn next(&mut self) -> Result<bool> {
        if self.blk == usize::MAX {
            return Ok(false);
        }
        self.pos += 1;
        while self.pos >= self.entries.len() {
            if self.blk + 1 >= self.sst.index.len() {
                self.blk = usize::MAX;
                return Ok(false);
            }
            let b = self.blk + 1;
            self.load(b)?;
            self.pos = 0;
        }
        Ok(true)
    }
}

impl Sst {
    /// The raw stored bytes of a block (compressed payload, no trailer).
    pub fn physical(&self, h: Handle) -> Result<Vec<u8>> {
        let mut raw = vec![0u8; h.length as usize];
        self.blob.read_at(self.off + h.offset, &mut raw)?;
        Ok(raw)
    }
}
