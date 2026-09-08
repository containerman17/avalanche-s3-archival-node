//! C ABI over the store. Every call takes the database handle from `pf_open`;
//! hashes are 32-byte buffers; a status of code 0 is success, 1 means the
//! revision is not retained (Go maps it to ErrPruned), 2 carries a message
//! the caller frees with `pf_free_status`.

pub mod store;
pub mod trie;

use std::ffi::{c_char, CStr, CString};
use std::path::PathBuf;
use std::sync::RwLock;

use store::{Config, Error, Store};
use trie::{Hash, ROW};

pub struct Db(RwLock<Store>);

#[repr(C)]
pub struct pf_status {
    pub code: i32,
    pub msg: *mut c_char,
}

#[repr(C)]
pub struct pf_value {
    /// 0 when the key is absent.
    pub len: u32,
    pub data: [u8; 72],
}

#[repr(C)]
pub struct pf_head {
    pub root: [u8; 32],
    pub block: [u8; 32],
    pub height: u64,
}

const OK: pf_status = pf_status { code: 0, msg: std::ptr::null_mut() };

fn status<T>(r: store::Result<T>) -> pf_status {
    match r {
        Ok(_) => OK,
        Err(Error::Pruned) => pf_status { code: 1, msg: std::ptr::null_mut() },
        Err(Error::Msg(m)) => pf_status { code: 2, msg: CString::new(m).unwrap_or_default().into_raw() },
    }
}

unsafe fn hash(p: *const u8) -> Hash {
    *(p as *const Hash)
}

fn read(db: *const Db) -> std::sync::RwLockReadGuard<'static, Store> {
    unsafe { &*db }.0.read().unwrap_or_else(|e| e.into_inner())
}
fn write(db: *const Db) -> std::sync::RwLockWriteGuard<'static, Store> {
    unsafe { &*db }.0.write().unwrap_or_else(|e| e.into_inner())
}

/// Opens or creates the state directory. Zero values select the defaults.
#[no_mangle]
pub unsafe extern "C" fn pf_open(dir: *const c_char, retain: u64, commit_interval: u64, journal_limit: i64, out: *mut *mut Db) -> pf_status {
    let dir = PathBuf::from(CStr::from_ptr(dir).to_string_lossy().into_owned());
    match Store::open(Config { dir, retain, commit_interval, journal_limit }) {
        Ok(s) => {
            *out = Box::into_raw(Box::new(Db(RwLock::new(s))));
            OK
        }
        Err(e) => status::<()>(Err(e)),
    }
}

/// Closes and frees the handle.
#[no_mangle]
pub unsafe extern "C" fn pf_close(db: *mut Db) -> pf_status {
    let db = Box::from_raw(db);
    let r = db.0.write().unwrap_or_else(|e| e.into_inner()).close();
    status(r)
}

/// Computes the root of applying ops to the revision at parent and keeps the
/// proposal for `pf_update`.
#[no_mangle]
pub unsafe extern "C" fn pf_propose(db: *mut Db, parent: *const u8, ops: *const u8, ops_len: usize, out_root: *mut u8) -> pf_status {
    let ops = if ops_len == 0 { &[][..] } else { std::slice::from_raw_parts(ops, ops_len) };
    let r = write(db).propose(&hash(parent), ops);
    if let Ok(root) = &r {
        std::ptr::copy_nonoverlapping(root.as_ptr(), out_root, 32);
    }
    status(r)
}

#[no_mangle]
pub unsafe extern "C" fn pf_update(db: *mut Db, root: *const u8, parent: *const u8, height: u64, parent_hash: *const u8, block_hash: *const u8) -> pf_status {
    status(write(db).update(&hash(root), &hash(parent), height, &hash(parent_hash), &hash(block_hash)))
}

#[no_mangle]
pub unsafe extern "C" fn pf_commit(db: *mut Db, root: *const u8) -> pf_status {
    status(write(db).commit(&hash(root)))
}

#[no_mangle]
pub unsafe extern "C" fn pf_get_account(db: *mut Db, root: *const u8, key: *const u8, out: *mut pf_value) -> pf_status {
    let r = read(db).get_account(&hash(root), &hash(key));
    if let Ok(v) = &r {
        match v {
            Some(row) => {
                (*out).len = ROW as u32;
                (*out).data = *row;
            }
            None => (*out).len = 0,
        }
    }
    status(r)
}

#[no_mangle]
pub unsafe extern "C" fn pf_get_storage(db: *mut Db, root: *const u8, key: *const u8, slot: *const u8, out: *mut pf_value) -> pf_status {
    let r = read(db).get_storage(&hash(root), &hash(key), &hash(slot));
    if let Ok(v) = &r {
        match v {
            Some((bytes, len)) => {
                (*out).len = *len as u32;
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*out).data.as_mut_ptr(), 32);
            }
            None => (*out).len = 0,
        }
    }
    status(r)
}

/// Code 1 when no retained revision has this root.
#[no_mangle]
pub unsafe extern "C" fn pf_has_root(db: *mut Db, root: *const u8) -> pf_status {
    status(read(db).has_root(&hash(root)))
}

#[no_mangle]
pub unsafe extern "C" fn pf_set_hash_and_height(db: *mut Db, hash_: *const u8, height: u64) {
    write(db).set_hash_and_height(&hash(hash_), height)
}

#[no_mangle]
pub unsafe extern "C" fn pf_clear_all(db: *mut Db) -> pf_status {
    status(write(db).clear_all())
}

#[no_mangle]
pub unsafe extern "C" fn pf_current(db: *mut Db, out: *mut pf_head) {
    let (root, block, height) = read(db).head();
    (*out).root = root;
    (*out).block = block;
    (*out).height = height;
}

/// Bytes held by the node arena.
#[no_mangle]
pub unsafe extern "C" fn pf_size(db: *mut Db) -> u64 {
    read(db).bytes() as u64
}

/// Forces a checkpoint (tests).
#[no_mangle]
pub unsafe extern "C" fn pf_checkpoint(db: *mut Db) -> pf_status {
    status(write(db).checkpoint())
}

#[no_mangle]
pub unsafe extern "C" fn pf_free_status(s: pf_status) {
    if !s.msg.is_null() {
        drop(CString::from_raw(s.msg));
    }
}
