//! cnode-check --data <dir> --bootstrap <export dir> [--every SECS] [--roll-every N] [--once]
//!
//! The root checker as its own process, off the node's box of CPU and memory:
//! seeds rs/state's trie from the bootstrap export on the first run (root
//! verified against meta.json), then every `--every` seconds tails the
//! node's diff log (`<data>/difflog`), replays the new blocks and compares
//! ONE root, the last block's, with its header root. A mismatch writes
//! `<data>/HALTED` (the node's applier stops on it) and exits 3. Progress
//! goes to `<data>/CHECKED`; every `--roll-every` blocks the rows are merged
//! into a new run (the node's restart snapshot) and the log is pruned.
use cnode::checker::{self, Checker};
use cnode::{difflog, import};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).map(|i| a[i + 1].clone())
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(arg(&a, "--data").expect("--data"));
    let bootstrap = PathBuf::from(arg(&a, "--bootstrap").expect("--bootstrap"));
    let every = Duration::from_secs(arg(&a, "--every").map_or(60, |s| s.parse().unwrap()));
    let roll_every: u64 = arg(&a, "--roll-every").map_or(4000, |s| s.parse().unwrap());
    let once = a.iter().any(|x| x == "--once");
    if let Err(e) = run(&data, &bootstrap, every, roll_every, once) {
        eprintln!("cnode-check: {e:#}");
        std::process::exit(2);
    }
}

fn run(data: &PathBuf, bootstrap: &PathBuf, every: Duration, roll_every: u64, once: bool) -> anyhow::Result<()> {
    std::fs::create_dir_all(data)?;
    if let Some(m) = checker::read_halt_file(data) {
        anyhow::bail!("{}: HALTED at {} (header {} vs ours {}); decide by hand", data.display(), m.height, m.expected, m.got);
    }
    let vmstate = data.join("vmstate");
    let log_dir = data.join("difflog");
    let mut c = match checker::read_manifest(&vmstate)? {
        Some(m) => {
            let (c, _) = Checker::open(&vmstate)?;
            eprintln!("cnode-check: open at gen {} height {} root {}", m.gen, m.height, m.root);
            c
        }
        None => {
            let meta = import::read_meta(bootstrap)?;
            let t = Instant::now();
            let mut rows = import::ExportIter::open(bootstrap)?;
            let c = Checker::seed(&vmstate, &mut rows, meta.height, meta.state_root)?;
            eprintln!("cnode-check: seeded from the export, {} rows, root verified at {} in {:.1?}", rows.rows, meta.height, t.elapsed());
            c
        }
    };
    checker::write_checked(data, c.height, c.root)?;

    loop {
        let t = Instant::now();
        let mut r = difflog::Reader::new(&log_dir, c.height);
        let (mut n, mut last) = (0u64, None);
        while let Some(rec) = r.next()? {
            c.replay(rec.height, &rec.rows, rec.root);
            last = Some((rec.height, rec.root));
            n += 1;
        }
        if let Some((h, root)) = last {
            let (first, replay_ms) = (h + 1 - n, t.elapsed().as_millis());
            if let Err(m) = c.check(h, root) {
                eprintln!("CHECKER MISMATCH in blocks {first}..={h}: header root {} at {h} but our state rolls to {}. HALTED.", m.expected, m.got);
                checker::write_halt_file(data, &m);
                std::process::exit(3);
            }
            checker::write_checked(data, h, root)?;
            eprintln!("cnode-check: {first}..={h} ok ({n} blocks, replay {replay_ms} ms, root {} ms)", t.elapsed().as_millis() as u128 - replay_ms);
        }
        c.maybe_roll(roll_every)?;
        if let Some(m) = checker::read_manifest(&vmstate)? {
            let pruned = difflog::prune(&log_dir, m.height)?;
            if pruned > 0 {
                eprintln!("cnode-check: pruned {pruned} log segments below the roll at {}", m.height);
            }
        }
        if once && !c.rolling() {
            return Ok(());
        }
        std::thread::sleep(every);
    }
}
