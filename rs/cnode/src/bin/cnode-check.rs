//! cnode-check --data <dir> --bootstrap <export dir> [--every SECS] [--roll-every N] [--once]
//!
//! The root checker as its own process, off the node's box of CPU and memory.
//! On EVERY start it seeds rs/state's trie from the newest dump the last run
//! blessed (`<data>/dumps/<height>` at or below `<data>/CHECKED`, else the
//! bootstrap export), sorting it first when it is a fork dump, and requires
//! the rolled root to equal the header root in meta.json. Then every
//! `--every` seconds it tails the node's diff log (`<data>/difflog`),
//! replays the new blocks and compares ONE root, the last block's, with its
//! header root. A mismatch writes `<data>/HALTED` (the node's applier stops
//! on it) and exits 3. Progress goes to `<data>/CHECKED`. Log segments
//! below the newest dump are pruned every pass.
use cnode::checker::{self, Checker};
use cnode::{difflog, dump, import};
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
    let roll_every: u64 = arg(&a, "--roll-every").map_or(0, |s| s.parse().unwrap());
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
    let dumps = data.join("dumps");
    let log_dir = data.join("difflog");

    // Seed: the trie over the newest blessed dump, root checked on every start.
    let blessed = checker::read_checked(data).map(|c| c.height);
    let src = match dump::newest(&dumps, blessed)? {
        Some((_, p)) => p,
        None => bootstrap.clone(),
    };
    let meta = import::read_meta(&src)?;
    let t = Instant::now();
    let sorted = if meta.sorted {
        src.clone()
    } else {
        let d = data.join("sorted");
        import::sort_export(&src, &d)?;
        eprintln!("cnode-check: sorted {} into {} in {:.1?}", src.display(), d.display(), t.elapsed());
        d
    };
    let vmstate = data.join("vmstate");
    let _ = std::fs::remove_dir_all(&vmstate);
    let mut rows = import::ExportIter::open(&sorted)?;
    let mut c = Checker::seed(&vmstate, &mut rows, meta.height, meta.state_root)?;
    eprintln!("cnode-check: trie seeded from {}: {} rows, root {} verified at height {} in {:.1?}", src.display(), rows.rows, meta.state_root, meta.height, t.elapsed());
    if sorted != src {
        let _ = std::fs::remove_dir_all(&sorted);
    }
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
            eprintln!("cnode-check: {first}..={h} ok ({n} blocks, replay {replay_ms} ms, root {} ms)", t.elapsed().as_millis() - replay_ms);
        }
        if roll_every > 0 {
            c.maybe_roll(roll_every)?;
        }
        if let Some((dh, _)) = dump::newest(&dumps, None)? {
            let pruned = difflog::prune(&log_dir, dh)?;
            if pruned > 0 {
                eprintln!("cnode-check: pruned {pruned} log segments below the dump at {dh}");
            }
        }
        if once && !c.rolling() {
            return Ok(());
        }
        std::thread::sleep(every);
    }
}
