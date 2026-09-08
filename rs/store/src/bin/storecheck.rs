//! Oracle and ops CLI for rs/store. Subcommands are added as the port grows.
use anyhow::{anyhow, Result};
use std::path::PathBuf;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).cloned().unwrap_or_default();
    match mode.as_str() {
        "probe" => probe(&args),
        _ => Err(anyhow!("usage: storecheck probe --data DIR")),
    }
}

/// Opens every run of a data dir, prints section properties and row counts,
/// and checks that Rust zstd reproduces the Go block bytes.
fn probe(args: &[String]) -> Result<()> {
    let dir = PathBuf::from(arg(args, "--data").ok_or_else(|| anyhow!("--data"))?);
    let cas = store::casfs::Store::local(&dir)?;
    let man: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
    for r in man["runs"].as_array().unwrap() {
        let name = r["name"].as_str().unwrap();
        let level = r["level"].as_i64().unwrap() as i32;
        let run = store::run::Run::open(&cas, name)?;
        println!("run {name} level {level} tx [{},{}) blocks [{},{}]", run.footer.from_tx, run.footer.to_tx, run.footer.from_height, run.footer.to_height);
        for (i, s) in run.sec.iter().enumerate() {
            println!("  section {i}: len {} two_level {} data_blocks {} filter {}", s.len, s.two_level, s.num_data_blocks(), s.filter.as_ref().map(|f| f.len()).unwrap_or(0));
            for (k, v) in &s.props {
                let ks = String::from_utf8_lossy(k);
                let vs = if v.iter().all(|c| c.is_ascii_graphic() || *c == b' ') && !v.is_empty() { String::from_utf8_lossy(v).into_owned() } else { hex::encode(v) };
                println!("    {ks} = {vs}");
            }
            let mut n = 0u64;
            let sec = [store::format::Section::Chain, store::format::Section::State, store::format::Section::Lookup][i];
            run.scan_range(sec, &[], None, |_, _| {
                n += 1;
                true
            })?;
            println!("    rows {n}");
            // zstd determinism: recompress the first few data blocks
            let level = if level >= 1 { 9 } else { 1 };
            let mut cctx = zstd::zstd_safe::CCtx::create();
            let mut same = 0;
            let mut diff = 0;
            for h in s.data_handles().take(50) {
                let plain = s.read_block(h)?;
                let mut raw = vec![0u8; h.length as usize + 5];
                // re-read the physical block through the section's blob
                let _ = &raw;
                let mut out = vec![0u8; zstd::zstd_safe::compress_bound(plain.len()) + 10];
                let n = cctx.compress(&mut out, &plain, level).map_err(|e| anyhow!("zstd: {e:?}"))?;
                out.truncate(n);
                let mut expect = Vec::new();
                store::sst::put_uvarint(&mut expect, plain.len() as u64);
                expect.extend_from_slice(&out);
                let phys = s.physical(h)?;
                raw.truncate(0);
                if phys == expect { same += 1 } else { diff += 1 }
            }
            println!("    zstd level {level}: {same} blocks identical, {diff} differ");
        }
    }
    Ok(())
}
