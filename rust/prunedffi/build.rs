fn main() {
    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let config = cbindgen::Config::from_file("cbindgen.toml").expect("cbindgen.toml");
    if let Ok(b) = cbindgen::Builder::new().with_crate(&dir).with_config(config).generate() {
        b.write_to_file("prunedffi.h");
    }
    println!("cargo:rerun-if-changed=src/lib.rs");
}
