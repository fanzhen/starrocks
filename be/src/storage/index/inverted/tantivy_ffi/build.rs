fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(cbindgen::Config::from_file(format!("{}/cbindgen.toml", crate_dir))
            .expect("Unable to read cbindgen.toml"))
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("tantivy_ffi.h");
}
