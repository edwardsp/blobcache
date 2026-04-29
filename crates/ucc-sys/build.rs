use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");

    let lib = pkg_config::Config::new()
        .atleast_version("1.0")
        .probe("ucc")
        .expect("pkg-config could not find `ucc` (install libucc-dev / set PKG_CONFIG_PATH)");

    for path in &lib.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }
    for libname in &lib.libs {
        println!("cargo:rustc-link-lib={}", libname);
    }

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("ucc_.*")
        .allowlist_type("ucc_.*")
        .allowlist_var("UCC_.*")
        .derive_default(true)
        .layout_tests(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    for include in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", include.display()));
    }

    let bindings = builder
        .generate()
        .expect("bindgen failed to generate UCC bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("failed to write UCC bindings");
}
