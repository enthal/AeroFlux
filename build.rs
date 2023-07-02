use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .file_descriptor_set_path(
            &PathBuf::from(env::var("OUT_DIR").unwrap()).join("aeroflux_descriptor.bin"),
        )
        .compile(&["proto/aeroflux.proto"], &["proto/"])?;

    Ok(())
}
