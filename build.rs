use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // For reflection
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    //tonic_build::
    //   .file_descriptor_set_path(out_dir.join("myservice_descriptor.bin"))
    //  .compile(&["proto/myservice.proto"], &["proto"])?;

    // For code generation
    // Allows tonic::include_proto!("mokashop.v1");
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("barista.v1.bin"))
        .compile_protos(&["proto/mokashop/v1/barista.proto"], &[])?;
    //tonic_prost_build::compile_protos("proto/mokashop/v1/mokashop.proto")?;

    Ok(())
}
