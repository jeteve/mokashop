fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/mokashop/v1/mokashop.proto")?;
    Ok(())
}
