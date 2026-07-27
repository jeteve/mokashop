// This is where the v1 traits live:
pub mod v1 {
    const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("barista.v1");

    // For reflection
    pub fn file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }

    // Code
    tonic::include_proto!("mokashop.v1");
}

pub mod raft;
