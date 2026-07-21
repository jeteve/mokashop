// Following https://github.com/grpc/grpc-rust/blob/master/examples/helloworld-tutorial.md

use mokashop::v1::barista_service_server::BaristaService;
use mokashop::v1::{HelloRequest, HelloResponse};

#[derive(Debug, Default)]
struct MyMokaShop;

#[tonic::async_trait]
impl BaristaService for MyMokaShop {
    async fn hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, tonic::Status> {
        let reply = HelloResponse {
            message: format!("Hello {}!", request.into_inner().name),
        };
        Ok(tonic::Response::new(reply))
    }
}

// See also https://medium.com/@drewjaja/how-to-add-grpc-reflection-with-rust-tonic-reflection-1f4e14e6750e

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let shop = MyMokaShop;

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(mokashop::v1::file_descriptor_set())
        .build_v1()?;

    tonic::transport::Server::builder()
        .add_service(reflection_service)
        .add_service(mokashop::v1::barista_service_server::BaristaServiceServer::new(shop))
        .serve(addr)
        .await?;

    Ok(())
}
