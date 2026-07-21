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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let shop = MyMokaShop;

    tonic::transport::Server::builder()
        .add_service(mokashop::v1::barista_service_server::BaristaServiceServer::new(shop))
        .serve(addr)
        .await?;

    Ok(())
}
