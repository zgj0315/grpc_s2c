use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
};

use grpc_s2c::grpc_s2c_api::{
    grpc_s2c_api_server::{GrpcS2cApi, GrpcS2cApiServer},
    stream_rsp, StreamReq, StreamRsp,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{codec::CompressionEncoding, transport::Server, Request, Response, Status, Streaming};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();
    let server = GrpcS2cServer::default();
    let service = GrpcS2cApiServer::new(server)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    log::info!("Server is running on {}", addr);
    Server::builder().add_service(service).serve(addr).await?;
    Ok(())
}

#[derive(Default)]
struct GrpcS2cServer {}

#[tonic::async_trait]
impl GrpcS2cApi for GrpcS2cServer {
    type BidirectionalStream = Pin<Box<dyn Stream<Item = Result<StreamRsp, Status>> + Send>>;
    async fn bidirectional(
        &self,
        req: Request<Streaming<StreamReq>>,
    ) -> Result<Response<Self::BidirectionalStream>, Status> {
        let (tx, rx) = mpsc::channel(10);
        let input = "hello client";
        let stream_rsp = StreamRsp {
            input: Some(stream_rsp::Input::Input001(input.into())),
        };

        tx.send(Ok(stream_rsp)).await.unwrap();

        // 启动接收返回的协程
        let mut in_stream = req.into_inner();
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(stream_req) => {
                        log::info!("server get rsp from client: {:?}", stream_req);
                    }
                    Err(e) => {
                        log::error!("err: {}", e);
                    }
                }
            }
        });
        log::info!("server send msg to client: {}", input);
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
