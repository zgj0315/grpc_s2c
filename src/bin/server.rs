use grpc_s2c::grpc_s2c_api::{
    grpc_s2c_api_server::{GrpcS2cApi, GrpcS2cApiServer},
    rsp, Input001, Req, Rsp,
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
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
    type BidirectionalStream = Pin<Box<dyn Stream<Item = Result<Rsp, Status>> + Send>>;
    async fn bidirectional(
        &self,
        request: Request<Streaming<Req>>,
    ) -> Result<Response<Self::BidirectionalStream>, Status> {
        let (tx, rx) = mpsc::channel(10);
        let input = Rsp {
            input: Some(rsp::Input::Input001(Input001 {
                task_id: "task_id_001".to_string(),
                msg: "the msg from server".to_string(),
            })),
        };
        tx.send(Ok(input)).await.unwrap();

        // 启动接收返回的协程
        let mut in_stream = request.into_inner();
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
        // log::info!("server send msg to client: {:?}", input);
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn unary(&self, request: Request<Req>) -> Result<Response<Rsp>, Status> {
        let req = request.into_inner();
        match req.output {
            None => {
                log::info!("get none from client");
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                let ts_now = chrono::Utc::now().timestamp_millis();
                let rsp = Rsp {
                    input: Some(rsp::Input::Input001(Input001 {
                        task_id: format!("task_id_{}", ts_now),
                        msg: "the msg from server".to_string(),
                    })),
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                log::info!("send a task to client: {:?}", rsp);
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                return Ok(Response::new(rsp));
            }
            Some(output) => {
                log::info!("get from client: {:?}", output);
                let ts_now = chrono::Utc::now().timestamp_millis();
                let rsp = Rsp {
                    input: Some(rsp::Input::Input001(Input001 {
                        task_id: format!("task_id_{}", ts_now),
                        msg: "the msg from server".to_string(),
                    })),
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                log::info!("send a task to client: {:?}", rsp);
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                return Ok(Response::new(rsp));
            }
        }
    }
}
