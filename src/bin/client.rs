use grpc_s2c::grpc_s2c_api::{grpc_s2c_api_client::GrpcS2cApiClient, stream_req, StreamReq};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();
    let mut client = GrpcS2cApiClient::connect("http://127.0.0.1:50051").await?;
    let (tx, mut rx) = mpsc::channel(10);
    let outbound = async_stream::stream! {
        while let Some(rpc_fn_req) = rx.recv().await {
           log::info!("client get req: {:?}", rpc_fn_req);
           log::info!("do some work and send rsp to server");
           tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
           let output = "hello server";
           log::info!("client send rsp to server: {}", output);
           let stream_req = StreamReq {
               output: Some(stream_req::Output::Output001(output.to_string())),
           };
           yield stream_req;
        }
    };
    let stream_req = Request::new(outbound);

    // 接收server下发的信息
    let rsp = client.bidirectional(stream_req).await?;
    let mut rsp_stream = rsp.into_inner();
    while let Some(received) = rsp_stream.next().await {
        match received {
            Ok(rpc_fn_req) => {
                tx.send(rpc_fn_req).await?;
            }
            Err(e) => {
                log::error!("err: {}", e);
            }
        }
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    Ok(())
}
