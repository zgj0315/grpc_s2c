use grpc_s2c::grpc_s2c_api::rsp::Input;
use grpc_s2c::grpc_s2c_api::{
    grpc_s2c_api_server::{GrpcS2cApi, GrpcS2cApiServer},
    req, rsp, Input001, Req, Rsp,
};
use grpc_s2c::X_TASK_ID;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{codec::CompressionEncoding, transport::Server, Request, Response, Status, Streaming};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();
    if let Err(e) = TASK_SENDER.set(HashMap::new().into()) {
        log::error!("TASK_SENDER set err: {:?}", e);
    };
    if let Err(e) = RSP_LIST.set(VecDeque::new().into()) {
        log::error!("TASK_RSP set err: {:?}", e);
    };
    for i in 0..10 {
        tokio::spawn(async move {
            let msg = format!("message {:03}", i);
            log::info!("make task: {}", msg);
            let input = Input::Input001(Input001 { msg });
            match exec_fn(input).await {
                Ok(req) => {
                    log::info!("get msg from client: {:?}", req);
                }
                Err(e) => {
                    log::error!("run taks err: {}", e);
                }
            }
        });
    }
    let server = GrpcS2cServer::default();
    let service = GrpcS2cApiServer::new(server)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
    log::info!("Server is running on {}", addr);
    Server::builder().add_service(service).serve(addr).await?;
    Ok(())
}

static TASK_SENDER: OnceCell<RwLock<HashMap<String, Sender<Req>>>> = OnceCell::new();
static RSP_LIST: OnceCell<RwLock<VecDeque<(String, Rsp)>>> = OnceCell::new();

async fn exec_fn(input: rsp::Input) -> anyhow::Result<Option<req::Output>> {
    let (task_tx, task_rx) = tokio::sync::oneshot::channel::<Req>();
    let task_id = Uuid::new_v4().to_string();
    // 全局HashMap中加入task和sender
    if let Some(task_sender) = TASK_SENDER.get() {
        let mut task_sender_lock = task_sender.write();
        task_sender_lock.insert(task_id.clone(), task_tx);
    };
    // 全局RSP_LIST中加入rsp
    let rsp = Rsp { input: Some(input) };
    if let Some(rsp_list) = RSP_LIST.get() {
        let mut rsp_list_lock = rsp_list.write();
        rsp_list_lock.push_back((task_id, rsp));
    };
    // 等待回复
    match task_rx.await {
        Ok(req) => Ok(req.output),
        Err(e) => {
            log::error!("task rx err: {}", e);
            Err(e.into())
        }
    }
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
        // req header 中读取 task_id
        let request_metadata = request.metadata();
        let task_id_opt = request_metadata
            .get(X_TASK_ID)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());
        let req = request.into_inner();
        if let Some(ref _output) = req.output {
            // 通过channel发给函数调用者
            if let Some(task_sender) = TASK_SENDER.get() {
                let mut task_sender_lock = task_sender.write();
                if let Some(task_id) = task_id_opt {
                    if let Some(sender) = task_sender_lock.remove(&task_id) {
                        if let Err(e) = sender.send(req) {
                            log::error!("sender req err: {:?}", e);
                        };
                    };
                }
            }
        }
        // 获取一个待下发的任务
        if let Some(rsp_list) = RSP_LIST.get() {
            let mut rsp_list_lock = rsp_list.write();
            let rsp_opt = rsp_list_lock.pop_front();
            if let Some((task_id, rsp)) = rsp_opt {
                let mut response = Response::new(rsp.clone());
                if let Ok(metada_value) = MetadataValue::try_from(task_id.as_str()) {
                    response.metadata_mut().insert(X_TASK_ID, metada_value);
                };
                return Ok(response);
            }
        }
        return Ok(Response::new(Rsp { input: None }));
    }
}
