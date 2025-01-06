use std::collections::VecDeque;

use grpc_s2c::grpc_s2c_api::rsp::Input;
use grpc_s2c::grpc_s2c_api::{
    grpc_s2c_api_client::GrpcS2cApiClient, req, Input001, Input002, Output001, Output002, Req,
};
use grpc_s2c::grpc_s2c_api::{req_task, ReqTask, RspTask};
use grpc_s2c::X_TASK_ID;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{transport::Channel, Request};

static TASK_TODO: OnceCell<Mutex<VecDeque<RspTask>>> = OnceCell::new();
static TASK_DONE: OnceCell<Mutex<VecDeque<ReqTask>>> = OnceCell::new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();
    if let Err(e) = TASK_TODO.set(VecDeque::new().into()) {
        log::error!("TASK_DOTO set err: {:?}", e);
    };
    if let Err(e) = TASK_DONE.set(VecDeque::new().into()) {
        log::error!("TASK_DONE set err: {:?}", e);
    };
    let client = GrpcS2cApiClient::connect("http://127.0.0.1:50051").await?;
    let client_clone = client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            log::info!("do_task_by_bidirectional run");
            interval.tick().await;
            if let Err(e) = do_task_by_bidirectional(client_clone.clone()).await {
                log::error!("do_task_by_bidirectional err: {}", e);
            };
        }
    });

    // exec_fn_by_unary_one_by_one(client).await?;
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        handle_task().await?;
    }
    // Ok(())
}

async fn handle_task() -> anyhow::Result<()> {
    log::info!("handle_task run");
    loop {
        let rsp_task_op = match TASK_TODO.get() {
            Some(task_todo) => {
                let mut task_todo_lock = task_todo.lock();
                task_todo_lock.pop_front()
            }
            None => None,
        };
        log::info!("rsp_task_op: {:?}", rsp_task_op);
        match rsp_task_op {
            Some(rsp_task) => {
                log::info!("handle task: {:?}", rsp_task);
                let output = if let Some(input) = rsp_task.input {
                    match input {
                        grpc_s2c::grpc_s2c_api::rsp_task::Input::Input001(input001) => {
                            let output = fn_001(&input001).await?;
                            Some(req_task::Output::Output001(output))
                        }
                        grpc_s2c::grpc_s2c_api::rsp_task::Input::Input002(input002) => {
                            let output = fn_002(&input002).await?;
                            Some(req_task::Output::Output002(output))
                        }
                    }
                } else {
                    None
                };

                let req_task = ReqTask {
                    task_id: rsp_task.task_id,
                    output,
                };
                if let Some(task_done) = TASK_DONE.get() {
                    let mut task_done_lock = task_done.lock();
                    task_done_lock.push_back(req_task);
                }
            }
            None => {
                break;
            }
        }
    }
    Ok(())
}

async fn do_task_by_bidirectional(mut client: GrpcS2cApiClient<Channel>) -> anyhow::Result<()> {
    log::info!("do_task_by_bidirectional start");
    let outbound = async_stream::stream! {
        loop {
            let req_task_op = match TASK_DONE.get() {
                Some(task_done) => {
                    let mut task_done_lock = task_done.lock();
                    task_done_lock.pop_front()
                }
                None => None,
            };
            match req_task_op {
                Some(req_task) => {
                    yield req_task;
                }
                None => {
                    break;
                }
            }
        }
    };
    let stream_req = Request::new(outbound);
    log::info!("send req to recever");
    // 接收server下发的信息
    let rsp = client.bidirectional(stream_req).await?;
    let mut rsp_stream = rsp.into_inner();
    while let Some(received) = rsp_stream.next().await {
        log::info!("get rsp_stream");
        match received {
            Ok(rpc_fn_req) => {
                log::info!("get task: {}", rpc_fn_req.task_id);
                if let Some(task_todo) = TASK_TODO.get() {
                    let mut task_todo_lock = task_todo.lock();
                    task_todo_lock.push_back(rpc_fn_req);
                }
            }
            Err(e) => {
                log::error!("err: {}", e);
            }
        }
    }
    log::info!("do_task_by_bidirectional end");
    // tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    Ok(())
}
async fn _exec_fn_by_unary_one_by_one(mut client: GrpcS2cApiClient<Channel>) -> anyhow::Result<()> {
    let mut do_next = true;
    // 带着一个空消息，去找Server要一个task
    let mut req = Req { output: None };
    let mut task_id_opt: Option<String> = None;
    while do_next {
        let mut request = Request::new(req.clone());

        // req header中写入task_id
        if let Some(task_id) = task_id_opt {
            let request_metadata = request.metadata_mut();
            let metadata_value = MetadataValue::try_from(task_id.as_str())?;
            request_metadata.insert(X_TASK_ID, metadata_value);
        }
        let response = client.unary(request).await?;
        // rsp header中读取task_id
        let response_metadata = response.metadata();
        task_id_opt = response_metadata
            .get(X_TASK_ID)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());

        let rsp = response.into_inner();
        if let Some(ref input) = rsp.input {
            match input {
                Input::Input001(input_001) => {
                    let output_001 = fn_001(input_001).await?;
                    req = Req {
                        output: Some(req::Output::Output001(output_001)),
                    };
                }
                Input::Input002(input_002) => {
                    let output_002 = fn_002(input_002).await?;
                    req = Req {
                        output: Some(req::Output::Output002(output_002)),
                    };
                }
            }
        } else {
            req = Req { output: None };
            do_next = false;
        }
    }
    Ok(())
}

async fn fn_001(input: &Input001) -> anyhow::Result<Output001> {
    // for _ in 0..5 {
    //     log::info!("exec fn 001: {:?}", input);
    //     tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    // }
    // tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    // 把执行结果，返回给Server，同时得到一个新的task
    let output = Output001 {
        msg: format!("finsih {}", input.msg),
    };
    Ok(output)
}

async fn fn_002(input: &Input002) -> anyhow::Result<Output002> {
    // for _ in 0..5 {
    //     log::info!("exec fn 002: {:?}", input);
    //     tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    // }
    // tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    // 把执行结果，返回给Server，同时得到一个新的task
    let output = Output002 {
        code: 1,
        msg: format!("finsih {}", input.name),
    };
    Ok(output)
}
