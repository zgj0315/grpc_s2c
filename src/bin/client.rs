use grpc_s2c::grpc_s2c_api::rsp::Input;
use grpc_s2c::grpc_s2c_api::{
    grpc_s2c_api_client::GrpcS2cApiClient, req, Input001, Input002, Output001, Output002, Req,
};
use grpc_s2c::X_TASK_ID;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{transport::Channel, Request};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_line_number(true).init();
    let client = GrpcS2cApiClient::connect("http://127.0.0.1:50051").await?;
    // do_task_by_bidirectional(client).await?;
    // do_task_by_unary(client).await?;
    exec_fn_by_unary_one_by_one(client).await?;
    Ok(())
}
async fn _do_task_by_bidirectional(mut client: GrpcS2cApiClient<Channel>) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(10);
    let outbound = async_stream::stream! {
        while let Some(rpc_fn_req) = rx.recv().await {
           log::info!("client get req: {:?}", rpc_fn_req);
           log::info!("do some work and send rsp to server");
           tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
           let output = "hello server";
           log::info!("client send rsp to server: {}", output);
           let stream_req = Req {
               output: Some(req::Output::Output001(Output001 {
                  task_id: "".to_string(),
                  msg: "".to_string(),
               })),
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

async fn _do_task_by_unary(mut client: GrpcS2cApiClient<Channel>) -> anyhow::Result<()> {
    // 带着一个空消息，去找Server要一个task
    let mut req = Req { output: None };
    loop {
        // log::info!("send task output to server: {:?}", req);
        // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let response = client.unary(req.clone()).await?;
        let rsp = response.into_inner();
        if let Some(input) = rsp.input {
            match input {
                Input::Input001(input_001) => {
                    // log::info!("get task from server: {:?}", input_001);
                    // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    for _ in 0..5 {
                        log::info!("do some work for task: {:?}", input_001);
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    // 把执行结果，返回给Server，同时得到一个新的task
                    req = Req {
                        output: Some(req::Output::Output001(Output001 {
                            task_id: input_001.task_id,
                            msg: format!("finsih {}", input_001.msg),
                        })),
                    };
                }
                Input::Input002(_) => {}
            }
        } else {
            req = Req { output: None };
        }
    }
}

async fn exec_fn_by_unary_one_by_one(mut client: GrpcS2cApiClient<Channel>) -> anyhow::Result<()> {
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
    for _ in 0..5 {
        log::info!("exec fn 001: {:?}", input);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    // 把执行结果，返回给Server，同时得到一个新的task
    let output = Output001 {
        task_id: input.task_id.clone(),
        msg: format!("finsih {}", input.msg),
    };
    Ok(output)
}

async fn fn_002(input: &Input002) -> anyhow::Result<Output002> {
    for _ in 0..5 {
        log::info!("exec fn 002: {:?}", input);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    // 把执行结果，返回给Server，同时得到一个新的task
    let output = Output002 {
        task_id: input.task_id.clone(),
        msg: format!("finsih {}", input.name),
    };
    Ok(output)
}
