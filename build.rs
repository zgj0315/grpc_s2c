fn main() -> anyhow::Result<()> {
    tonic_build::compile_protos("proto/grpc_s2c_api.proto")?;
    Ok(())
}
