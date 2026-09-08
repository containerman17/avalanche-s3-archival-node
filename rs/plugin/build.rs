fn main() {
    let protos = [
        "vm/vm.proto",
        "vm/runtime/runtime.proto",
        "http/http.proto",
        "validatorstate/validator_state.proto",
        "appsender/appsender.proto",
        "sharedmemory/sharedmemory.proto",
        "rpcdb/rpcdb.proto",
        "warp/message.proto",
        "aliasreader/aliasreader.proto",
    ];
    for p in protos {
        println!("cargo:rerun-if-changed=proto/{p}");
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])
        .expect("protoc");
}
