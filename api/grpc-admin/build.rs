use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Compile the service interface protobuf files
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("jobworkerp_conductor_descriptor.bin")) // for reflection
        .compile_protos(
            &[
                // TODO proto file path
                "../../proto/protobuf/jobworkerp_conductor/service/common.proto",
                "../../proto/protobuf/jobworkerp_conductor/service/jobworkerp_server.proto",
                "../../proto/protobuf/jobworkerp_conductor/service/cron_scheduler.proto",
                "../../proto/protobuf/jobworkerp_conductor/service/worker_result_handler.proto",
                "../../proto/protobuf/jobworkerp_conductor/service/slack_event_handler.proto",
                "../../proto/protobuf/jobworkerp_conductor/service/config_management.proto",
                "../../proto/protobuf/jobworkerp_conductor/service/execution_status.proto",
            ],
            &["../../proto/protobuf/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {e:?}"));

    Ok(())
}
