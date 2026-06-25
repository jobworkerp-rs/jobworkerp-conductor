use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Compile the basic protobuf files
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("jobworkerp_conductor_descriptor.bin")) // for reflection
        .compile_protos(
            &[
                "protobuf/jobworkerp_conductor/data/jobworkerp_server.proto",
                "protobuf/jobworkerp_conductor/data/common.proto",
                "protobuf/jobworkerp_conductor/data/cron_scheduler.proto",
                "protobuf/jobworkerp_conductor/data/worker_result_handler.proto",
                "protobuf/jobworkerp_conductor/data/slack_event_handler.proto",
                "protobuf/jobworkerp_conductor/data/execution_ref.proto",
                "protobuf/jobworkerp_conductor/data/event.proto",
            ],
            &["protobuf"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {e:?}"));

    Ok(())
}
