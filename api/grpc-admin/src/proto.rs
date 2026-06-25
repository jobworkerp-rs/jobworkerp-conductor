pub mod jobworkerp_conductor {
    // type alias作っておかないと proto自動生成コード内部の依存関係がおかしくなる
    // (protoの自動生成コード内でsuperでクラス参照解決していたため擬似的にdataクラスの位置関係があうようにする)
    pub mod data {
        use proto::jobworkerp_conductor::data;
        pub type WorkerResultHandlerId = data::WorkerResultHandlerId;
        pub type WorkerResultHandlerData = data::WorkerResultHandlerData;
        pub type WorkerResultHandler = data::WorkerResultHandler;
        pub type JobworkerpServerId = data::JobworkerpServerId;
        pub type JobworkerpServerData = data::JobworkerpServerData;
        pub type JobworkerpServer = data::JobworkerpServer;
        pub type CronSchedulerId = data::CronSchedulerId;
        pub type CronSchedulerData = data::CronSchedulerData;
        pub type CronScheduler = data::CronScheduler;
        pub type SlackEventHandlerId = data::SlackEventHandlerId;
        pub type SlackEventHandlerData = data::SlackEventHandlerData;
        pub type SlackEventHandler = data::SlackEventHandler;
        pub type ExecutionRefId = data::ExecutionRefId;
        pub type ExecutionRef = data::ExecutionRef;
        pub type ExecutionRuntimeStatus = data::ExecutionRuntimeStatus;
        pub type ExecutionSourceType = data::ExecutionSourceType;
        pub type ResolvedExecutionStatus = data::ResolvedExecutionStatus;
        pub type ExecutionStatusSource = data::ExecutionStatusSource;
    }
    pub mod service {
        tonic::include_proto!("jobworkerp_conductor.service");
    }
}

// for reflection
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("jobworkerp_conductor_descriptor");
