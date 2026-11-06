# Cron timeout open issues

## Medium: verify timeout behavior against the deployed jobworkerp RPC server

- **Affected areas:** `modules/jobworkerp-client/src/client/wrapper.rs`, `modules/jobworkerp-client/src/client/helper.rs`, cron scheduler dispatch and manual execution.
- **Problem:** local client and worker code support per-job timeout values, but the deployed server's generated RPC schema and behavior have not been verified together. Workflow and worker enqueue, scheduler updates, Direct fallback, and `0` as unlimited therefore lack end-to-end confirmation against the production-compatible server version.
- **Impact:** a server version mismatch may reject or ignore the timeout field, retain an earlier timeout after cron updates, apply a different timeout on Direct fallback, or interpret zero as an immediate timeout rather than unlimited.
- **Verification procedure:** start a jobworkerp server matching the deployment version; enqueue workflow and worker jobs with unset, `0`, and a short positive timeout; inspect the accepted job data and observe runtime/stream behavior. Repeat after changing and clearing the cron timeout, and exercise a runner that uses Direct fallback. Confirm the server RPC descriptors include the timeout field and record the server version with results.
- **Implementation plan:** if the server rejects or misinterprets a value, align the client/server protobuf and timeout conversion semantics, add a version-matched integration fixture or CI service, and rerun the cases above before claiming end-to-end support.
- **Status:** deferred; unit, persistence, resolver propagation, and frontend tests pass, but the jobworkerp RPC endpoint is unavailable in the current test environment.
