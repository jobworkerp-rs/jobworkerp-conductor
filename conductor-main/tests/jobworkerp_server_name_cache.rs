//! FindByName is served from a memory cache; updates and deletes must not leave
//! a stale entry behind under the old or the new name.

#[path = "support/conductor_process.rs"]
mod conductor_process;

use conductor_process::{find_server_by_name, server_data, ConductorProcess};
use grpc_admin::proto::jobworkerp_conductor::data::JobworkerpServer;

#[tokio::test]
async fn find_by_name_sees_an_update() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let id = client
        .create(server_data("a", "9000"))
        .await
        .expect("create")
        .into_inner()
        .id;
    find_server_by_name(&mut client, "a").await.expect("cached");

    client
        .update(JobworkerpServer {
            id,
            data: Some(server_data("a", "9100")),
        })
        .await
        .expect("update");

    let found = find_server_by_name(&mut client, "a").await.expect("found");
    assert_eq!(found.data.expect("data").port, "9100");
}

#[tokio::test]
async fn find_by_name_forgets_the_old_name_after_a_rename() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let id = client
        .create(server_data("old", "9000"))
        .await
        .expect("create")
        .into_inner()
        .id;
    find_server_by_name(&mut client, "old")
        .await
        .expect("cached");

    client
        .update(JobworkerpServer {
            id,
            data: Some(server_data("new", "9000")),
        })
        .await
        .expect("rename");

    assert!(find_server_by_name(&mut client, "old").await.is_none());
    assert!(find_server_by_name(&mut client, "new").await.is_some());
}

#[tokio::test]
async fn find_by_name_forgets_a_deleted_server() {
    let mut conductor = ConductorProcess::spawn();
    let mut client = conductor.admin_client().await;
    let id = client
        .create(server_data("gone", "9000"))
        .await
        .expect("create")
        .into_inner()
        .id
        .expect("id");
    find_server_by_name(&mut client, "gone")
        .await
        .expect("cached");

    client.delete(id).await.expect("delete");

    assert!(find_server_by_name(&mut client, "gone").await.is_none());
}
