fn main() {
    // `sqlx::migrate!` embeds these files; rebuild when they change.
    println!("cargo:rerun-if-changed=sql/sqlite");
}
