# Database Migrations

How the conductor schema evolves, and what to do when you change it.

## SQLite

SQLite migrations are applied automatically at startup.

- `api/infra/sql/sqlite/` is embedded as sqlx versioned migrations. Applied versions are recorded in the `_sqlx_migrations` table.
- A database created before migrations were tracked has no history. At startup, any baseline columns (`002_schema.sql`) missing from its existing tables are added, and then `002` is recorded as applied.
- An existing database that needs changes is first copied to `<database file>.pre-migration.bak`. Only one generation is kept. If the migration fails, the error message includes the backup path.

### Changing the schema

1. Add a new file, `api/infra/sql/sqlite/NNN_<description>.sql`, numbered after the latest one.
2. Never edit `002_schema.sql` or any other applied migration. Applied databases store each file's checksum, and an edited file makes startup fail.
3. Add a test that migrates a database from the previous version and checks that existing rows are kept.

`sqlite/` in this directory holds the manual SQLite migrations used before automatic migration existed. It is kept for reference only.

## MySQL

MySQL schemas are not changed at startup. Operators apply migrations by hand.

- New databases are initialized from `api/infra/sql/mysql/002_schema.sql`.
- Existing databases are upgraded with the files in `mysql/` in this directory, applied in numeric order, each exactly once.

### Changing the schema

1. Update `api/infra/sql/mysql/002_schema.sql` so new databases get the full schema.
2. Add `mysql/NNN_<description>.sql` with the incremental change for existing databases. If the statement is not idempotent, say so in a comment.
3. Describe in the pull request any deployment-order constraints between the migration, the backend, and the configuration clients.

### Applying to an existing database

1. Stop the application and any clients that write configuration.
2. Back up the database.
3. Apply the pending files in `mysql/` in numeric order, following any conditions written in each file.
4. Deploy the backend and configuration clients that match the new schema.
5. Before resuming writes, check that the service starts, existing data reads back, and the changed feature saves and reloads correctly.
