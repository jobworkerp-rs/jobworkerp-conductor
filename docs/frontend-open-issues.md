# Frontend open issues

## Medium: ESLint dependency override causes lint startup failure

- **Affected files:** `frontend/package.json`, `frontend/pnpm-lock.yaml`
- **Problem:** the `brace-expansion@^5` override is resolved below `@eslint/config-array`, which expects the `minimatch`-compatible function export. ESLint startup fails with `TypeError: expand is not a function`.
- **Implementation plan:** review the current security requirements and dependency tree; replace or narrow the override with a compatible version, regenerate the lockfile, then run a frozen install followed by frontend lint, tests, and type checking.
- **Status:** deferred from the cron timeout change; frontend lint remains blocked until this is resolved.
