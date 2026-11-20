# Schema v1
SQLAlchemy metadata creates users, audits, workloads, candidates, experiments and telemetry on first startup. Workload/candidate natural names are UNIQUE; experiment snapshots are immutable JSON. Foreign keys prevent orphan references. Index trials affect only the dedicated telemetry table, under MySQL GET_LOCK, and drop the trial index after each execution. No production database connection is accepted.

# v2 — 重采样对照
experiments gains `kind` (`index` / `comparison`), `attempt` (counter for resume) and `evidence` (JSON: last checkpoint payload + checkpoint list + superseded attempts). Existing rows default to `kind='index'`, `attempt=1`, no evidence. Applied idempotently by `src/config/schema.py::upgrade` on startup, so existing data volumes upgrade without a rebuild. Comparison runs use a per-experiment copy table `telemetry_cmp_<id>` in the same schema and require the app account to hold `FLUSH_TABLES` (granted by `backend/scripts/mysql-init.sql` on a fresh volume).
