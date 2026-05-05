# Execution Plan — AWS Production Delivery

Delivering the architecture in `ARCHITECTURE_AWS.md` with a small team
(1 EM/me + 3 senior engineers + 0.5 SRE). Three two-week sprints to a
shadow-mode launch; one more for ramp + DR rehearsal.

## Milestones

### Sprint 1 — Foundations (week 1–2)
**Goal:** Aurora + CDC + Iceberg in dev; CI green; CDK pipeline live.

- AWS landing zone (Org / accounts / IAM baselines / KMS / VPC) — *SRE*.
- Aurora Postgres in dev with logical decoding ON; replicate the
  prototype's `init.sql` schema — *Eng A*.
- Kinesis stream + Debezium-on-MSK-Connect (or DMS) in dev — *Eng A*.
- Iceberg bootstrap on S3 with Glue catalog; partition spec
  (`tenant_id, event_date`) — *Eng B*.
- CDK monorepo + GitHub Actions: PR-gated unit tests, `cdk diff` on
  every PR, `cdk deploy` on merge to dev — *Eng C*.
- **I do**: design review on Iceberg partitioning, schema fingerprint
  contract, security model.

**Exit gate:** end-to-end happy path in dev — one row inserted in
Postgres lands in an Iceberg snapshot within 5 min.

### Sprint 2 — Pipeline (week 3–4)
**Goal:** Production-grade pipeline with crash-consistency,
deduplication, observability.

- Lambda micro-batcher: 1–2 min window, deterministic `run_id`
  computation, idempotent Iceberg commit — *Eng A*.
- DLQ + poison-pill quarantine; replay tooling — *Eng A*.
- EventBridge bus + per-tenant SNS+SQS topology — *Eng B*.
- DynamoDB cursor service with conditional-update advance semantics —
  *Eng B*.
- CloudWatch dashboards (lag, shard utilization, snapshot latency),
  alarms, structured logs — *Eng C*.
- **I do**: code review on the deduplication logic and the run_id
  port from the prototype; SLO definitions; on-call playbook v1.

**Exit gate:** 24-h soak at 1 K writes/sec sustained without lag
breach; chaos test of the batcher (kill mid-window) recovers cleanly.

### Sprint 3 — Consumer + launch readiness (week 5–6)
**Goal:** Public share API; load-tested at peak; DR rehearsed.

- API Gateway + Lambda `/share/changes` endpoint with cursor +
  pagination + ETag — *Eng B*.
- Athena+Iceberg integration for cold-path replay — *Eng C*.
- Load test at 5 K writes/sec + 1 K QPS reads on a shadow stack —
  *SRE + Eng A*.
- DR rehearsal: kill primary region, fail over via DDB Global Tables
  + S3 CRR; measure RTO — *SRE*.
- Docs: consumer SDK guide, runbook, schema-evolution policy —
  *Eng C + me*.
- **I do**: review consumer-API contract vs prototype manifest, sign
  off on canary criteria, executive readout of risks.

**Exit gate:** load test SLOs green; DR rehearsal RTO < 30 min;
runbook on file with owner.

## Quality gates

- **Per-PR**: unit tests, `cdk synth` lint, type-check, dependency
  scan (GH Advanced Security), commit-msg + branch policy.
- **Per-merge to main**: integration tests against an ephemeral
  AWS stack (CDK pipelines), Iceberg snapshot diff vs golden state.
- **Pre-deploy to staging**: schema-fingerprint compatibility check
  against the live consumer schema fingerprint.
- **Pre-deploy to prod**: synthetic canary on staging — continuous
  dry-run ingest + cursor freshness probe — must be green for 4 h.
- **Rollout**: canary deploy to one tenant for 24 h, then 1 % of
  traffic for 24 h, then full ramp. Automatic rollback on lag SLO
  breach.

## What I do vs delegate

- **Delegate (concrete, well-scoped):** CDK / IaC, Aurora and
  Kinesis day-2 ops, Lambda implementation, CloudWatch wiring, load
  tests, DR rehearsal logistics.
- **Do myself (cross-cutting, high-blast-radius):** the
  crash-consistency boundary (Iceberg commit + cursor advance
  ordering), `run_id` and schema-fingerprint contracts, SLO and
  alerting design, consumer API surface compatibility with the
  prototype, the post-launch retro and quality bar.

## Top 5 risks and mitigations

| Risk | Mitigation |
|---|---|
| Schema drift between source and lake silently corrupts consumers | Schema-fingerprint hard fail on mismatch; consumer must opt into a new fingerprint via runtime config; integration test asserts fingerprint stability per release. |
| Stream backpressure during 5 K/s peak causes lag SLO breach | Pre-provisioned shards sized for 2× peak; Lambda reserved concurrency; DLQ for poisoned records; lag alarm at 60 % of SLO with paging. |
| Iceberg compaction lag during sustained writes | Compaction job every 6 h with min/max file-size targets; alarm on file-count growth rate; auto-trigger ad-hoc compaction at threshold. |
| Multi-tenant noisy neighbor (one tenant = 99 % of write traffic) | Per-tenant rate limiting at the API Gateway and Kinesis shard partitioning by tenant; isolated DDB partition keys per tenant; per-tenant SLO dashboards. |
| Consumer cursor drift / orphaned cursors block replay | DDB TTL on inactive cursors; weekly reconciliation job; alert on cursors more than 24 h stale; documented reset procedure. |
