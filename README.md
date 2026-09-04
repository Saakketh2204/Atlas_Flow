# AtlasFlow

**A fault-tolerant distributed workflow orchestration platform**, built
from scratch in Java/Spring Boot: partitioned scheduling primitives,
idempotent event-driven task dispatch over SQS, exponential-backoff
retries with dead-lettering, and automatic lease-based recovery from
worker failures — no manual intervention required.

![CI](https://github.com/your-username/atlasflow/actions/workflows/ci.yml/badge.svg)
![Java](https://img.shields.io/badge/java-21-orange)
![License](https://img.shields.io/badge/license-MIT-green)

> **Before anything else:** this README includes an explicit,
> unglamorous account of exactly what was and wasn't compiled/run during
> development, and why — see
> [`docs/local-development.md`](docs/local-development.md). Read that
> first if you're evaluating this project; the short version is at the
> bottom of this file too.

## What it does

Submit a workflow like this:

```
ValidatePayment
      |
      v
ReserveInventory
    /     \
   v       v
 Ship     Notify
   \       /
    v     v
 CompleteOrder
```

and AtlasFlow schedules each task the moment its dependencies complete,
dispatches it over SQS to whichever worker picks it up, deduplicates
redelivered messages so nothing runs twice, retries transient failures
with jittered exponential backoff, and — if a worker dies mid-task —
automatically detects the orphaned work via lease expiration and hands it
to another worker, with no human ever paging in.

## Why this exists

Built specifically to demonstrate the distributed-systems fundamentals
that show up repeatedly across Amazon SDE1/SDE2 postings regardless of
team (retail, AWS core services, fulfillment, fintech): partitioned,
event-driven backend systems in Java, built on SQS/SNS/DynamoDB, with
idempotent processing, retry/backoff, and automated failure recovery —
see [`docs/architecture.md`](docs/architecture.md) for the full design
rationale and what was deliberately left out of scope.

## Architecture

```
CLIENT -> REST API -> WorkflowOrchestrationService -> DynamoDB (execution + task state)
                              |                              ^
                              v                              |
                        SQS (atlasflow-tasks)          FailureRecoveryEngine
                              |                        (scans for expired
                              v                         leases every 10s,
                    TaskEventListener (worker)          reclaims orphaned
                    - idempotency claim                 tasks automatically)
                    - execute handler
                    - heartbeat/renew lease
                    - report success/failure
```

Full diagram and design rationale: [`docs/architecture.md`](docs/architecture.md).

## Core techniques implemented from scratch

| Technique | Where | Why it's correct |
|---|---|---|
| Idempotent processing under at-least-once delivery | `core.idempotency.IdempotencyKey` + `aws.DynamoIdempotencyStore` | DynamoDB conditional write (`attribute_not_exists`); key includes the **attempt number**, so retries are allowed but redeliveries of the same attempt are rejected — see the worked explanation in `docs/architecture.md` |
| Exponential backoff with full jitter | `core.retry.RetryPolicy` | AWS's documented "Exponential Backoff And Jitter" pattern; delivered via SQS `DelaySeconds`, non-blocking |
| DAG scheduling | `core.workflow.WorkflowGraph` | Cycle detection + topological ordering (Kahn's algorithm); `readyTasks()` is the single source of truth used by every dispatch decision |
| Lease-based failure detection | `core.lease.LeaseEvaluator` + `recovery.FailureRecoveryEngine` | No crash notification exists — a worker's silence (an unrenewed lease) *is* the failure signal |
| Consistent hashing | `core.partition.ConsistentHashPartitioner` | Virtual-node ring; adding/removing an owner reshuffles ~1/N of keys, not almost all of them (unlike `hash % N`) |

**Every technique in that table is unit-tested, and the five in `core.*`
are additionally verified with nothing but a JDK** — see below.

## Verification: two tiers, on purpose

```bash
./scripts/verify-core-logic.sh
```
Compiles and runs the dependency-free core algorithms (retry/backoff,
consistent hashing, DAG scheduling, idempotency keys, lease expiration,
and domain state transitions) with plain `javac`/`java` — **45 checks,
actually run during this project's development, no Maven or network
required.**

```bash
mvn test      # Mockito-based unit tests for the orchestration service
mvn verify    # + Testcontainers integration test against real LocalStack
```
The full Spring Boot/AWS SDK layer, validated by `.github/workflows/ci.yml`
on every push (GitHub's runners have the internet + Docker access this
project's original dev sandbox didn't — see
[`docs/local-development.md`](docs/local-development.md) for exactly what
that means and why it's called out this explicitly).

## Quick start

```bash
git clone https://github.com/your-username/atlasflow.git
cd atlasflow

# Whole platform, locally, no AWS account needed (LocalStack stands in
# for DynamoDB/SQS):
docker compose up --build

# In another terminal:
curl -X POST localhost:8080/executions \
  -H 'Content-Type: application/json' \
  -d '{"workflowDefinitionId": "process-order-v1"}'
# -> {"executionId": "..."}

curl localhost:8080/executions/<executionId>
```

Scale workers to see concurrent processing: `docker compose up --scale worker=3`.

Demo the retry path deterministically: see the chaos-injection env vars
in `docker-compose.yml` and [`docs/local-development.md`](docs/local-development.md).

## REST API

```
POST /workflows                    register a workflow definition
GET  /workflows                    list registered definitions
POST /executions                   start an execution
GET  /executions/{id}              full execution + per-task status
GET  /executions/{id}/status       lightweight status summary
POST /executions/{id}/cancel       cancel a running execution
```

## Project structure

```
atlasflow/
├── pom.xml
├── Dockerfile                    multi-stage: Maven build -> slim JRE runtime
├── docker-compose.yml            LocalStack + control-plane + worker
├── infra/localstack/init-aws.sh  creates DynamoDB tables + SQS queues locally
├── scripts/verify-core-logic.sh  offline core-logic verification (no Maven)
├── tools/offline-verify/         the dependency-free verification harness
├── src/main/java/com/atlasflow/
│   ├── core/                     pure algorithms, ZERO external dependencies
│   │   ├── retry/                 exponential backoff + jitter
│   │   ├── partition/              consistent hashing
│   │   ├── workflow/               DAG scheduling + cycle detection
│   │   ├── idempotency/            idempotency key construction
│   │   └── lease/                  lease expiration logic
│   ├── domain/                   plain records (also dependency-free)
│   ├── config/                   @ConfigurationProperties
│   ├── aws/                      DynamoDB/SQS/SNS client config + repositories
│   ├── scheduler/                the orchestration service
│   ├── worker/                   SQS polling, task handlers, lease claim/heartbeat
│   ├── recovery/                 the failure-recovery scheduled scan
│   └── api/                      REST controller + DTOs
├── src/test/java/com/atlasflow/
│   ├── core/, scheduler/         JUnit 5 / Mockito unit tests
│   └── integration/              Testcontainers + LocalStack integration test
├── docs/
│   ├── architecture.md
│   └── local-development.md      <- read this for the verification story
└── .github/workflows/ci.yml
```

## Tech stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.2 |
| Messaging | Amazon SQS (event-driven task dispatch), SNS-ready |
| State | Amazon DynamoDB (raw SDK v2 client, explicit conditional writes) |
| Local dev | LocalStack, Docker Compose |
| Testing | JUnit 5, Mockito, Testcontainers |
| CI/CD | GitHub Actions |

## License

[MIT](LICENSE)
