# Architecture

## Overview

```
                         CLIENT
                           |
                           v
                 WorkflowController (REST)
                           |
                           v
              WorkflowOrchestrationService  <---- the scheduling brain
               /          |            \
              v           v             v
   DynamoDB executions  DynamoDB tasks   SQS (atlasflow-tasks)
   (WorkflowExecution)  (TaskExecution)        |
                                                v
                                    TaskEventListener (worker)
                                                |
                              idempotency claim (DynamoDB conditional write)
                                                |
                                     TaskHandler.handle(...)
                                          /            \
                                    success           failure
                                       |                 |
                          onTaskCompleted()      onTaskFailed()
                       (advance the DAG,        (retry w/ backoff,
                        dispatch next            or dead-letter after
                        ready tasks)              max attempts)

   Meanwhile, independently:
   FailureRecoveryEngine (control-plane) --- scans DynamoDB tasks table
   every 10s for RUNNING tasks whose lease has expired, and calls
   WorkflowOrchestrationService.reclaimOrphanedTask() on each one.
```

## The task dependency graph

The example used throughout this repo's tests, demo, and docs:

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

`WorkflowGraph.readyTasks(completed, inProgress)` (in the dependency-free
`core` package) is the single source of truth for "what can run right
now" -- both the initial dispatch on submission and every subsequent
dispatch after a task completes call the same method.

## Package layout and the core/framework split

```
com.atlasflow.core        pure algorithms, ZERO external dependencies
com.atlasflow.domain       plain records (also dependency-free)
com.atlasflow.config       @ConfigurationProperties
com.atlasflow.aws          DynamoDB/SQS/SNS client config + repositories
com.atlasflow.scheduler    the orchestration service + definition registry
com.atlasflow.worker       SQS polling, task handlers, heartbeat/lease claim
com.atlasflow.recovery     the failure-recovery scheduled scan
com.atlasflow.api          REST controller + DTOs + exception handling
```

`core` and `domain` are deliberately kept free of any Spring or AWS SDK
import. That's not an academic purity exercise -- see
[local-development.md](local-development.md) for the very concrete reason
it mattered while building this project, and `scripts/verify-core-logic.sh`
for how it's exploited: the hardest, most interview-relevant logic
(idempotency, retry/backoff, consistent hashing, DAG scheduling, lease
expiration) compiles and runs correctly with nothing but a JDK, completely
independent of whether Maven can reach a dependency repository.

## Why idempotency needs the *attempt number* in the key

`IdempotencyKey` is `(executionId, taskId, attempt)`, not just
`(executionId, taskId)`. This is the detail that makes the whole recovery
story actually correct, not just plausible-looking:

- SQS is at-least-once. A message can be redelivered for reasons that have
  nothing to do with failure (e.g. a worker that finished processing but
  crashed before it could call `DeleteMessage`).
- If the idempotency key didn't include the attempt number, EVERY retry of
  a task would collide with the SAME key as the original attempt --
  meaning after a task failed once, the retry would immediately be
  rejected as a "duplicate" of the original, and the task would never
  actually run again.
- Because each attempt gets its own key, a genuine retry (new attempt
  number) is allowed through, while a true duplicate delivery of the *same*
  attempt is correctly rejected.

This single design decision is exercised directly by
`tests/test_idempotency` in the core suite and by
`ChaosRecoveryIntegrationTest.aTaskOrphanedByACrashedWorkerIsAutomatically
ReclaimedAndBecomesClaimableAgain`, which asserts the reclaimed task's new
attempt is freshly claimable — i.e., proves this bug class doesn't exist,
rather than just asserting the happy path works.

## Retry and dead-lettering

`core.retry.RetryPolicy` implements exponential backoff with full jitter
(AWS's documented "Exponential Backoff And Jitter" pattern): each retry's
delay is a random value between 0 and `min(maxDelay, baseDelay * 2^attempt)`.
Full jitter (not "no jitter" or "equal jitter") specifically avoids
synchronized retry storms when many tasks fail at once.

Retries are delivered via SQS's native per-message `DelaySeconds`, not by
blocking a worker thread -- a failed task's worker goes straight back to
polling for other work; the delayed message simply isn't visible to any
consumer until its delay elapses. Once `RetryPolicy.shouldRetry` returns
false, the task is marked `FAILED`, its owning execution is marked
`FAILED`, and the message is routed to `atlasflow-tasks-dlq`.

The exact same retry-budget logic applies whether a task failed by
throwing an exception (`onTaskFailed`) or by its worker's lease simply
expiring (`reclaimOrphanedTask`) -- both eventually call the same
`RetryPolicy.shouldRetry` check, so "how many times will AtlasFlow retry a
task" has one answer regardless of *why* an attempt didn't succeed.

## Partitioned scheduling (consistent hashing)

`core.partition.ConsistentHashPartitioner` is included for the scenario
where multiple scheduler/control-plane instances divide ownership of
workflow executions among themselves (e.g. `hash(executionId)` decides
which instance's scan picks up a given execution). It is NOT currently
wired into `WorkflowOrchestrationService` -- this repo runs a single
control-plane instance, and multi-instance partitioning is a real "next
step," not a component I bolted on without connecting anything to it.
The class and its test suite exist because the underlying idea (why
consistent hashing bounds reshuffling to ~1/N of keys on scale events,
versus almost all of them with plain `hash % N`) is worth having a
correct, tested implementation of, and is exactly the kind of question a
distributed-systems interview asks about directly.

## What's deliberately out of scope

Kept out to keep this project's core story (idempotent scheduling +
recovery) sharp rather than diluted across 14 loosely-connected features:

- **Multi-region failover.** Real AWS spend, slow to build, and a
  materially different (and separately interesting) problem from
  single-region fault tolerance.
- **Live EKS deployment / autoscaling.** The Dockerfile and
  docker-compose.yml prove the containerization story locally; deploying
  to a real EKS cluster is an infrastructure exercise, not a scheduling
  one.
- **Workflow definition persistence/versioning.** `WorkflowDefinitionRegistry`
  is in-memory (see its Javadoc) -- a real CRUD/versioning subsystem
  would be a natural next step, not core to what this project demonstrates.
- **Distributed cancellation of in-flight tasks.** `POST
  /executions/{id}/cancel` stops the workflow from advancing further but
  doesn't interrupt a task a worker is already executing.

## Local development without Maven Central or Docker

See [local-development.md](local-development.md) for the full story,
including exactly what could and couldn't be verified in this project's
original development environment.
