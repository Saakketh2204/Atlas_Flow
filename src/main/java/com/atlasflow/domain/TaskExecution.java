package com.atlasflow.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * The runtime state of one task within one workflow execution, including
 * the lease/heartbeat fields the failure-recovery engine relies on:
 * {@code workerId} + {@code leaseExpiresAt} together record "who currently
 * owns this task, and until when" -- see {@code core.lease.LeaseEvaluator}
 * for the pure expiration-check logic, and {@code recovery.FailureRecoveryEngine}
 * for how an expired lease turns back into a schedulable task.
 */
public record TaskExecution(
        String executionId,
        String taskId,
        TaskStatus status,
        int attempt,
        Optional<String> workerId,
        Optional<Instant> leaseExpiresAt,
        Optional<String> lastError,
        Instant updatedAt) {

    public TaskExecution {
        Objects.requireNonNull(executionId, "executionId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        workerId = workerId == null ? Optional.empty() : workerId;
        leaseExpiresAt = leaseExpiresAt == null ? Optional.empty() : leaseExpiresAt;
        lastError = lastError == null ? Optional.empty() : lastError;
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be >= 0");
        }
    }

    public static TaskExecution pending(String executionId, String taskId, Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.PENDING, 0, Optional.empty(), Optional.empty(), Optional.empty(), now);
    }

    /** Transition to SCHEDULED for a new attempt (dispatched onto the event bus, no worker claimed yet).
     * Preserves any existing {@code lastError} -- if this attempt follows a
     * failed one, that failure's reason stays visible (useful for anyone
     * checking status mid-retry) until the task actually succeeds via
     * {@link #withCompleted}, which is what clears it. */
    public TaskExecution withScheduled(Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.SCHEDULED, attempt + 1, Optional.empty(), Optional.empty(),
                lastError, now);
    }

    /** Transition to RUNNING once a worker claims this attempt and establishes a lease. */
    public TaskExecution withClaimedBy(String workerId, Instant leaseExpiresAt, Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.RUNNING, attempt, Optional.of(workerId),
                Optional.of(leaseExpiresAt), Optional.empty(), now);
    }

    public TaskExecution withHeartbeat(Instant newLeaseExpiresAt, Instant now) {
        return new TaskExecution(
                executionId, taskId, status, attempt, workerId, Optional.of(newLeaseExpiresAt), lastError, now);
    }

    public TaskExecution withCompleted(Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.COMPLETED, attempt, workerId, Optional.empty(), Optional.empty(), now);
    }

    public TaskExecution withRetrying(String error, Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.RETRYING, attempt, Optional.empty(), Optional.empty(),
                Optional.of(error), now);
    }

    public TaskExecution withFailed(String error, Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.FAILED, attempt, Optional.empty(), Optional.empty(),
                Optional.of(error), now);
    }

    /** Transition back to PENDING after the recovery engine reclaims an orphaned (lease-expired) task. */
    public TaskExecution withReclaimed(Instant now) {
        return new TaskExecution(
                executionId, taskId, TaskStatus.PENDING, attempt, Optional.empty(), Optional.empty(),
                Optional.of("reclaimed after worker lease expired"), now);
    }
}
