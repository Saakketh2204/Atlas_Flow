package com.atlasflow.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * The runtime state of one execution of a {@link WorkflowDefinition}.
 * Immutable -- state transitions produce a new instance (via the
 * {@code with*} methods) rather than mutating in place, which mirrors how
 * the DynamoDB-backed repository actually persists changes: read the
 * current item, compute the next state, write it back.
 *
 * @param completedTaskIds task IDs that have finished successfully -- this
 *                          is exactly the {@code completed} set that
 *                          {@code WorkflowGraph.readyTasks} needs to compute
 *                          what to schedule next
 */
public record WorkflowExecution(
        String executionId,
        String workflowDefinitionId,
        ExecutionStatus status,
        Set<String> completedTaskIds,
        Instant createdAt,
        Instant updatedAt) {

    public WorkflowExecution {
        Objects.requireNonNull(executionId, "executionId must not be null");
        Objects.requireNonNull(workflowDefinitionId, "workflowDefinitionId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        completedTaskIds = completedTaskIds == null ? Set.of() : Set.copyOf(completedTaskIds);
    }

    public static WorkflowExecution newExecution(String executionId, String workflowDefinitionId, Instant now) {
        return new WorkflowExecution(executionId, workflowDefinitionId, ExecutionStatus.RUNNING, Set.of(), now, now);
    }

    public WorkflowExecution withTaskCompleted(String taskId, Instant now) {
        Set<String> updated = new java.util.HashSet<>(completedTaskIds);
        updated.add(taskId);
        return new WorkflowExecution(executionId, workflowDefinitionId, status, updated, createdAt, now);
    }

    public WorkflowExecution withStatus(ExecutionStatus newStatus, Instant now) {
        return new WorkflowExecution(executionId, workflowDefinitionId, newStatus, completedTaskIds, createdAt, now);
    }
}
