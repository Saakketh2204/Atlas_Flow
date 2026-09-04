package com.atlasflow.core.workflow;

import java.util.List;
import java.util.Objects;

/**
 * One node in a workflow's task dependency graph: a task ID and the IDs of
 * the tasks that must complete before this one may start.
 *
 * Example (the ProcessOrder workflow used throughout this repo's demo/tests):
 * <pre>
 *   ValidatePayment (no dependencies)
 *         |
 *         v
 *   ReserveInventory (depends on: ValidatePayment)
 *      /        \
 *     v          v
 *   Ship        Notify        (both depend on: ReserveInventory)
 *     \          /
 *      v        v
 *     CompleteOrder            (depends on: Ship, Notify)
 * </pre>
 */
public final class TaskNode {

    private final String taskId;
    private final List<String> dependsOn;

    public TaskNode(String taskId, List<String> dependsOn) {
        this.taskId = Objects.requireNonNull(taskId, "taskId must not be null");
        this.dependsOn = List.copyOf(Objects.requireNonNull(dependsOn, "dependsOn must not be null"));
    }

    public String taskId() {
        return taskId;
    }

    public List<String> dependsOn() {
        return dependsOn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskNode other)) return false;
        return taskId.equals(other.taskId) && dependsOn.equals(other.dependsOn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, dependsOn);
    }

    @Override
    public String toString() {
        return taskId + (dependsOn.isEmpty() ? "" : " (depends on: " + String.join(", ", dependsOn) + ")");
    }
}
