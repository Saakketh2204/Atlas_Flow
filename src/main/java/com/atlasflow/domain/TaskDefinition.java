package com.atlasflow.domain;

import java.util.List;
import java.util.Objects;

/**
 * One task within a {@link WorkflowDefinition}: which handler executes it,
 * and which other tasks (by ID) must complete first.
 *
 * @param taskId a unique identifier for this task within its workflow, e.g. "ReserveInventory"
 * @param handlerName the name of the registered handler that executes this task
 *                     (see {@code TaskHandlerRegistry}) -- kept as a plain string
 *                     rather than a class reference so workflow definitions stay
 *                     data (storable in DynamoDB / submittable as JSON) rather
 *                     than requiring code changes to add a new workflow shape
 * @param dependsOn task IDs that must be COMPLETED before this task becomes ready
 */
public record TaskDefinition(String taskId, String handlerName, List<String> dependsOn) {

    public TaskDefinition {
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(handlerName, "handlerName must not be null");
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        if (taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (handlerName.isBlank()) {
            throw new IllegalArgumentException("handlerName must not be blank");
        }
    }

    public static TaskDefinition rootTask(String taskId, String handlerName) {
        return new TaskDefinition(taskId, handlerName, List.of());
    }
}
