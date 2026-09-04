package com.atlasflow.domain;

import com.atlasflow.core.workflow.TaskNode;
import com.atlasflow.core.workflow.WorkflowGraph;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A named, reusable workflow shape: an ordered list of {@link TaskDefinition}s.
 * Submitting this same definition multiple times creates multiple independent
 * {@link WorkflowExecution}s (e.g. one ProcessOrder definition, one execution
 * per customer order).
 *
 * @param workflowDefinitionId stable identifier for this workflow shape, e.g. "process-order-v1"
 * @param name human-readable display name
 * @param tasks the task graph -- validated for cycles/dangling dependencies at construction time
 */
public record WorkflowDefinition(String workflowDefinitionId, String name, List<TaskDefinition> tasks) {

    public WorkflowDefinition {
        Objects.requireNonNull(workflowDefinitionId, "workflowDefinitionId must not be null");
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(tasks, "tasks must not be null");
        tasks = List.copyOf(tasks);
        if (workflowDefinitionId.isBlank()) {
            throw new IllegalArgumentException("workflowDefinitionId must not be blank");
        }
        // Fail fast: validate the graph shape (cycles, dangling deps) at
        // definition time, not the first time someone tries to schedule it.
        toGraph(tasks);
    }

    /** Builds the (validated) core-package graph representation used for scheduling. */
    public WorkflowGraph toGraph() {
        return toGraph(tasks);
    }

    private static WorkflowGraph toGraph(List<TaskDefinition> tasks) {
        List<TaskNode> nodes = tasks.stream()
                .map(t -> new TaskNode(t.taskId(), t.dependsOn()))
                .collect(Collectors.toList());
        return new WorkflowGraph(nodes);
    }

    public TaskDefinition task(String taskId) {
        return tasks.stream()
                .filter(t -> t.taskId().equals(taskId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no such task: " + taskId));
    }

    /**
     * The canonical example used throughout this repo's docs, tests, and demo
     * scripts: ValidatePayment -> ReserveInventory -> {Ship, Notify} -> CompleteOrder.
     */
    public static WorkflowDefinition processOrderExample() {
        return new WorkflowDefinition(
                "process-order-v1",
                "Process Order",
                List.of(
                        TaskDefinition.rootTask("ValidatePayment", "validatePayment"),
                        new TaskDefinition("ReserveInventory", "reserveInventory", List.of("ValidatePayment")),
                        new TaskDefinition("Ship", "ship", List.of("ReserveInventory")),
                        new TaskDefinition("Notify", "notify", List.of("ReserveInventory")),
                        new TaskDefinition("CompleteOrder", "completeOrder", List.of("Ship", "Notify"))));
    }
}
