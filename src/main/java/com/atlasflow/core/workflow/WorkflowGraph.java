package com.atlasflow.core.workflow;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A validated, immutable workflow task-dependency graph.
 *
 * Construction validates the graph up front (no duplicate task IDs, no
 * dependencies on tasks that don't exist, no cycles) so that every
 * {@code WorkflowGraph} instance that exists is known-schedulable — the
 * scheduler never has to defensively re-check graph validity on every
 * scheduling decision, only once, when the workflow is first submitted.
 *
 * The core scheduling operation this repo cares about is
 * {@link #readyTasks}: given which tasks have completed and which are
 * currently in flight, which tasks can start *right now*? That's the
 * question AtlasFlow's {@code WorkflowOrchestrationService} asks every
 * time a task completes, to decide what to dispatch next.
 */
public final class WorkflowGraph {

    private final Map<String, TaskNode> nodesById;
    private final List<String> topologicalOrder;

    public WorkflowGraph(List<TaskNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("a workflow must have at least one task");
        }

        Map<String, TaskNode> byId = new LinkedHashMap<>();
        for (TaskNode node : nodes) {
            if (byId.put(node.taskId(), node) != null) {
                throw new IllegalArgumentException("duplicate task id: " + node.taskId());
            }
        }
        for (TaskNode node : byId.values()) {
            for (String dep : node.dependsOn()) {
                if (!byId.containsKey(dep)) {
                    throw new IllegalArgumentException(
                            "task '" + node.taskId() + "' depends on unknown task '" + dep + "'");
                }
            }
        }

        this.nodesById = byId;
        this.topologicalOrder = computeTopologicalOrder(byId);
    }

    public static WorkflowGraph of(TaskNode... nodes) {
        return new WorkflowGraph(List.of(nodes));
    }

    /**
     * Kahn's algorithm: repeatedly remove nodes with no unsatisfied
     * dependencies. If every node gets removed, the result is a valid
     * topological order. If some nodes are left over at the end, they —
     * and only they — are involved in a cycle, which is what makes this
     * algorithm convenient for producing an actionable error (the
     * offending task IDs) rather than just "a cycle exists somewhere".
     */
    private static List<String> computeTopologicalOrder(Map<String, TaskNode> byId) {
        Map<String, Integer> inDegree = new LinkedHashMap<>();
        Map<String, List<String>> dependents = new LinkedHashMap<>();
        for (String id : byId.keySet()) {
            inDegree.put(id, 0);
            dependents.put(id, new ArrayList<>());
        }
        for (TaskNode node : byId.values()) {
            inDegree.put(node.taskId(), node.dependsOn().size());
            for (String dep : node.dependsOn()) {
                dependents.get(dep).add(node.taskId());
            }
        }

        Queue<String> noDeps = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                noDeps.add(entry.getKey());
            }
        }

        List<String> order = new ArrayList<>();
        while (!noDeps.isEmpty()) {
            String id = noDeps.poll();
            order.add(id);
            for (String dependent : dependents.get(id)) {
                int updated = inDegree.merge(dependent, -1, Integer::sum);
                if (updated == 0) {
                    noDeps.add(dependent);
                }
            }
        }

        if (order.size() != byId.size()) {
            List<String> remaining = new ArrayList<>(byId.keySet());
            remaining.removeAll(order);
            throw new CycleDetectedException(remaining);
        }

        return List.copyOf(order);
    }

    public int size() {
        return nodesById.size();
    }

    public boolean contains(String taskId) {
        return nodesById.containsKey(taskId);
    }

    public TaskNode get(String taskId) {
        TaskNode node = nodesById.get(taskId);
        if (node == null) {
            throw new IllegalArgumentException("no such task: " + taskId);
        }
        return node;
    }

    /** One valid topological ordering of all tasks in this graph. */
    public List<String> topologicalOrder() {
        return topologicalOrder;
    }

    /**
     * Tasks whose dependencies are all in {@code completed}, and which are
     * not already completed or in {@code inProgress}. Returned in this
     * graph's topological order for deterministic, reproducible scheduling
     * decisions (useful for tests and for reasoning about behavior, even
     * though any order among mutually-ready tasks is logically valid).
     */
    public List<String> readyTasks(Set<String> completed, Set<String> inProgress) {
        List<String> ready = new ArrayList<>();
        for (String taskId : topologicalOrder) {
            if (completed.contains(taskId) || inProgress.contains(taskId)) {
                continue;
            }
            if (completed.containsAll(nodesById.get(taskId).dependsOn())) {
                ready.add(taskId);
            }
        }
        return ready;
    }

    public boolean isComplete(Set<String> completed) {
        return completed.containsAll(nodesById.keySet());
    }
}
