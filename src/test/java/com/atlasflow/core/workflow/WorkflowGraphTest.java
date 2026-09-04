package com.atlasflow.core.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowGraphTest {

    /** The ProcessOrder example graph used throughout this repo's docs/demo. */
    private static WorkflowGraph processOrderGraph() {
        return WorkflowGraph.of(
                new TaskNode("ValidatePayment", List.of()),
                new TaskNode("ReserveInventory", List.of("ValidatePayment")),
                new TaskNode("Ship", List.of("ReserveInventory")),
                new TaskNode("Notify", List.of("ReserveInventory")),
                new TaskNode("CompleteOrder", List.of("Ship", "Notify")));
    }

    @Test
    void initiallyOnlyRootTasksAreReady() {
        WorkflowGraph graph = processOrderGraph();
        List<String> ready = graph.readyTasks(Set.of(), Set.of());
        assertEquals(List.of("ValidatePayment"), ready);
    }

    @Test
    void completingATaskUnlocksItsDependents() {
        WorkflowGraph graph = processOrderGraph();
        List<String> ready = graph.readyTasks(Set.of("ValidatePayment"), Set.of());
        assertEquals(List.of("ReserveInventory"), ready);
    }

    @Test
    void parallelBranchesAreBothReadySimultaneously() {
        WorkflowGraph graph = processOrderGraph();
        List<String> ready = graph.readyTasks(Set.of("ValidatePayment", "ReserveInventory"), Set.of());
        assertEquals(Set.of("Ship", "Notify"), Set.copyOf(ready));
        assertEquals(2, ready.size());
    }

    @Test
    void joinTaskWaitsForBothBranchesEvenIfOneFinishesFirst() {
        WorkflowGraph graph = processOrderGraph();
        // Ship finished, Notify hasn't -- CompleteOrder must NOT be ready yet.
        List<String> ready = graph.readyTasks(Set.of("ValidatePayment", "ReserveInventory", "Ship"), Set.of());
        assertFalse(ready.contains("CompleteOrder"));

        // Now both branches are done.
        ready = graph.readyTasks(
                Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify"), Set.of());
        assertEquals(List.of("CompleteOrder"), ready);
    }

    @Test
    void tasksAlreadyInProgressAreNotReadyAgain() {
        WorkflowGraph graph = processOrderGraph();
        List<String> ready = graph.readyTasks(Set.of(), Set.of("ValidatePayment"));
        assertTrue(ready.isEmpty());
    }

    @Test
    void isCompleteOnlyWhenEveryTaskIsDone() {
        WorkflowGraph graph = processOrderGraph();
        assertFalse(graph.isComplete(Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify")));
        assertTrue(graph.isComplete(
                Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify", "CompleteOrder")));
    }

    @Test
    void topologicalOrderRespectsAllDependencies() {
        WorkflowGraph graph = processOrderGraph();
        List<String> order = graph.topologicalOrder();
        assertTrue(order.indexOf("ValidatePayment") < order.indexOf("ReserveInventory"));
        assertTrue(order.indexOf("ReserveInventory") < order.indexOf("Ship"));
        assertTrue(order.indexOf("ReserveInventory") < order.indexOf("Notify"));
        assertTrue(order.indexOf("Ship") < order.indexOf("CompleteOrder"));
        assertTrue(order.indexOf("Notify") < order.indexOf("CompleteOrder"));
    }

    @Test
    void detectsASimpleTwoNodeCycle() {
        assertThrows(CycleDetectedException.class, () -> WorkflowGraph.of(
                new TaskNode("A", List.of("B")),
                new TaskNode("B", List.of("A"))));
    }

    @Test
    void detectsALongerCycle() {
        assertThrows(CycleDetectedException.class, () -> WorkflowGraph.of(
                new TaskNode("A", List.of("C")),
                new TaskNode("B", List.of("A")),
                new TaskNode("C", List.of("B"))));
    }

    @Test
    void rejectsDependencyOnUnknownTask() {
        assertThrows(IllegalArgumentException.class, () -> WorkflowGraph.of(
                new TaskNode("A", List.of("does-not-exist"))));
    }

    @Test
    void rejectsDuplicateTaskIds() {
        assertThrows(IllegalArgumentException.class, () -> WorkflowGraph.of(
                new TaskNode("A", List.of()),
                new TaskNode("A", List.of())));
    }

    @Test
    void rejectsEmptyGraph() {
        assertThrows(IllegalArgumentException.class, () -> new WorkflowGraph(List.of()));
    }

    @Test
    void singleTaskWorkflowIsImmediatelyReadyAndCompletesInOneStep() {
        WorkflowGraph graph = WorkflowGraph.of(new TaskNode("OnlyTask", List.of()));
        assertEquals(List.of("OnlyTask"), graph.readyTasks(Set.of(), Set.of()));
        assertTrue(graph.isComplete(Set.of("OnlyTask")));
    }
}
