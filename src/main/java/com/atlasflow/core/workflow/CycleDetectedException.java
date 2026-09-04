package com.atlasflow.core.workflow;

import java.util.List;

/** Thrown when a workflow definition's task dependencies form a cycle,
 * which would make the workflow impossible to ever fully schedule. */
public final class CycleDetectedException extends RuntimeException {

    private final List<String> cycle;

    public CycleDetectedException(List<String> cycle) {
        super("Cycle detected in workflow task dependencies: " + String.join(" -> ", cycle));
        this.cycle = List.copyOf(cycle);
    }

    public List<String> cycle() {
        return cycle;
    }
}
