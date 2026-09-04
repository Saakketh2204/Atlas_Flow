package com.atlasflow.scheduler;

import com.atlasflow.domain.WorkflowDefinition;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds registered {@link WorkflowDefinition}s in memory, keyed by ID.
 *
 * Scope note: definitions are NOT persisted to DynamoDB in this version --
 * they're registered via {@code POST /workflows} (or pre-loaded at
 * startup, see the {@code processOrderExample} seeding below) and live
 * only as long as the control-plane process does. Given this project's
 * purpose is demonstrating the *execution* engine (scheduling, retries,
 * idempotency, failure recovery), a full definition CRUD/versioning
 * subsystem would add real scope without adding to what the project is
 * actually meant to showcase -- see docs/architecture.md for the full
 * list of what's deliberately in vs. out of scope. Persisting definitions
 * would be a natural, small next step (same repository pattern as
 * DynamoWorkflowExecutionRepository) if this project were extended.
 */
@Component
public class WorkflowDefinitionRegistry {

    private final Map<String, WorkflowDefinition> definitionsById = new ConcurrentHashMap<>();

    public WorkflowDefinitionRegistry() {
        WorkflowDefinition example = WorkflowDefinition.processOrderExample();
        definitionsById.put(example.workflowDefinitionId(), example);
    }

    public void register(WorkflowDefinition definition) {
        definitionsById.put(definition.workflowDefinitionId(), definition);
    }

    public Optional<WorkflowDefinition> get(String workflowDefinitionId) {
        return Optional.ofNullable(definitionsById.get(workflowDefinitionId));
    }

    public Map<String, WorkflowDefinition> all() {
        return Map.copyOf(definitionsById);
    }
}
