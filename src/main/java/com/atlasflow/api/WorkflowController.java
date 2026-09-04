package com.atlasflow.api;

import com.atlasflow.api.dto.ExecutionStatusResponse;
import com.atlasflow.api.dto.StartExecutionRequest;
import com.atlasflow.aws.DynamoTaskExecutionRepository;
import com.atlasflow.aws.DynamoWorkflowExecutionRepository;
import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.WorkflowDefinition;
import com.atlasflow.domain.WorkflowExecution;
import com.atlasflow.scheduler.WorkflowDefinitionRegistry;
import com.atlasflow.scheduler.WorkflowOrchestrationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The control-plane REST API:
 * <pre>
 *   POST /workflows                    register a workflow definition
 *   GET  /workflows                    list registered definitions
 *   POST /executions                   start an execution of a registered definition
 *   GET  /executions/{id}              full execution + per-task status
 *   GET  /executions/{id}/status       lightweight status summary
 *   POST /executions/{id}/cancel       cancel a running execution
 * </pre>
 */
@RestController
public class WorkflowController {

    private final WorkflowDefinitionRegistry definitionRegistry;
    private final WorkflowOrchestrationService orchestrationService;
    private final DynamoWorkflowExecutionRepository executionRepository;
    private final DynamoTaskExecutionRepository taskRepository;
    private final Clock clock;

    public WorkflowController(
            WorkflowDefinitionRegistry definitionRegistry,
            WorkflowOrchestrationService orchestrationService,
            DynamoWorkflowExecutionRepository executionRepository,
            DynamoTaskExecutionRepository taskRepository,
            Clock clock) {
        this.definitionRegistry = definitionRegistry;
        this.orchestrationService = orchestrationService;
        this.executionRepository = executionRepository;
        this.taskRepository = taskRepository;
        this.clock = clock;
    }

    @PostMapping("/workflows")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> registerWorkflow(@RequestBody WorkflowDefinition definition) {
        // WorkflowDefinition's compact constructor already validates the
        // task graph (no cycles, no dangling dependencies) -- an invalid
        // definition never reaches this line; see GlobalExceptionHandler
        // for how that construction failure is turned into a 400 response.
        definitionRegistry.register(definition);
        return Map.of("workflowDefinitionId", definition.workflowDefinitionId());
    }

    @GetMapping("/workflows")
    public Map<String, WorkflowDefinition> listWorkflows() {
        return definitionRegistry.all();
    }

    @PostMapping("/executions")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> startExecution(@Valid @RequestBody StartExecutionRequest request) {
        WorkflowDefinition definition = definitionRegistry.get(request.workflowDefinitionId())
                .orElseThrow(() -> new NoSuchElementException(
                        "no such workflow definition: " + request.workflowDefinitionId()));
        String executionId = orchestrationService.submit(definition);
        return Map.of("executionId", executionId);
    }

    @GetMapping("/executions/{executionId}")
    public ExecutionStatusResponse getExecution(@PathVariable String executionId) {
        WorkflowExecution execution = executionRepository.get(executionId)
                .orElseThrow(() -> new NoSuchElementException("no such execution: " + executionId));
        return ExecutionStatusResponse.from(execution, taskRepository.findByExecution(executionId));
    }

    @GetMapping("/executions/{executionId}/status")
    public Map<String, Object> getExecutionStatus(@PathVariable String executionId) {
        WorkflowExecution execution = executionRepository.get(executionId)
                .orElseThrow(() -> new NoSuchElementException("no such execution: " + executionId));
        return Map.of(
                "executionId", execution.executionId(),
                "status", execution.status(),
                "completedTaskCount", execution.completedTaskIds().size());
    }

    @PostMapping("/executions/{executionId}/cancel")
    public Map<String, String> cancelExecution(@PathVariable String executionId) {
        WorkflowExecution execution = executionRepository.get(executionId)
                .orElseThrow(() -> new NoSuchElementException("no such execution: " + executionId));

        if (execution.status() == ExecutionStatus.RUNNING) {
            executionRepository.save(execution.withStatus(ExecutionStatus.CANCELLED, Instant.now(clock)));
        }
        // Tasks already dispatched to a worker before cancellation will
        // still run to completion in this implementation -- their results
        // are simply not propagated further, since
        // WorkflowOrchestrationService checks the execution's status
        // before dispatching any *new* tasks. Actively interrupting
        // in-flight work on cancellation is a natural extension but adds
        // real distributed-cancellation complexity (see docs/architecture.md).
        return Map.of("executionId", executionId, "status", "CANCELLED");
    }
}
