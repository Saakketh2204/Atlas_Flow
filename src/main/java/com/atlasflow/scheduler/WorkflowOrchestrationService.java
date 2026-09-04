package com.atlasflow.scheduler;

import com.atlasflow.aws.DynamoTaskExecutionRepository;
import com.atlasflow.aws.DynamoWorkflowExecutionRepository;
import com.atlasflow.aws.SqsTaskEventPublisher;
import com.atlasflow.aws.TaskDispatchMessage;
import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.core.retry.RetryPolicy;
import com.atlasflow.core.workflow.WorkflowGraph;
import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import com.atlasflow.domain.WorkflowDefinition;
import com.atlasflow.domain.WorkflowExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The scheduling brain of AtlasFlow: decides what should run next and
 * dispatches it, using {@link WorkflowGraph#readyTasks} (pure logic,
 * already unit-tested in isolation) as the single source of truth for
 * "given what's done and what's in flight, what's ready now?"
 *
 * This service is used by both the REST API (to submit new executions)
 * and the worker (to react to task completion/failure and advance the
 * workflow) -- in this repo's single-jar, profile-based deployment (see
 * docker-compose.yml) both run in the same process space and share these
 * beans directly, rather than needing a callback/webhook mechanism between
 * separately-deployed control-plane and worker services.
 */
@Service
public class WorkflowOrchestrationService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowOrchestrationService.class);

    private final DynamoWorkflowExecutionRepository executionRepository;
    private final DynamoTaskExecutionRepository taskRepository;
    private final SqsTaskEventPublisher publisher;
    private final WorkflowDefinitionRegistry registry;
    private final RetryPolicy retryPolicy;
    private final Clock clock;

    public WorkflowOrchestrationService(
            DynamoWorkflowExecutionRepository executionRepository,
            DynamoTaskExecutionRepository taskRepository,
            SqsTaskEventPublisher publisher,
            WorkflowDefinitionRegistry registry,
            AtlasFlowProperties properties,
            Clock clock) {
        this.executionRepository = executionRepository;
        this.taskRepository = taskRepository;
        this.publisher = publisher;
        this.registry = registry;
        this.retryPolicy = new RetryPolicy(
                properties.getRetryBaseDelay(), properties.getRetryMaxDelay(), properties.getRetryMaxAttempts());
        this.clock = clock;
    }

    /** Starts a new execution of the given workflow definition and dispatches its root task(s). */
    public String submit(WorkflowDefinition definition) {
        String executionId = UUID.randomUUID().toString();
        Instant now = Instant.now(clock);

        WorkflowExecution execution = WorkflowExecution.newExecution(executionId, definition.workflowDefinitionId(), now);
        executionRepository.save(execution);

        for (var task : definition.tasks()) {
            taskRepository.save(TaskExecution.pending(executionId, task.taskId(), now));
        }

        log.info("submitted execution {} of workflow '{}'", executionId, definition.workflowDefinitionId());
        dispatchReadyTasks(definition, executionId, Set.of(), Set.of());
        return executionId;
    }

    /** Called when a worker successfully completes a task attempt. Advances the workflow. */
    public void onTaskCompleted(WorkflowDefinition definition, String executionId, String taskId) {
        Instant now = Instant.now(clock);

        WorkflowExecution execution = executionRepository.get(executionId)
                .orElseThrow(() -> new IllegalStateException("no such execution: " + executionId));
        TaskExecution task = taskRepository.get(executionId, taskId)
                .orElseThrow(() -> new IllegalStateException("no such task: " + executionId + "/" + taskId));

        taskRepository.save(task.withCompleted(now));
        WorkflowExecution updatedExecution = execution.withTaskCompleted(taskId, now);

        WorkflowGraph graph = definition.toGraph();
        if (graph.isComplete(updatedExecution.completedTaskIds())) {
            executionRepository.save(updatedExecution.withStatus(ExecutionStatus.COMPLETED, now));
            log.info("execution {} completed", executionId);
            return;
        }

        executionRepository.save(updatedExecution);

        if (updatedExecution.status() != ExecutionStatus.RUNNING) {
            // The execution was cancelled (or already failed via a
            // sibling task) between this task being dispatched and it
            // completing -- record its completion for the audit trail,
            // but don't schedule any further work on a workflow that's no
            // longer supposed to be advancing.
            log.info("execution {} is {} -- not dispatching further tasks after {} completed",
                    executionId, updatedExecution.status(), taskId);
            return;
        }

        Set<String> inProgress = currentlyInProgressTaskIds(executionId);
        dispatchReadyTasks(definition, executionId, updatedExecution.completedTaskIds(), inProgress);
    }

    /**
     * Called when a worker's attempt at a task fails. Either schedules a
     * jittered-backoff retry (same task, next attempt) or, once retries are
     * exhausted, marks the task and its owning execution FAILED and routes
     * the message to the dead-letter queue.
     */
    public void onTaskFailed(WorkflowDefinition definition, String executionId, String taskId, String errorMessage) {
        Instant now = Instant.now(clock);
        TaskExecution task = taskRepository.get(executionId, taskId)
                .orElseThrow(() -> new IllegalStateException("no such task: " + executionId + "/" + taskId));

        if (retryPolicy.shouldRetry(task.attempt())) {
            TaskExecution rescheduled = task.withRetrying(errorMessage, now).withScheduled(now);
            taskRepository.save(rescheduled);

            var delay = retryPolicy.nextDelay(rescheduled.attempt());
            log.warn("task {}/{} failed (attempt {}), retrying in {} -- cause: {}",
                    executionId, taskId, task.attempt(), delay, errorMessage);
            publisher.publishWithDelay(
                    new TaskDispatchMessage(executionId, taskId, rescheduled.attempt()), delay);
        } else {
            log.error("task {}/{} failed permanently after {} attempts -- cause: {}",
                    executionId, taskId, task.attempt(), errorMessage);
            taskRepository.save(task.withFailed(errorMessage, now));
            executionRepository.get(executionId).ifPresent(
                    execution -> executionRepository.save(execution.withStatus(ExecutionStatus.FAILED, now)));
            publisher.publishToDeadLetterQueue(new TaskDispatchMessage(executionId, taskId, task.attempt()));
        }
    }

    private void dispatchReadyTasks(
            WorkflowDefinition definition, String executionId, Set<String> completed, Set<String> inProgress) {
        WorkflowGraph graph = definition.toGraph();
        List<String> ready = graph.readyTasks(completed, inProgress);

        for (String taskId : ready) {
            Instant now = Instant.now(clock);
            TaskExecution task = taskRepository.get(executionId, taskId)
                    .orElseThrow(() -> new IllegalStateException("no such task: " + executionId + "/" + taskId));
            TaskExecution scheduled = task.withScheduled(now);
            taskRepository.save(scheduled);
            publisher.publish(new TaskDispatchMessage(executionId, taskId, scheduled.attempt()));
            log.info("dispatched {}/{} (attempt {})", executionId, taskId, scheduled.attempt());
        }
    }

    private Set<String> currentlyInProgressTaskIds(String executionId) {
        return taskRepository.findByExecution(executionId).stream()
                .filter(t -> t.status() == TaskStatus.SCHEDULED
                        || t.status() == TaskStatus.RUNNING
                        || t.status() == TaskStatus.RETRYING)
                .map(TaskExecution::taskId)
                .collect(Collectors.toSet());
    }

    /**
     * Called by {@code recovery.FailureRecoveryEngine} for a task whose
     * worker lease expired without the task completing -- the worker most
     * likely crashed mid-execution. Either re-dispatches the task as a
     * fresh attempt, or, if the retry budget is exhausted, dead-letters it,
     * using the exact same {@link RetryPolicy} as an explicit handler
     * failure (a crashed worker and a thrown exception are both just
     * "this attempt didn't succeed" from the workflow's point of view).
     *
     * The attempt number MUST increment here (via {@code withScheduled}):
     * reusing the orphaned attempt's number would reuse its
     * {@link com.atlasflow.core.idempotency.IdempotencyKey} too, which is
     * already claimed in the idempotency store from the crashed worker's
     * attempt -- the new worker's {@code tryClaim} would then immediately
     * (and incorrectly) reject it as a duplicate, and the task would never
     * actually run again.
     */
    public void reclaimOrphanedTask(WorkflowDefinition definition, TaskExecution orphaned) {
        Instant now = Instant.now(clock);

        if (!retryPolicy.shouldRetry(orphaned.attempt())) {
            log.error("orphaned task {}/{} exhausted its retry budget after lease expiration, dead-lettering",
                    orphaned.executionId(), orphaned.taskId());
            taskRepository.save(orphaned.withFailed("worker lease expired; retry budget exhausted", now));
            executionRepository.get(orphaned.executionId()).ifPresent(
                    execution -> executionRepository.save(execution.withStatus(ExecutionStatus.FAILED, now)));
            publisher.publishToDeadLetterQueue(
                    new TaskDispatchMessage(orphaned.executionId(), orphaned.taskId(), orphaned.attempt()));
            return;
        }

        TaskExecution rescheduled = orphaned.withReclaimed(now).withScheduled(now);
        taskRepository.save(rescheduled);
        log.warn("reclaimed orphaned task {}/{} (worker '{}' lease expired), redispatching as attempt {}",
                orphaned.executionId(), orphaned.taskId(), orphaned.workerId().orElse("unknown"), rescheduled.attempt());
        publisher.publish(new TaskDispatchMessage(orphaned.executionId(), orphaned.taskId(), rescheduled.attempt()));
    }

    public WorkflowDefinitionRegistry definitionRegistry() {
        return registry;
    }
}
