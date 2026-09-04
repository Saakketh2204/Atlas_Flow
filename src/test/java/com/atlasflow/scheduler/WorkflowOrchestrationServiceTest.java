package com.atlasflow.scheduler;

import com.atlasflow.aws.DynamoTaskExecutionRepository;
import com.atlasflow.aws.DynamoWorkflowExecutionRepository;
import com.atlasflow.aws.SqsTaskEventPublisher;
import com.atlasflow.aws.TaskDispatchMessage;
import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import com.atlasflow.domain.WorkflowDefinition;
import com.atlasflow.domain.WorkflowExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the scheduling brain, using Mockito to fake the
 * DynamoDB/SQS dependencies -- these test the *decision logic* (what to
 * dispatch, when to retry vs. dead-letter, respecting cancellation) in
 * isolation from AWS, which is what a real AWS integration test
 * (integration/ChaosRecoveryIntegrationTest) additionally covers end to end.
 */
@ExtendWith(MockitoExtension.class)
class WorkflowOrchestrationServiceTest {

    @Mock DynamoWorkflowExecutionRepository executionRepository;
    @Mock DynamoTaskExecutionRepository taskRepository;
    @Mock SqsTaskEventPublisher publisher;

    private final Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private final WorkflowDefinition processOrder = WorkflowDefinition.processOrderExample();

    private WorkflowOrchestrationService service;

    @BeforeEach
    void setUp() {
        WorkflowDefinitionRegistry registry = new WorkflowDefinitionRegistry();
        AtlasFlowProperties properties = new AtlasFlowProperties();
        properties.setRetryBaseDelay(Duration.ofSeconds(1));
        properties.setRetryMaxDelay(Duration.ofSeconds(10));
        properties.setRetryMaxAttempts(3);
        service = new WorkflowOrchestrationService(
                executionRepository, taskRepository, publisher, registry, properties, clock);
    }

    /** Stubs taskRepository.get(executionId, taskId) to return a PENDING task for whatever executionId is passed. */
    private void stubPendingTask(String taskId) {
        when(taskRepository.get(anyString(), eq(taskId))).thenAnswer(
                inv -> Optional.of(TaskExecution.pending(inv.getArgument(0), taskId, Instant.now(clock))));
    }

    @Test
    void submitDispatchesOnlyTheRootTask() {
        stubPendingTask("ValidatePayment");

        String executionId = service.submit(processOrder);

        verify(executionRepository).save(any(WorkflowExecution.class));
        // 5 PENDING rows created (one per task) + 1 SCHEDULED transition for the root task
        verify(taskRepository, times(6)).save(any(TaskExecution.class));

        ArgumentCaptor<TaskDispatchMessage> captor = ArgumentCaptor.forClass(TaskDispatchMessage.class);
        verify(publisher, times(1)).publish(captor.capture());
        assertEquals(executionId, captor.getValue().executionId());
        assertEquals("ValidatePayment", captor.getValue().taskId());
        assertEquals(1, captor.getValue().attempt());
    }

    @Test
    void completingRootTaskDispatchesItsDependent() {
        String executionId = "exec-1";
        WorkflowExecution execution = WorkflowExecution.newExecution(executionId, "process-order-v1", Instant.now(clock));
        when(executionRepository.get(executionId)).thenReturn(Optional.of(execution));

        TaskExecution runningValidate = TaskExecution.pending(executionId, "ValidatePayment", Instant.now(clock))
                .withScheduled(Instant.now(clock))
                .withClaimedBy("worker-1", Instant.now(clock).plusSeconds(30), Instant.now(clock));
        when(taskRepository.get(executionId, "ValidatePayment")).thenReturn(Optional.of(runningValidate));
        when(taskRepository.findByExecution(executionId)).thenReturn(List.of());
        stubPendingTask("ReserveInventory");

        service.onTaskCompleted(processOrder, executionId, "ValidatePayment");

        verify(taskRepository).save(argThatCompleted("ValidatePayment"));

        ArgumentCaptor<TaskDispatchMessage> captor = ArgumentCaptor.forClass(TaskDispatchMessage.class);
        verify(publisher).publish(captor.capture());
        assertEquals("ReserveInventory", captor.getValue().taskId());
    }

    @Test
    void completingTheFinalTaskMarksTheExecutionCompletedWithoutDispatchingMore() {
        String executionId = "exec-2";
        WorkflowExecution execution = new WorkflowExecution(
                executionId, "process-order-v1", ExecutionStatus.RUNNING,
                java.util.Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify"),
                Instant.now(clock), Instant.now(clock));
        when(executionRepository.get(executionId)).thenReturn(Optional.of(execution));

        TaskExecution completeOrderTask = TaskExecution.pending(executionId, "CompleteOrder", Instant.now(clock))
                .withScheduled(Instant.now(clock))
                .withClaimedBy("worker-1", Instant.now(clock).plusSeconds(30), Instant.now(clock));
        when(taskRepository.get(executionId, "CompleteOrder")).thenReturn(Optional.of(completeOrderTask));

        service.onTaskCompleted(processOrder, executionId, "CompleteOrder");

        ArgumentCaptor<WorkflowExecution> captor = ArgumentCaptor.forClass(WorkflowExecution.class);
        verify(executionRepository).save(captor.capture());
        assertEquals(ExecutionStatus.COMPLETED, captor.getValue().status());
        verify(publisher, never()).publish(any());
    }

    @Test
    void completingATaskOnACancelledExecutionDoesNotDispatchFurtherWork() {
        String executionId = "exec-3";
        WorkflowExecution cancelled = new WorkflowExecution(
                executionId, "process-order-v1", ExecutionStatus.CANCELLED,
                java.util.Set.of(), Instant.now(clock), Instant.now(clock));
        when(executionRepository.get(executionId)).thenReturn(Optional.of(cancelled));

        TaskExecution task = TaskExecution.pending(executionId, "ValidatePayment", Instant.now(clock))
                .withScheduled(Instant.now(clock));
        when(taskRepository.get(executionId, "ValidatePayment")).thenReturn(Optional.of(task));

        service.onTaskCompleted(processOrder, executionId, "ValidatePayment");

        verify(publisher, never()).publish(any());
        verify(publisher, never()).publishWithDelay(any(), any());
    }

    @Test
    void failedTaskBelowRetryLimitIsRescheduledWithBackoff() {
        String executionId = "exec-4";
        TaskExecution firstAttempt = TaskExecution.pending(executionId, "ReserveInventory", Instant.now(clock))
                .withScheduled(Instant.now(clock)); // attempt = 1
        when(taskRepository.get(executionId, "ReserveInventory")).thenReturn(Optional.of(firstAttempt));

        service.onTaskFailed(processOrder, executionId, "ReserveInventory", "downstream timeout");

        ArgumentCaptor<TaskExecution> taskCaptor = ArgumentCaptor.forClass(TaskExecution.class);
        verify(taskRepository).save(taskCaptor.capture());
        assertEquals(TaskStatus.SCHEDULED, taskCaptor.getValue().status());
        assertEquals(2, taskCaptor.getValue().attempt());
        assertEquals(Optional.of("downstream timeout"), taskCaptor.getValue().lastError());

        ArgumentCaptor<Duration> delayCaptor = ArgumentCaptor.forClass(Duration.class);
        ArgumentCaptor<TaskDispatchMessage> messageCaptor = ArgumentCaptor.forClass(TaskDispatchMessage.class);
        verify(publisher).publishWithDelay(messageCaptor.capture(), delayCaptor.capture());
        assertEquals(2, messageCaptor.getValue().attempt());
        assertTrue(delayCaptor.getValue().compareTo(Duration.ZERO) >= 0);

        verify(publisher, never()).publishToDeadLetterQueue(any());
        verify(executionRepository, never()).save(any());
    }

    @Test
    void failedTaskAtRetryLimitIsDeadLetteredAndExecutionMarkedFailed() {
        String executionId = "exec-5";
        // maxAttempts=3 in setUp(); simulate a task already on its 3rd attempt.
        TaskExecution thirdAttempt = TaskExecution.pending(executionId, "Ship", Instant.now(clock))
                .withScheduled(Instant.now(clock))  // attempt 1
                .withScheduled(Instant.now(clock))  // attempt 2
                .withScheduled(Instant.now(clock)); // attempt 3
        when(taskRepository.get(executionId, "Ship")).thenReturn(Optional.of(thirdAttempt));

        WorkflowExecution execution = WorkflowExecution.newExecution(executionId, "process-order-v1", Instant.now(clock));
        when(executionRepository.get(executionId)).thenReturn(Optional.of(execution));

        service.onTaskFailed(processOrder, executionId, "Ship", "carrier API down");

        ArgumentCaptor<TaskExecution> taskCaptor = ArgumentCaptor.forClass(TaskExecution.class);
        verify(taskRepository).save(taskCaptor.capture());
        assertEquals(TaskStatus.FAILED, taskCaptor.getValue().status());

        ArgumentCaptor<WorkflowExecution> executionCaptor = ArgumentCaptor.forClass(WorkflowExecution.class);
        verify(executionRepository).save(executionCaptor.capture());
        assertEquals(ExecutionStatus.FAILED, executionCaptor.getValue().status());

        verify(publisher).publishToDeadLetterQueue(any());
        verify(publisher, never()).publishWithDelay(any(), any());
    }

    @Test
    void reclaimingAnOrphanedTaskBelowRetryLimitRedispatchesWithIncrementedAttempt() {
        String executionId = "exec-6";
        TaskExecution orphaned = TaskExecution.pending(executionId, "Notify", Instant.now(clock))
                .withScheduled(Instant.now(clock)) // attempt 1
                .withClaimedBy("worker-dead", Instant.now(clock).minusSeconds(5), Instant.now(clock).minusSeconds(35));

        service.reclaimOrphanedTask(processOrder, orphaned);

        ArgumentCaptor<TaskDispatchMessage> captor = ArgumentCaptor.forClass(TaskDispatchMessage.class);
        verify(publisher).publish(captor.capture());
        // Must increment past the orphaned attempt's number -- reusing it
        // would collide with that attempt's already-claimed idempotency key.
        assertEquals(2, captor.getValue().attempt());
        verify(publisher, never()).publishToDeadLetterQueue(any());
    }

    @Test
    void reclaimingAnOrphanedTaskAtRetryLimitDeadLettersInstead() {
        String executionId = "exec-7";
        TaskExecution orphaned = TaskExecution.pending(executionId, "Notify", Instant.now(clock))
                .withScheduled(Instant.now(clock))  // attempt 1
                .withScheduled(Instant.now(clock))  // attempt 2
                .withScheduled(Instant.now(clock))  // attempt 3 (== maxAttempts)
                .withClaimedBy("worker-dead", Instant.now(clock).minusSeconds(5), Instant.now(clock).minusSeconds(35));
        when(executionRepository.get(executionId)).thenReturn(
                Optional.of(WorkflowExecution.newExecution(executionId, "process-order-v1", Instant.now(clock))));

        service.reclaimOrphanedTask(processOrder, orphaned);

        verify(publisher).publishToDeadLetterQueue(any());
        verify(publisher, never()).publish(any());

        ArgumentCaptor<WorkflowExecution> executionCaptor = ArgumentCaptor.forClass(WorkflowExecution.class);
        verify(executionRepository).save(executionCaptor.capture());
        assertEquals(ExecutionStatus.FAILED, executionCaptor.getValue().status());
    }

    private static TaskExecution argThatCompleted(String taskId) {
        return org.mockito.ArgumentMatchers.argThat(
                t -> t != null && t.taskId().equals(taskId) && t.status() == TaskStatus.COMPLETED);
    }
}
