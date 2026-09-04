package com.atlasflow.worker;

import com.atlasflow.aws.DynamoIdempotencyStore;
import com.atlasflow.aws.DynamoTaskExecutionRepository;
import com.atlasflow.aws.DynamoWorkflowExecutionRepository;
import com.atlasflow.aws.TaskDispatchMessage;
import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.core.idempotency.IdempotencyKey;
import com.atlasflow.domain.TaskDefinition;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import com.atlasflow.domain.WorkflowDefinition;
import com.atlasflow.domain.WorkflowExecution;
import com.atlasflow.scheduler.WorkflowDefinitionRegistry;
import com.atlasflow.scheduler.WorkflowOrchestrationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The worker side of AtlasFlow's event-driven architecture: long-polls the
 * task queue, and for each message:
 *
 * <ol>
 *   <li>Deserializes the {@link TaskDispatchMessage}.</li>
 *   <li>Claims it via {@link DynamoIdempotencyStore#tryClaim} -- a
 *       redelivered message for an attempt that's already claimed is
 *       silently skipped here, which is what makes SQS's at-least-once
 *       delivery safe to build on.</li>
 *   <li>Marks the task RUNNING with a lease, and starts a background
 *       heartbeat that periodically renews that lease while the handler
 *       runs.</li>
 *   <li>Invokes the registered {@link TaskHandler}.</li>
 *   <li>Reports success/failure back to {@link WorkflowOrchestrationService},
 *       which advances the workflow graph or schedules a retry.</li>
 * </ol>
 *
 * Only active under the {@code worker} Spring profile -- see
 * docker-compose.yml, where the worker container sets
 * {@code SPRING_PROFILES_ACTIVE=worker} and the control-plane container
 * does not, so exactly one of {@link TaskEventListener} (here) and
 * {@code recovery.FailureRecoveryEngine} runs in each container even
 * though both are built from the same jar.
 */
@Component
@Profile("worker")
public class TaskEventListener {

    private static final Logger log = LoggerFactory.getLogger(TaskEventListener.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final AtlasFlowProperties properties;
    private final WorkflowDefinitionRegistry definitionRegistry;
    private final WorkflowOrchestrationService orchestrationService;
    private final DynamoIdempotencyStore idempotencyStore;
    private final DynamoTaskExecutionRepository taskRepository;
    private final DynamoWorkflowExecutionRepository executionRepository;
    private final TaskHandlerRegistry handlerRegistry;
    private final Clock clock;

    private final String workerId = "worker-" + UUID.randomUUID();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newScheduledThreadPool(4);
    private String taskQueueUrl;

    public TaskEventListener(
            SqsClient sqsClient,
            ObjectMapper objectMapper,
            AtlasFlowProperties properties,
            WorkflowDefinitionRegistry definitionRegistry,
            WorkflowOrchestrationService orchestrationService,
            DynamoIdempotencyStore idempotencyStore,
            DynamoTaskExecutionRepository taskRepository,
            DynamoWorkflowExecutionRepository executionRepository,
            TaskHandlerRegistry handlerRegistry,
            Clock clock) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.definitionRegistry = definitionRegistry;
        this.orchestrationService = orchestrationService;
        this.idempotencyStore = idempotencyStore;
        this.taskRepository = taskRepository;
        this.executionRepository = executionRepository;
        this.handlerRegistry = handlerRegistry;
        this.clock = clock;
    }

    @PostConstruct
    void init() {
        this.taskQueueUrl = sqsClient
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(properties.getTaskQueueName()).build())
                .queueUrl();
        log.info("worker {} started, polling {}", workerId, properties.getTaskQueueName());
    }

    @PreDestroy
    void shutdown() {
        heartbeatExecutor.shutdownNow();
    }

    /**
     * Repeatedly long-polls for work. The fixedDelay here is small because
     * SQS's own {@code waitTimeSeconds} (long polling) does the actual
     * pacing -- when the queue is empty, this call blocks for up to
     * {@code sqsPollWaitSeconds} before returning zero messages, so the
     * effective poll interval under no load is bounded by that, not by
     * fixedDelay hammering the API.
     */
    @Scheduled(fixedDelay = 100)
    public void pollAndProcess() {
        ReceiveMessageResponse response = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(taskQueueUrl)
                .maxNumberOfMessages(properties.getWorkerPollBatchSize())
                .waitTimeSeconds(properties.getSqsPollWaitSeconds())
                .build());

        for (Message message : response.messages()) {
            try {
                processMessage(message);
            } catch (Exception e) {
                // A message we couldn't even parse/route is a poison
                // message -- log loudly and delete it rather than looping
                // on it forever, since neither retrying nor DLQ-routing
                // (which both require knowing the execution/task IDs) is
                // possible for a message we can't understand.
                log.error("unhandled error processing message, deleting: {}", message.body(), e);
                deleteMessage(message);
            }
        }
    }

    private void processMessage(Message message) throws Exception {
        TaskDispatchMessage dispatch = objectMapper.readValue(message.body(), TaskDispatchMessage.class);
        IdempotencyKey key = new IdempotencyKey(dispatch.executionId(), dispatch.taskId(), dispatch.attempt());
        Instant now = Instant.now(clock);

        if (!idempotencyStore.tryClaim(key, now)) {
            log.info("duplicate delivery for {}, already claimed -- skipping", key);
            deleteMessage(message);
            return;
        }

        Optional<WorkflowExecution> executionOpt = executionRepository.get(dispatch.executionId());
        if (executionOpt.isEmpty()) {
            log.error("no such execution for dispatched task, discarding: {}", dispatch);
            deleteMessage(message);
            return;
        }
        Optional<WorkflowDefinition> definitionOpt = definitionRegistry.get(executionOpt.get().workflowDefinitionId());
        if (definitionOpt.isEmpty()) {
            log.error("no registered definition '{}' for dispatched task, discarding: {}",
                    executionOpt.get().workflowDefinitionId(), dispatch);
            deleteMessage(message);
            return;
        }
        WorkflowDefinition definition = definitionOpt.get();
        TaskDefinition taskDefinition = definition.task(dispatch.taskId());

        Optional<TaskHandler> handlerOpt = handlerRegistry.get(taskDefinition.handlerName());
        if (handlerOpt.isEmpty()) {
            log.error("no handler registered for '{}', discarding: {}", taskDefinition.handlerName(), dispatch);
            deleteMessage(message);
            return;
        }

        claimAndExecute(definition, dispatch, handlerOpt.get(), now);
        // The original message is always deleted here: by this point we've
        // already durably recorded the outcome ourselves (COMPLETED, a
        // freshly re-published RETRYING message, or a dead-letter publish)
        // via WorkflowOrchestrationService, so leaving the original message
        // for SQS's visibility timeout to eventually redeliver would only
        // produce a redundant, idempotency-store-rejected duplicate.
        deleteMessage(message);
    }

    private void claimAndExecute(
            WorkflowDefinition definition, TaskDispatchMessage dispatch, TaskHandler handler, Instant now) {
        TaskExecution current = taskRepository.get(dispatch.executionId(), dispatch.taskId())
                .orElseThrow(() -> new IllegalStateException("no such task: " + dispatch));
        Instant leaseExpiresAt = now.plus(properties.getLeaseDuration());
        taskRepository.save(current.withClaimedBy(workerId, leaseExpiresAt, now));

        long heartbeatMillis = properties.getHeartbeatInterval().toMillis();
        ScheduledFuture<?> heartbeat = heartbeatExecutor.scheduleAtFixedRate(
                () -> renewLease(dispatch), heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);

        try {
            handler.handle(dispatch.executionId(), dispatch.taskId());
            orchestrationService.onTaskCompleted(definition, dispatch.executionId(), dispatch.taskId());
        } catch (Exception e) {
            String error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            orchestrationService.onTaskFailed(definition, dispatch.executionId(), dispatch.taskId(), error);
        } finally {
            heartbeat.cancel(false);
        }
    }

    private void renewLease(TaskDispatchMessage dispatch) {
        try {
            Instant now = Instant.now(clock);
            taskRepository.get(dispatch.executionId(), dispatch.taskId()).ifPresent(task -> {
                if (task.status() == TaskStatus.RUNNING) {
                    taskRepository.save(task.withHeartbeat(now.plus(properties.getLeaseDuration()), now));
                }
            });
        } catch (Exception e) {
            // A single missed heartbeat is not fatal -- it's exactly the
            // condition the lease-expiration/recovery mechanism exists to
            // handle. Log and let the next scheduled heartbeat try again.
            log.warn("heartbeat renewal failed for {}", dispatch, e);
        }
    }

    private void deleteMessage(Message message) {
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(taskQueueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
    }
}
