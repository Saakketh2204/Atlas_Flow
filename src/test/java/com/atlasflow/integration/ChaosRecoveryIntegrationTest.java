package com.atlasflow.integration;

import com.atlasflow.aws.DynamoIdempotencyStore;
import com.atlasflow.aws.DynamoTaskExecutionRepository;
import com.atlasflow.aws.DynamoWorkflowExecutionRepository;
import com.atlasflow.aws.SqsTaskEventPublisher;
import com.atlasflow.aws.TaskDispatchMessage;
import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.core.idempotency.IdempotencyKey;
import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import com.atlasflow.domain.WorkflowDefinition;
import com.atlasflow.domain.WorkflowExecution;
import com.atlasflow.scheduler.WorkflowDefinitionRegistry;
import com.atlasflow.scheduler.WorkflowOrchestrationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof, against a real (containerized) AWS-compatible backend,
 * of the property this whole project is built around: a worker that dies
 * mid-task is detected and recovered from automatically, with no manual
 * intervention, and without ever double-processing the task's original
 * (crashed) attempt.
 *
 * This deliberately does NOT boot a full Spring Boot application context
 * or run real worker/control-plane processes -- every class exercised
 * here (the repositories, the publisher, {@link WorkflowOrchestrationService})
 * uses plain constructor injection specifically so it CAN be exercised
 * this way, directly, against real infrastructure, without the added
 * complexity and slowness of standing up multiple Spring contexts or
 * separate JVM processes to simulate "a worker" and "a crash." A worker
 * crash is instead simulated the most direct way possible: manually
 * writing an already-expired lease into DynamoDB for a task, exactly as
 * if a real worker had claimed it and then stopped heartbeating.
 *
 * NOTE ON HOW THIS FILE WAS AUTHORED: this test could not be executed in
 * the sandboxed environment this project was originally built in (no
 * Docker, so Testcontainers cannot launch the LocalStack container there
 * at all) -- see docs/local-development.md. It is written to the same
 * standard of care as every other test in this repo and is exactly what
 * CI (.github/workflows/ci.yml) runs on every push; it just could not be
 * additionally hand-verified locally the way src/test/java/.../core/** was.
 */
@Testcontainers
class ChaosRecoveryIntegrationTest {

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4"))
            .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.SQS);

    static DynamoDbClient dynamoDbClient;
    static SqsClient sqsClient;
    static ObjectMapper objectMapper = new ObjectMapper();
    static AtlasFlowProperties properties;
    static Clock clock = Clock.systemUTC();

    static DynamoWorkflowExecutionRepository executionRepository;
    static DynamoTaskExecutionRepository taskRepository;
    static DynamoIdempotencyStore idempotencyStore;
    static SqsTaskEventPublisher publisher;
    static WorkflowOrchestrationService orchestrationService;

    @BeforeAll
    static void setUpInfrastructureAndServices() {
        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        createTable("atlasflow-executions", "executionId", null);
        createTable("atlasflow-tasks", "executionId", "taskId");
        createTable("atlasflow-idempotency", "idempotencyKey", null);
        sqsClient.createQueue(CreateQueueRequest.builder().queueName("atlasflow-tasks").build());
        sqsClient.createQueue(CreateQueueRequest.builder().queueName("atlasflow-tasks-dlq").build());

        properties = new AtlasFlowProperties();
        properties.setExecutionsTable("atlasflow-executions");
        properties.setTasksTable("atlasflow-tasks");
        properties.setIdempotencyTable("atlasflow-idempotency");
        properties.setTaskQueueName("atlasflow-tasks");
        properties.setDeadLetterQueueName("atlasflow-tasks-dlq");
        properties.setRetryBaseDelay(Duration.ofMillis(10));
        properties.setRetryMaxDelay(Duration.ofMillis(50));
        properties.setRetryMaxAttempts(3);

        executionRepository = new DynamoWorkflowExecutionRepository(dynamoDbClient, properties);
        taskRepository = new DynamoTaskExecutionRepository(dynamoDbClient, properties);
        idempotencyStore = new DynamoIdempotencyStore(dynamoDbClient, properties);
        publisher = new SqsTaskEventPublisher(sqsClient, objectMapper, properties);
        publisher.resolveQueueUrls();

        WorkflowDefinitionRegistry registry = new WorkflowDefinitionRegistry();
        orchestrationService = new WorkflowOrchestrationService(
                executionRepository, taskRepository, publisher, registry, properties, clock);
    }

    private static void createTable(String tableName, String partitionKey, String sortKey) {
        var attrs = sortKey == null
                ? List.of(AttributeDefinition.builder().attributeName(partitionKey).attributeType(ScalarAttributeType.S).build())
                : List.of(
                        AttributeDefinition.builder().attributeName(partitionKey).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(sortKey).attributeType(ScalarAttributeType.S).build());
        var keys = sortKey == null
                ? List.of(KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build())
                : List.of(
                        KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build());

        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(attrs)
                .keySchema(keys)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
    }

    @Test
    void idempotencyStoreRejectsASecondClaimOfTheSameAttempt() {
        IdempotencyKey key = new IdempotencyKey("idem-test-exec", "SomeTask", 1);
        assertTrue(idempotencyStore.tryClaim(key, Instant.now(clock)), "first claim should succeed");
        assertFalse(idempotencyStore.tryClaim(key, Instant.now(clock)), "second claim of the same attempt must be rejected");

        IdempotencyKey nextAttempt = new IdempotencyKey("idem-test-exec", "SomeTask", 2);
        assertTrue(idempotencyStore.tryClaim(nextAttempt, Instant.now(clock)), "a genuinely new attempt must be claimable");
    }

    @Test
    void workflowExecutionRepositoryRoundTripsIncludingEmptyCompletedSet() {
        String executionId = "round-trip-exec";
        WorkflowExecution fresh = WorkflowExecution.newExecution(executionId, "process-order-v1", Instant.now(clock));
        executionRepository.save(fresh);

        WorkflowExecution loaded = executionRepository.get(executionId).orElseThrow();
        assertEquals(fresh.executionId(), loaded.executionId());
        assertEquals(ExecutionStatus.RUNNING, loaded.status());
        assertTrue(loaded.completedTaskIds().isEmpty(), "empty completedTaskIds must round-trip as empty, not null/error");

        WorkflowExecution withOneDone = loaded.withTaskCompleted("ValidatePayment", Instant.now(clock));
        executionRepository.save(withOneDone);
        WorkflowExecution reloaded = executionRepository.get(executionId).orElseThrow();
        assertEquals(Set.of("ValidatePayment"), reloaded.completedTaskIds());
    }

    @Test
    void submittingAWorkflowPublishesTheRootTaskOntoTheRealQueue() {
        WorkflowDefinition definition = WorkflowDefinition.processOrderExample();
        String executionId = orchestrationService.submit(definition);

        Message message = receiveOneMessage();
        TaskDispatchMessage dispatch = parse(message);
        assertEquals(executionId, dispatch.executionId());
        assertEquals("ValidatePayment", dispatch.taskId());
        assertEquals(1, dispatch.attempt());

        sqsClient.deleteMessage(b -> b.queueUrl(taskQueueUrl()).receiptHandle(message.receiptHandle()));
    }

    /**
     * THE central scenario: submit a workflow, simulate a worker claiming
     * its root task and then dying (no completion, no further heartbeats
     * -- just an already-expired lease written to DynamoDB, exactly what a
     * real crash leaves behind), then run the same reclaim logic the
     * failure-recovery engine's scheduled scan runs, and prove: (1) the
     * task is automatically made available again with NO manual
     * intervention, (2) it gets a NEW attempt number, and (3) that new
     * attempt is genuinely claimable (not blocked by the dead attempt's
     * idempotency record).
     */
    @Test
    void aTaskOrphanedByACrashedWorkerIsAutomaticallyReclaimedAndBecomesClaimableAgain() {
        WorkflowDefinition definition = WorkflowDefinition.processOrderExample();
        String executionId = orchestrationService.submit(definition);
        Message rootMessage = receiveOneMessage();
        TaskDispatchMessage rootDispatch = parse(rootMessage);
        sqsClient.deleteMessage(b -> b.queueUrl(taskQueueUrl()).receiptHandle(rootMessage.receiptHandle()));

        // Simulate a worker claiming the task, then crashing: idempotency
        // claimed, lease written -- but ALREADY expired, standing in for
        // "this lease was never renewed because the worker died."
        IdempotencyKey deadAttemptKey = new IdempotencyKey(
                rootDispatch.executionId(), rootDispatch.taskId(), rootDispatch.attempt());
        assertTrue(idempotencyStore.tryClaim(deadAttemptKey, Instant.now(clock)));

        TaskExecution claimed = taskRepository.get(executionId, "ValidatePayment").orElseThrow();
        TaskExecution orphaned = claimed.withClaimedBy(
                "worker-that-crashed", Instant.now(clock).minusSeconds(1), Instant.now(clock).minusSeconds(31));
        taskRepository.save(orphaned);

        // This is the exact query FailureRecoveryEngine's scheduled scan runs.
        List<TaskExecution> expiredLeases = taskRepository.findExpiredLeases(Instant.now(clock));
        assertEquals(1, expiredLeases.size(), "the orphaned task must be found by the expired-lease scan");
        assertEquals("ValidatePayment", expiredLeases.get(0).taskId());

        orchestrationService.reclaimOrphanedTask(definition, expiredLeases.get(0));

        Message redispatched = receiveOneMessage();
        TaskDispatchMessage newDispatch = parse(redispatched);
        assertEquals("ValidatePayment", newDispatch.taskId());
        assertEquals(2, newDispatch.attempt(), "the reclaimed task must be redispatched as a NEW attempt");

        // The critical correctness property: the new attempt's idempotency
        // key must be distinct from the dead attempt's (already-claimed)
        // key, so a worker picking up this redispatched message can
        // actually claim and execute it, rather than being incorrectly
        // rejected as a duplicate of work that never actually finished.
        IdempotencyKey newAttemptKey = new IdempotencyKey(newDispatch.executionId(), newDispatch.taskId(), newDispatch.attempt());
        assertTrue(idempotencyStore.tryClaim(newAttemptKey, Instant.now(clock)),
                "the redispatched attempt must be freshly claimable, not blocked by the dead attempt's claim");

        TaskExecution finalState = taskRepository.get(executionId, "ValidatePayment").orElseThrow();
        assertEquals(TaskStatus.SCHEDULED, finalState.status());
        assertEquals(2, finalState.attempt());

        sqsClient.deleteMessage(b -> b.queueUrl(taskQueueUrl()).receiptHandle(redispatched.receiptHandle()));
    }

    private static Message receiveOneMessage() {
        List<Message> messages = List.of();
        for (int i = 0; i < 10 && messages.isEmpty(); i++) {
            messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(taskQueueUrl())
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(2)
                    .build()).messages();
        }
        if (messages.isEmpty()) {
            throw new AssertionError("expected a message on the queue but none arrived");
        }
        return messages.get(0);
    }

    private static TaskDispatchMessage parse(Message message) {
        try {
            return objectMapper.readValue(message.body(), TaskDispatchMessage.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String taskQueueUrl() {
        return sqsClient.getQueueUrl(b -> b.queueName("atlasflow-tasks")).queueUrl();
    }
}
