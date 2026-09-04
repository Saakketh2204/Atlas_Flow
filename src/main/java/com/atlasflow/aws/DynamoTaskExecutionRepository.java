package com.atlasflow.aws;

import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DynamoDB-backed storage for {@link TaskExecution} state.
 *
 * Table schema: partition key {@code executionId}, sort key {@code taskId}
 * -- see infra/localstack/init-aws.sh for the exact table definition used
 * in local development, which mirrors what a real deployment's
 * infrastructure-as-code would create.
 *
 * {@link #findExpiredLeases} uses a table Scan with a filter expression.
 * That's the right trade-off for a project at this scale: it's simple,
 * obviously correct, and easy to review -- but it would NOT be the right
 * choice at real production scale with a large, growing task table, where
 * a Global Secondary Index partitioned on status (or a
 * status+leaseExpiresAt composite) would turn this into a targeted Query
 * instead of a full-table Scan. That index is a natural "next thing to
 * add" if this project is extended; it's called out here rather than
 * silently designed around, because knowing *which* simplification you're
 * making and why is the actual point of a portfolio piece like this one.
 */
@Component
public class DynamoTaskExecutionRepository {

    private static final String ATTR_EXECUTION_ID = "executionId";
    private static final String ATTR_TASK_ID = "taskId";
    private static final String ATTR_STATUS = "status";
    private static final String ATTR_ATTEMPT = "attempt";
    private static final String ATTR_WORKER_ID = "workerId";
    private static final String ATTR_LEASE_EXPIRES_AT = "leaseExpiresAt";
    private static final String ATTR_LAST_ERROR = "lastError";
    private static final String ATTR_UPDATED_AT = "updatedAt";

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public DynamoTaskExecutionRepository(DynamoDbClient dynamoDbClient, AtlasFlowProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = properties.getTasksTable();
    }

    public void save(TaskExecution task) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(ATTR_EXECUTION_ID, AttributeValue.fromS(task.executionId()));
        item.put(ATTR_TASK_ID, AttributeValue.fromS(task.taskId()));
        item.put(ATTR_STATUS, AttributeValue.fromS(task.status().name()));
        item.put(ATTR_ATTEMPT, AttributeValue.fromN(Integer.toString(task.attempt())));
        item.put(ATTR_UPDATED_AT, AttributeValue.fromS(task.updatedAt().toString()));
        task.workerId().ifPresent(w -> item.put(ATTR_WORKER_ID, AttributeValue.fromS(w)));
        task.leaseExpiresAt().ifPresent(l -> item.put(ATTR_LEASE_EXPIRES_AT, AttributeValue.fromS(l.toString())));
        task.lastError().ifPresent(e -> item.put(ATTR_LAST_ERROR, AttributeValue.fromS(e)));

        dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    public Optional<TaskExecution> get(String executionId, String taskId) {
        GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(
                        ATTR_EXECUTION_ID, AttributeValue.fromS(executionId),
                        ATTR_TASK_ID, AttributeValue.fromS(taskId)))
                .build());
        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(fromItem(response.item()));
    }

    public List<TaskExecution> findByExecution(String executionId) {
        QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression(ATTR_EXECUTION_ID + " = :executionId")
                .expressionAttributeValues(Map.of(":executionId", AttributeValue.fromS(executionId)))
                .build());
        List<TaskExecution> tasks = new ArrayList<>();
        for (Map<String, AttributeValue> item : response.items()) {
            tasks.add(fromItem(item));
        }
        return tasks;
    }

    /** Tasks currently RUNNING whose lease expired strictly before {@code now}
     * -- these are the orphaned tasks the recovery engine reclaims. */
    public List<TaskExecution> findExpiredLeases(Instant now) {
        ScanResponse response = dynamoDbClient.scan(ScanRequest.builder()
                .tableName(tableName)
                .filterExpression(
                        ATTR_STATUS + " = :running AND " + ATTR_LEASE_EXPIRES_AT + " <= :now")
                .expressionAttributeValues(Map.of(
                        ":running", AttributeValue.fromS(TaskStatus.RUNNING.name()),
                        ":now", AttributeValue.fromS(now.toString())))
                .build());
        List<TaskExecution> expired = new ArrayList<>();
        for (Map<String, AttributeValue> item : response.items()) {
            expired.add(fromItem(item));
        }
        return expired;
    }

    private static TaskExecution fromItem(Map<String, AttributeValue> item) {
        return new TaskExecution(
                item.get(ATTR_EXECUTION_ID).s(),
                item.get(ATTR_TASK_ID).s(),
                TaskStatus.valueOf(item.get(ATTR_STATUS).s()),
                Integer.parseInt(item.get(ATTR_ATTEMPT).n()),
                item.containsKey(ATTR_WORKER_ID) ? Optional.of(item.get(ATTR_WORKER_ID).s()) : Optional.empty(),
                item.containsKey(ATTR_LEASE_EXPIRES_AT)
                        ? Optional.of(Instant.parse(item.get(ATTR_LEASE_EXPIRES_AT).s()))
                        : Optional.empty(),
                item.containsKey(ATTR_LAST_ERROR) ? Optional.of(item.get(ATTR_LAST_ERROR).s()) : Optional.empty(),
                Instant.parse(item.get(ATTR_UPDATED_AT).s()));
    }
}
