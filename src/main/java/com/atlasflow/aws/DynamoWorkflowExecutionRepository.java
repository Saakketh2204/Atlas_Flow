package com.atlasflow.aws;

import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.WorkflowExecution;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * DynamoDB-backed storage for {@link WorkflowExecution} state.
 * Table schema: partition key {@code executionId} only (see
 * infra/localstack/init-aws.sh).
 */
@Component
public class DynamoWorkflowExecutionRepository {

    private static final String ATTR_EXECUTION_ID = "executionId";
    private static final String ATTR_WORKFLOW_DEFINITION_ID = "workflowDefinitionId";
    private static final String ATTR_STATUS = "status";
    private static final String ATTR_COMPLETED_TASK_IDS = "completedTaskIds";
    private static final String ATTR_CREATED_AT = "createdAt";
    private static final String ATTR_UPDATED_AT = "updatedAt";

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public DynamoWorkflowExecutionRepository(DynamoDbClient dynamoDbClient, AtlasFlowProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = properties.getExecutionsTable();
    }

    public void save(WorkflowExecution execution) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(ATTR_EXECUTION_ID, AttributeValue.fromS(execution.executionId()));
        item.put(ATTR_WORKFLOW_DEFINITION_ID, AttributeValue.fromS(execution.workflowDefinitionId()));
        item.put(ATTR_STATUS, AttributeValue.fromS(execution.status().name()));
        item.put(ATTR_CREATED_AT, AttributeValue.fromS(execution.createdAt().toString()));
        item.put(ATTR_UPDATED_AT, AttributeValue.fromS(execution.updatedAt().toString()));
        // DynamoDB string sets cannot be empty -- omit the attribute entirely
        // when there are no completed tasks yet, rather than writing an
        // invalid empty SS, and treat "attribute absent" as "empty set" on read.
        if (!execution.completedTaskIds().isEmpty()) {
            item.put(ATTR_COMPLETED_TASK_IDS, AttributeValue.fromSs(List.copyOf(execution.completedTaskIds())));
        }

        dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    public Optional<WorkflowExecution> get(String executionId) {
        GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(ATTR_EXECUTION_ID, AttributeValue.fromS(executionId)))
                .build());
        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(fromItem(response.item()));
    }

    private static WorkflowExecution fromItem(Map<String, AttributeValue> item) {
        Set<String> completedTaskIds = item.containsKey(ATTR_COMPLETED_TASK_IDS)
                ? Set.copyOf(item.get(ATTR_COMPLETED_TASK_IDS).ss())
                : Set.of();
        return new WorkflowExecution(
                item.get(ATTR_EXECUTION_ID).s(),
                item.get(ATTR_WORKFLOW_DEFINITION_ID).s(),
                ExecutionStatus.valueOf(item.get(ATTR_STATUS).s()),
                completedTaskIds,
                Instant.parse(item.get(ATTR_CREATED_AT).s()),
                Instant.parse(item.get(ATTR_UPDATED_AT).s()));
    }
}
