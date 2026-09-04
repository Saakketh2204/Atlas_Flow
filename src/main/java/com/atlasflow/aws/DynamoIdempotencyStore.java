package com.atlasflow.aws;

import com.atlasflow.config.AtlasFlowProperties;
import com.atlasflow.core.idempotency.IdempotencyKey;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.util.Map;

/**
 * Deduplicates task processing using DynamoDB's conditional writes.
 *
 * The mechanism: before executing a task attempt, the worker tries to
 * {@code PutItem} a row for that attempt's {@link IdempotencyKey} with the
 * condition {@code attribute_not_exists(idempotencyKey)}. Two outcomes:
 *
 * <ul>
 *   <li>The put succeeds -> this is the first time this exact attempt has
 *       been claimed for processing. Proceed.</li>
 *   <li>The put fails with {@link ConditionalCheckFailedException} -> a
 *       row already exists for this exact (execution, task, attempt)
 *       triple, meaning this is a redelivered message for work that's
 *       already in flight or already done. Skip processing.</li>
 * </ul>
 *
 * This is the standard DynamoDB idempotency pattern, and it's what turns
 * SQS's at-least-once delivery guarantee into effectively-once task
 * execution from AtlasFlow's point of view -- see
 * {@code worker.TaskEventListener} for where this gets called.
 */
@Component
public class DynamoIdempotencyStore {

    private static final String ATTR_KEY = "idempotencyKey";
    private static final String ATTR_CLAIMED_AT = "claimedAt";

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public DynamoIdempotencyStore(DynamoDbClient dynamoDbClient, AtlasFlowProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = properties.getIdempotencyTable();
    }

    /**
     * Attempts to claim this attempt for processing.
     *
     * @return true if this call is the first to claim this key (proceed
     *         with processing); false if it was already claimed (this is a
     *         duplicate delivery -- skip processing).
     */
    public boolean tryClaim(IdempotencyKey key, Instant now) {
        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(Map.of(
                        ATTR_KEY, AttributeValue.fromS(key.asString()),
                        ATTR_CLAIMED_AT, AttributeValue.fromS(now.toString())))
                .conditionExpression("attribute_not_exists(" + ATTR_KEY + ")")
                .build();

        try {
            dynamoDbClient.putItem(request);
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }
}
