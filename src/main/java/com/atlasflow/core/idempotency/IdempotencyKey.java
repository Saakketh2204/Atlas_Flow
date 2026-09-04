package com.atlasflow.core.idempotency;

import java.util.Objects;

/**
 * An idempotency key uniquely identifies one *attempt* of one task within
 * one workflow execution: {@code workflowExecutionId + taskId + attempt}.
 *
 * This is deliberately NOT just {@code workflowExecutionId + taskId} —
 * including the attempt number means a legitimate retry (a new attempt
 * after a previous one failed) gets its own key and is allowed to
 * execute, while a *duplicate delivery of the same attempt* (e.g. SQS's
 * at-least-once delivery redelivering a message that was actually already
 * processed, perhaps because the consumer crashed after processing but
 * before deleting the message) collides on the same key and is rejected.
 *
 * The actual deduplication mechanism lives in the AWS layer
 * (DynamoWorkerTaskRepository uses this key's string form as a DynamoDB
 * partition key with a conditional PutItem: "only write if this key
 * doesn't already exist"). This class only owns the key's *shape* —
 * constructing it consistently and parsing it back apart — which is pure,
 * dependency-free logic worth getting right and testing in isolation from
 * DynamoDB.
 */
public final class IdempotencyKey {

    private static final String DELIMITER = "#";

    private final String workflowExecutionId;
    private final String taskId;
    private final int attempt;

    public IdempotencyKey(String workflowExecutionId, String taskId, int attempt) {
        requireNoDelimiter(workflowExecutionId, "workflowExecutionId");
        requireNoDelimiter(taskId, "taskId");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        this.workflowExecutionId = workflowExecutionId;
        this.taskId = taskId;
        this.attempt = attempt;
    }

    private static void requireNoDelimiter(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        if (value.contains(DELIMITER)) {
            // The delimiter must not appear inside a component, or parse()
            // could not unambiguously split the string back apart.
            throw new IllegalArgumentException(
                    fieldName + " must not contain the delimiter '" + DELIMITER + "'");
        }
    }

    public String workflowExecutionId() {
        return workflowExecutionId;
    }

    public String taskId() {
        return taskId;
    }

    public int attempt() {
        return attempt;
    }

    /** Canonical string form, suitable for use as a DynamoDB partition key. */
    public String asString() {
        return workflowExecutionId + DELIMITER + taskId + DELIMITER + attempt;
    }

    public static IdempotencyKey parse(String raw) {
        Objects.requireNonNull(raw, "raw must not be null");
        String[] parts = raw.split(DELIMITER);
        if (parts.length != 3) {
            throw new IllegalArgumentException("malformed idempotency key: " + raw);
        }
        int attempt;
        try {
            attempt = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed idempotency key (attempt not numeric): " + raw, e);
        }
        return new IdempotencyKey(parts[0], parts[1], attempt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdempotencyKey other)) return false;
        return attempt == other.attempt
                && workflowExecutionId.equals(other.workflowExecutionId)
                && taskId.equals(other.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowExecutionId, taskId, attempt);
    }

    @Override
    public String toString() {
        return asString();
    }
}
