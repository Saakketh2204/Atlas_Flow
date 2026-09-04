package com.atlasflow.aws;

/**
 * The message body published to SQS for each task dispatch: which
 * execution/task/attempt to run. Kept intentionally minimal -- workers
 * look up the rest (workflow definition, handler name) from the execution
 * ID rather than having it duplicated into every message, so a workflow
 * definition change can't leave stale copies of itself sitting in flight
 * in the queue.
 */
public record TaskDispatchMessage(String executionId, String taskId, int attempt) {
}
