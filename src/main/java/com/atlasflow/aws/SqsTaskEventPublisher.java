package com.atlasflow.aws;

import com.atlasflow.config.AtlasFlowProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.Duration;

/**
 * Publishes {@link TaskDispatchMessage}s onto the task queue -- this is
 * the "dispatch a ready task onto the event bus" half of AtlasFlow's
 * event-driven architecture; {@code worker.TaskEventListener} is the
 * other half, consuming from the same queue.
 *
 * Queue URLs are resolved from queue *names* once at startup (SQS's send
 * API needs the full URL, but names are what's configured and what stay
 * stable across environments/regions) and cached for the life of the bean.
 *
 * Retry delays (see {@link #publishWithDelay}) are implemented using SQS's
 * native per-message {@code DelaySeconds}, not by blocking a thread --
 * this is what lets AtlasFlow's exponential-backoff-and-jitter retry
 * policy (core.retry.RetryPolicy) actually be non-blocking: a worker that
 * fails a task immediately goes back to polling for other work instead of
 * sleeping, and the delayed message simply doesn't become visible to any
 * consumer until its delay elapses.
 */
@Component
public class SqsTaskEventPublisher {

    /** SQS's hard limit on a single message's DelaySeconds. */
    private static final int SQS_MAX_DELAY_SECONDS = 900;

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final String taskQueueName;
    private final String deadLetterQueueName;
    private String taskQueueUrl;
    private String deadLetterQueueUrl;

    public SqsTaskEventPublisher(SqsClient sqsClient, ObjectMapper objectMapper, AtlasFlowProperties properties) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.taskQueueName = properties.getTaskQueueName();
        this.deadLetterQueueName = properties.getDeadLetterQueueName();
    }

    @PostConstruct
    public void resolveQueueUrls() {
        this.taskQueueUrl = resolveQueueUrl(taskQueueName);
        this.deadLetterQueueUrl = resolveQueueUrl(deadLetterQueueName);
    }

    private String resolveQueueUrl(String queueName) {
        return sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
    }

    public void publish(TaskDispatchMessage message) {
        send(taskQueueUrl, message, Duration.ZERO);
    }

    public void publishWithDelay(TaskDispatchMessage message, Duration delay) {
        send(taskQueueUrl, message, delay);
    }

    public void publishToDeadLetterQueue(TaskDispatchMessage message) {
        send(deadLetterQueueUrl, message, Duration.ZERO);
    }

    private void send(String queueUrl, TaskDispatchMessage message, Duration delay) {
        String body;
        try {
            body = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            // A TaskDispatchMessage is a simple record of primitives -- if
            // this ever fails, it's a programming error (e.g. a field type
            // Jackson genuinely can't handle), not a transient/retryable
            // condition, so surface it loudly rather than swallowing it.
            throw new IllegalStateException("failed to serialize task dispatch message: " + message, e);
        }

        int delaySeconds = (int) Math.min(delay.toSeconds(), SQS_MAX_DELAY_SECONDS);
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .delaySeconds(delaySeconds)
                .build());
    }
}
