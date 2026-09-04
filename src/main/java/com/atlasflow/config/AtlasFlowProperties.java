package com.atlasflow.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * All AtlasFlow-specific configuration in one place, bound from
 * {@code application.yml}'s {@code atlasflow.*} keys. See
 * src/main/resources/application.yml for the default values and
 * docker-compose.yml for how they're overridden for local development
 * against LocalStack.
 */
@ConfigurationProperties(prefix = "atlasflow")
public class AtlasFlowProperties {

    /** DynamoDB table names. */
    private String executionsTable = "atlasflow-executions";
    private String tasksTable = "atlasflow-tasks";
    private String idempotencyTable = "atlasflow-idempotency";

    /** SQS queue URLs (or names, resolved to URLs at startup). */
    private String taskQueueName = "atlasflow-tasks";
    private String deadLetterQueueName = "atlasflow-tasks-dlq";

    /** Optional endpoint override -- set to LocalStack's URL for local dev,
     * left null in production to use AWS's real regional endpoints. */
    private String awsEndpointOverride;

    private String awsRegion = "us-east-1";

    /** Lease duration a worker holds on a task before it's considered orphaned. */
    private Duration leaseDuration = Duration.ofSeconds(30);

    /** How often the recovery engine scans for expired leases. */
    private Duration recoveryScanInterval = Duration.ofSeconds(10);

    /** How often a running worker renews (heartbeats) its lease. Should be
     * comfortably shorter than leaseDuration -- this repo defaults to 1/3,
     * giving a heartbeat two chances to fail before the lease actually lapses. */
    private Duration heartbeatInterval = Duration.ofSeconds(10);

    private Duration retryBaseDelay = Duration.ofSeconds(1);
    private Duration retryMaxDelay = Duration.ofSeconds(30);
    private int retryMaxAttempts = 5;

    private int schedulerMaxBatchSize = 8;

    /** If set, this task ID will deterministically fail its first N attempts
     * (see chaosFailCount) before succeeding -- a controllable way to demo
     * the retry/backoff path without relying on real, non-reproducible
     * failures. Leave null/blank in production. */
    private String chaosFailTaskName;
    private int chaosFailCount = 0;

    /** Max messages fetched per SQS ReceiveMessage call (SQS's own hard cap is 10). */
    private int workerPollBatchSize = 5;
    /** SQS long-poll wait time -- reduces empty-poll overhead/cost vs. short polling. */
    private int sqsPollWaitSeconds = 10;

    public String getExecutionsTable() {
        return executionsTable;
    }

    public void setExecutionsTable(String executionsTable) {
        this.executionsTable = executionsTable;
    }

    public String getTasksTable() {
        return tasksTable;
    }

    public void setTasksTable(String tasksTable) {
        this.tasksTable = tasksTable;
    }

    public String getIdempotencyTable() {
        return idempotencyTable;
    }

    public void setIdempotencyTable(String idempotencyTable) {
        this.idempotencyTable = idempotencyTable;
    }

    public String getTaskQueueName() {
        return taskQueueName;
    }

    public void setTaskQueueName(String taskQueueName) {
        this.taskQueueName = taskQueueName;
    }

    public String getDeadLetterQueueName() {
        return deadLetterQueueName;
    }

    public void setDeadLetterQueueName(String deadLetterQueueName) {
        this.deadLetterQueueName = deadLetterQueueName;
    }

    public String getAwsEndpointOverride() {
        return awsEndpointOverride;
    }

    public void setAwsEndpointOverride(String awsEndpointOverride) {
        this.awsEndpointOverride = awsEndpointOverride;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(Duration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    public Duration getRecoveryScanInterval() {
        return recoveryScanInterval;
    }

    public void setRecoveryScanInterval(Duration recoveryScanInterval) {
        this.recoveryScanInterval = recoveryScanInterval;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Duration getRetryBaseDelay() {
        return retryBaseDelay;
    }

    public void setRetryBaseDelay(Duration retryBaseDelay) {
        this.retryBaseDelay = retryBaseDelay;
    }

    public Duration getRetryMaxDelay() {
        return retryMaxDelay;
    }

    public void setRetryMaxDelay(Duration retryMaxDelay) {
        this.retryMaxDelay = retryMaxDelay;
    }

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public void setRetryMaxAttempts(int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    public int getSchedulerMaxBatchSize() {
        return schedulerMaxBatchSize;
    }

    public void setSchedulerMaxBatchSize(int schedulerMaxBatchSize) {
        this.schedulerMaxBatchSize = schedulerMaxBatchSize;
    }

    public String getChaosFailTaskName() {
        return chaosFailTaskName;
    }

    public void setChaosFailTaskName(String chaosFailTaskName) {
        this.chaosFailTaskName = chaosFailTaskName;
    }

    public int getChaosFailCount() {
        return chaosFailCount;
    }

    public void setChaosFailCount(int chaosFailCount) {
        this.chaosFailCount = chaosFailCount;
    }

    public int getWorkerPollBatchSize() {
        return workerPollBatchSize;
    }

    public void setWorkerPollBatchSize(int workerPollBatchSize) {
        this.workerPollBatchSize = workerPollBatchSize;
    }

    public int getSqsPollWaitSeconds() {
        return sqsPollWaitSeconds;
    }

    public void setSqsPollWaitSeconds(int sqsPollWaitSeconds) {
        this.sqsPollWaitSeconds = sqsPollWaitSeconds;
    }
}
