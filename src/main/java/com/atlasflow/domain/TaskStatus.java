package com.atlasflow.domain;

/** Lifecycle states of a single task's execution within a workflow run. */
public enum TaskStatus {
    /** Not yet dispatched -- waiting on unmet dependencies. */
    PENDING,
    /** Dependencies satisfied, dispatched onto the event bus, awaiting a worker. */
    SCHEDULED,
    /** A worker has claimed the task and is executing it. */
    RUNNING,
    /** Finished successfully. */
    COMPLETED,
    /** The most recent attempt failed but a retry will be scheduled. */
    RETRYING,
    /** All retry attempts exhausted; sent to the dead-letter queue. */
    FAILED,
    /** Its owning workflow execution was cancelled before this task ran. */
    CANCELLED
}
