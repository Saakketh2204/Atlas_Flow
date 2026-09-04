package com.atlasflow.domain;

/** Lifecycle states of an entire workflow execution (all of its tasks combined). */
public enum ExecutionStatus {
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}
