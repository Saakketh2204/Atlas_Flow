package com.atlasflow.api.dto;

import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import com.atlasflow.domain.WorkflowExecution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record ExecutionStatusResponse(
        String executionId,
        String workflowDefinitionId,
        ExecutionStatus status,
        List<String> completedTaskIds,
        List<TaskStatusView> tasks,
        Instant createdAt,
        Instant updatedAt) {

    public static ExecutionStatusResponse from(WorkflowExecution execution, List<TaskExecution> tasks) {
        return new ExecutionStatusResponse(
                execution.executionId(),
                execution.workflowDefinitionId(),
                execution.status(),
                execution.completedTaskIds().stream().sorted().toList(),
                tasks.stream().map(TaskStatusView::from).toList(),
                execution.createdAt(),
                execution.updatedAt());
    }

    public record TaskStatusView(
            String taskId, TaskStatus status, int attempt, Optional<String> workerId, Optional<String> lastError) {

        public static TaskStatusView from(TaskExecution task) {
            return new TaskStatusView(task.taskId(), task.status(), task.attempt(), task.workerId(), task.lastError());
        }
    }
}
