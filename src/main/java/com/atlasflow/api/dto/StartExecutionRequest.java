package com.atlasflow.api.dto;

import jakarta.validation.constraints.NotBlank;

public record StartExecutionRequest(@NotBlank String workflowDefinitionId) {
}
