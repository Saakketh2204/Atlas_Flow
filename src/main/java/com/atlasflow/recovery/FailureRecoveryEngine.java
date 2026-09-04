package com.atlasflow.recovery;

import com.atlasflow.aws.DynamoTaskExecutionRepository;
import com.atlasflow.aws.DynamoWorkflowExecutionRepository;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.scheduler.WorkflowDefinitionRegistry;
import com.atlasflow.scheduler.WorkflowOrchestrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * This is where AtlasFlow actually finds out a worker died: nothing tells
 * it directly (there's no crash notification) -- it periodically scans for
 * tasks stuck RUNNING with an expired lease (see
 * {@code core.lease.LeaseEvaluator} for the pure boundary logic, and
 * {@code DynamoTaskExecutionRepository#findExpiredLeases} for the query)
 * and treats "lease expired" as "assume the worker holding it is gone,
 * make the task available again." No manual intervention required --
 * this is the automated-recovery story the whole platform is built around.
 *
 * Runs only on control-plane instances (not inside worker containers --
 * see the {@code @Profile} on {@code worker.TaskEventListener} for the
 * mirror image of this split), so exactly one class of process is
 * responsible for detecting orphaned work regardless of how many worker
 * replicas are running.
 */
@Component
@Profile("!worker")
public class FailureRecoveryEngine {

    private static final Logger log = LoggerFactory.getLogger(FailureRecoveryEngine.class);

    // NOTE: this interval is a literal, not wired to
    // `atlasflow.recovery-scan-interval` in application.yml. @Scheduled's
    // fixedDelay must be resolvable at bean-registration time; making it
    // dynamically configurable would mean depending on Spring's
    // fixedDelayString duration-suffix parsing (e.g. "10s"), whose exact
    // supported syntax has changed across Spring versions -- not a risk
    // worth taking in code this project can't compile-check locally (see
    // docs/local-development.md). This value matches the property's
    // documented default; change both together if you tune it.
    private static final long SCAN_INTERVAL_MILLIS = 10_000;

    private final DynamoTaskExecutionRepository taskRepository;
    private final DynamoWorkflowExecutionRepository executionRepository;
    private final WorkflowDefinitionRegistry definitionRegistry;
    private final WorkflowOrchestrationService orchestrationService;
    private final Clock clock;

    public FailureRecoveryEngine(
            DynamoTaskExecutionRepository taskRepository,
            DynamoWorkflowExecutionRepository executionRepository,
            WorkflowDefinitionRegistry definitionRegistry,
            WorkflowOrchestrationService orchestrationService,
            Clock clock) {
        this.taskRepository = taskRepository;
        this.executionRepository = executionRepository;
        this.definitionRegistry = definitionRegistry;
        this.orchestrationService = orchestrationService;
        this.clock = clock;
    }

    @Scheduled(fixedDelay = SCAN_INTERVAL_MILLIS)
    public void scanAndReclaimExpiredLeases() {
        Instant now = Instant.now(clock);
        List<TaskExecution> expired = taskRepository.findExpiredLeases(now);

        for (TaskExecution orphaned : expired) {
            reclaimOne(orphaned);
        }

        if (!expired.isEmpty()) {
            log.info("recovery scan reclaimed {} orphaned task(s)", expired.size());
        }
    }

    private void reclaimOne(TaskExecution orphaned) {
        executionRepository.get(orphaned.executionId())
                .flatMap(execution -> definitionRegistry.get(execution.workflowDefinitionId()))
                .ifPresentOrElse(
                        definition -> orchestrationService.reclaimOrphanedTask(definition, orphaned),
                        () -> log.error(
                                "cannot reclaim {}/{}: its execution or workflow definition is no longer known",
                                orphaned.executionId(), orphaned.taskId()));
    }
}
