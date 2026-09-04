package com.atlasflow.worker;

import com.atlasflow.config.AtlasFlowProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulated handlers for the ProcessOrder example workflow
 * (ValidatePayment -> ReserveInventory -> {Ship, Notify} -> CompleteOrder).
 * Each one just logs and sleeps briefly to stand in for real work -- the
 * point of this repo is the orchestration platform around task execution
 * (scheduling, retries, idempotency, recovery), not these handlers'
 * business logic.
 *
 * Deterministic chaos injection: when {@code atlasflow.chaos-fail-task-name}
 * matches a handler's task, that handler will throw for its first
 * {@code atlasflow.chaos-fail-count} invocations (tracked globally, across
 * all executions) before succeeding -- a reliable way to demonstrate the
 * retry-with-backoff path in a live demo, rather than hoping for a real
 * failure to happen to occur.
 */
@Component
public class DemoTaskHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoTaskHandlers.class);

    private final TaskHandlerRegistry registry;
    private final AtlasFlowProperties properties;
    private final ConcurrentHashMap<String, AtomicInteger> chaosInvocationCounts = new ConcurrentHashMap<>();

    public DemoTaskHandlers(TaskHandlerRegistry registry, AtlasFlowProperties properties) {
        this.registry = registry;
        this.properties = properties;
    }

    @PostConstruct
    void registerHandlers() {
        registry.register("validatePayment", (executionId, taskId) -> simulate(executionId, taskId, 80));
        registry.register("reserveInventory", (executionId, taskId) -> simulate(executionId, taskId, 120));
        registry.register("ship", (executionId, taskId) -> simulate(executionId, taskId, 150));
        registry.register("notify", (executionId, taskId) -> simulate(executionId, taskId, 60));
        registry.register("completeOrder", (executionId, taskId) -> simulate(executionId, taskId, 40));
    }

    private void simulate(String executionId, String taskId, long simulatedWorkMillis) throws Exception {
        maybeInjectChaosFailure(taskId);
        log.info("executing {}/{}", executionId, taskId);
        Thread.sleep(simulatedWorkMillis);
        log.info("completed {}/{}", executionId, taskId);
    }

    private void maybeInjectChaosFailure(String taskId) throws Exception {
        if (properties.getChaosFailTaskName() == null || !properties.getChaosFailTaskName().equals(taskId)) {
            return;
        }
        AtomicInteger count = chaosInvocationCounts.computeIfAbsent(taskId, t -> new AtomicInteger(0));
        int invocation = count.incrementAndGet();
        if (invocation <= properties.getChaosFailCount()) {
            throw new RuntimeException(
                    "chaos-injected failure for task '" + taskId + "' (attempt " + invocation
                            + " of " + properties.getChaosFailCount() + " forced failures)");
        }
    }
}
