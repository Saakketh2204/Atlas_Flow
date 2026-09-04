import com.atlasflow.core.idempotency.IdempotencyKey;
import com.atlasflow.core.lease.LeaseEvaluator;
import com.atlasflow.core.partition.ConsistentHashPartitioner;
import com.atlasflow.core.retry.RetryPolicy;
import com.atlasflow.core.workflow.CycleDetectedException;
import com.atlasflow.core.workflow.TaskNode;
import com.atlasflow.core.workflow.WorkflowGraph;
import com.atlasflow.domain.ExecutionStatus;
import com.atlasflow.domain.TaskExecution;
import com.atlasflow.domain.TaskStatus;
import com.atlasflow.domain.WorkflowDefinition;
import com.atlasflow.domain.WorkflowExecution;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Standalone, dependency-free verification of AtlasFlow's core
 * distributed-systems logic (idempotency, retry/backoff, consistent
 * hashing, DAG scheduling, lease expiration).
 *
 * WHY THIS EXISTS: the full project depends on Spring Boot, the AWS SDK,
 * JUnit, and Testcontainers, all resolved from Maven Central. In an
 * environment without access to Maven Central (a restricted corporate
 * network, an air-gapped CI runner, or — how this file first came to
 * exist — a sandboxed dev environment), `mvn test` cannot run at all, even
 * though the core algorithmic logic itself has zero external dependencies
 * and doesn't actually need Maven for anything.
 *
 * This file exercises that core logic directly, compiled and run with
 * nothing but a JDK:
 *
 *   ./scripts/verify-core-logic.sh
 *
 * It is intentionally NOT a replacement for the real JUnit test suite
 * under src/test/java (which is more thorough and is what CI runs) — it's
 * a fast, zero-dependency sanity check that the core logic is present and
 * behaving correctly, usable anywhere a JDK exists.
 */
public final class OfflineCoreVerifier {

    private static int checksRun = 0;
    private static int checksFailed = 0;

    public static void main(String[] args) {
        System.out.println("AtlasFlow -- offline core-logic verification (no Maven/network required)");
        System.out.println("=".repeat(75));

        verifyRetryPolicy();
        verifyConsistentHashPartitioner();
        verifyWorkflowGraph();
        verifyIdempotencyKey();
        verifyLeaseEvaluator();
        verifyDomainModel();

        System.out.println("=".repeat(75));
        System.out.printf("%d checks run, %d failed%n", checksRun, checksFailed);
        if (checksFailed > 0) {
            System.out.println("FAILED");
            System.exit(1);
        }
        System.out.println("ALL CHECKS PASSED");
    }

    // ---- RetryPolicy ----

    private static void verifyRetryPolicy() {
        section("RetryPolicy (exponential backoff + jitter)");

        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), 6);
        check("cap at attempt 1 is base delay (1s)",
                policy.capForAttempt(1).equals(Duration.ofSeconds(1)));
        check("cap at attempt 2 is 2s", policy.capForAttempt(2).equals(Duration.ofSeconds(2)));
        check("cap at attempt 4 is 8s", policy.capForAttempt(4).equals(Duration.ofSeconds(8)));
        check("cap at attempt 6 is clamped to maxDelay (30s), not 32s",
                policy.capForAttempt(6).equals(Duration.ofSeconds(30)));
        check("cap stays clamped for very large attempt numbers",
                policy.capForAttempt(50).equals(Duration.ofSeconds(30)));

        check("shouldRetry is true below maxAttempts", policy.shouldRetry(1) && policy.shouldRetry(5));
        check("shouldRetry is false at maxAttempts", !policy.shouldRetry(6));

        boolean allDelaysWithinCap = true;
        for (int attempt = 1; attempt <= 6; attempt++) {
            Duration cap = policy.capForAttempt(attempt);
            for (int i = 0; i < 200; i++) {
                Duration delay = policy.nextDelay(attempt);
                if (delay.isNegative() || delay.compareTo(cap) > 0) {
                    allDelaysWithinCap = false;
                }
            }
        }
        check("1200 sampled jittered delays all fall within [0, cap]", allDelaysWithinCap);

        java.util.random.RandomGenerator alwaysHalf = new java.util.random.RandomGenerator() {
            public long nextLong() { return 0; }
            public double nextDouble() { return 0.5; }
        };
        RetryPolicy deterministic = new RetryPolicy(Duration.ofSeconds(4), Duration.ofSeconds(60), 5, alwaysHalf);
        check("fixed random source (0.5) at attempt 2 (cap=8s) yields exactly 4s",
                deterministic.nextDelay(2).equals(Duration.ofSeconds(4)));

        boolean rejectedBadArgs = throwsIllegalArgument(() -> new RetryPolicy(Duration.ZERO, Duration.ofSeconds(1), 1));
        check("rejects zero baseDelay", rejectedBadArgs);
    }

    // ---- ConsistentHashPartitioner ----

    private static void verifyConsistentHashPartitioner() {
        section("ConsistentHashPartitioner");

        List<String> owners = List.of("scheduler-1", "scheduler-2", "scheduler-3");
        ConsistentHashPartitioner partitioner = new ConsistentHashPartitioner(owners, 150);

        boolean allAssignedToKnownOwner = true;
        for (int i = 0; i < 300; i++) {
            if (!owners.contains(partitioner.ownerFor("workflow-" + i))) {
                allAssignedToKnownOwner = false;
            }
        }
        check("every key maps to one of the registered owners", allAssignedToKnownOwner);

        String first = partitioner.ownerFor("stable-key");
        boolean stable = true;
        for (int i = 0; i < 20; i++) {
            if (!partitioner.ownerFor("stable-key").equals(first)) {
                stable = false;
            }
        }
        check("the same key always maps to the same owner", stable);

        Map<String, String> before = new HashMap<>();
        int n = 2000;
        for (int i = 0; i < n; i++) {
            before.put("wf-" + i, partitioner.ownerFor("wf-" + i));
        }
        partitioner.addOwner("scheduler-4");
        int moved = 0;
        for (int i = 0; i < n; i++) {
            if (!before.get("wf-" + i).equals(partitioner.ownerFor("wf-" + i))) {
                moved++;
            }
        }
        double fractionMoved = moved / (double) n;
        check("adding a 4th owner moves a minority of keys (moved " + Math.round(fractionMoved * 100) + "%, expected well under 50%)",
                fractionMoved < 0.5);
    }

    // ---- WorkflowGraph ----

    private static void verifyWorkflowGraph() {
        section("WorkflowGraph (ProcessOrder DAG)");

        WorkflowGraph graph = WorkflowGraph.of(
                new TaskNode("ValidatePayment", List.of()),
                new TaskNode("ReserveInventory", List.of("ValidatePayment")),
                new TaskNode("Ship", List.of("ReserveInventory")),
                new TaskNode("Notify", List.of("ReserveInventory")),
                new TaskNode("CompleteOrder", List.of("Ship", "Notify")));

        check("only the root task is ready initially",
                graph.readyTasks(Set.of(), Set.of()).equals(List.of("ValidatePayment")));

        List<String> afterValidate = graph.readyTasks(Set.of("ValidatePayment"), Set.of());
        check("ReserveInventory becomes ready after ValidatePayment completes",
                afterValidate.equals(List.of("ReserveInventory")));

        List<String> afterReserve = graph.readyTasks(Set.of("ValidatePayment", "ReserveInventory"), Set.of());
        check("both Ship and Notify become ready in parallel after ReserveInventory",
                Set.copyOf(afterReserve).equals(Set.of("Ship", "Notify")));

        List<String> onlyShipDone = graph.readyTasks(Set.of("ValidatePayment", "ReserveInventory", "Ship"), Set.of());
        check("CompleteOrder is NOT ready when only one of its two dependencies is done",
                !onlyShipDone.contains("CompleteOrder"));

        List<String> bothDone = graph.readyTasks(
                Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify"), Set.of());
        check("CompleteOrder becomes ready once both Ship and Notify are done",
                bothDone.equals(List.of("CompleteOrder")));

        check("graph is not complete until every task is done",
                !graph.isComplete(Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify")));
        check("graph is complete once all 5 tasks are done",
                graph.isComplete(Set.of("ValidatePayment", "ReserveInventory", "Ship", "Notify", "CompleteOrder")));

        boolean cycleDetected = false;
        try {
            WorkflowGraph.of(new TaskNode("A", List.of("B")), new TaskNode("B", List.of("A")));
        } catch (CycleDetectedException e) {
            cycleDetected = true;
        }
        check("a 2-node cycle (A depends on B, B depends on A) is detected", cycleDetected);

        boolean danglingDepRejected = throwsIllegalArgument(
                () -> WorkflowGraph.of(new TaskNode("A", List.of("does-not-exist"))));
        check("a dependency on a nonexistent task is rejected", danglingDepRejected);
    }

    // ---- IdempotencyKey ----

    private static void verifyIdempotencyKey() {
        section("IdempotencyKey");

        IdempotencyKey key = new IdempotencyKey("exec-123", "ReserveInventory", 2);
        IdempotencyKey parsed = IdempotencyKey.parse(key.asString());
        check("asString/parse round-trips to an equal key", key.equals(parsed));
        check("parsed fields match the original", parsed.workflowExecutionId().equals("exec-123")
                && parsed.taskId().equals("ReserveInventory") && parsed.attempt() == 2);

        IdempotencyKey attempt1 = new IdempotencyKey("exec-123", "Ship", 1);
        IdempotencyKey attempt2 = new IdempotencyKey("exec-123", "Ship", 2);
        check("different attempts of the same task produce different keys (legitimate retries are allowed)",
                !attempt1.equals(attempt2));

        IdempotencyKey dupA = new IdempotencyKey("exec-123", "Ship", 1);
        IdempotencyKey dupB = new IdempotencyKey("exec-123", "Ship", 1);
        check("identical (execution, task, attempt) triples collide -- this is what makes redelivery detectable",
                dupA.equals(dupB) && dupA.asString().equals(dupB.asString()));

        boolean rejectsDelimiterInField = throwsIllegalArgument(() -> new IdempotencyKey("exec#123", "Ship", 1));
        check("rejects a field containing the delimiter character", rejectsDelimiterInField);
    }

    // ---- LeaseEvaluator ----

    private static void verifyLeaseEvaluator() {
        section("LeaseEvaluator (worker-failure detection)");

        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(30));
        Instant expiration = Instant.parse("2026-01-01T00:00:30Z");

        check("lease is not expired one second before its expiration",
                !evaluator.isExpired(expiration, expiration.minusSeconds(1)));
        check("lease IS expired exactly at its expiration instant (inclusive boundary)",
                evaluator.isExpired(expiration, expiration));
        check("lease is expired well after its expiration",
                evaluator.isExpired(expiration, expiration.plus(Duration.ofMinutes(5))));

        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        check("expirationFor adds the default lease duration",
                evaluator.expirationFor(now).equals(now.plusSeconds(30)));
    }

    // ---- Domain model (WorkflowDefinition / WorkflowExecution / TaskExecution) ----

    private static void verifyDomainModel() {
        section("Domain model (WorkflowDefinition + execution state transitions)");

        WorkflowDefinition processOrder = WorkflowDefinition.processOrderExample();
        check("processOrderExample builds a valid 5-task graph",
                processOrder.toGraph().size() == 5);
        check("processOrderExample's graph agrees with WorkflowGraph on what's ready first",
                processOrder.toGraph().readyTasks(Set.of(), Set.of()).equals(List.of("ValidatePayment")));

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        WorkflowExecution execution = WorkflowExecution.newExecution("exec-1", "process-order-v1", t0);
        check("a new execution starts RUNNING with no completed tasks",
                execution.status() == com.atlasflow.domain.ExecutionStatus.RUNNING
                        && execution.completedTaskIds().isEmpty());

        Instant t1 = t0.plusSeconds(5);
        WorkflowExecution afterOneTask = execution.withTaskCompleted("ValidatePayment", t1);
        check("withTaskCompleted adds to completedTaskIds without mutating the original instance",
                afterOneTask.completedTaskIds().equals(Set.of("ValidatePayment"))
                        && execution.completedTaskIds().isEmpty());
        check("withTaskCompleted updates updatedAt but preserves createdAt",
                afterOneTask.updatedAt().equals(t1) && afterOneTask.createdAt().equals(t0));

        TaskExecution pending = TaskExecution.pending("exec-1", "ReserveInventory", t0);
        check("a pending task starts at attempt 0 with no worker/lease",
                pending.status() == TaskStatus.PENDING && pending.attempt() == 0
                        && pending.workerId().isEmpty() && pending.leaseExpiresAt().isEmpty());

        TaskExecution scheduled = pending.withScheduled(t1);
        check("withScheduled increments the attempt counter", scheduled.attempt() == 1);
        check("the original pending instance is unchanged (immutability)", pending.attempt() == 0);

        Instant lease = t1.plusSeconds(30);
        TaskExecution running = scheduled.withClaimedBy("worker-7", lease, t1);
        check("withClaimedBy records the worker id and lease expiration",
                running.status() == TaskStatus.RUNNING
                        && running.workerId().equals(Optional.of("worker-7"))
                        && running.leaseExpiresAt().equals(Optional.of(lease)));

        TaskExecution reclaimed = running.withReclaimed(lease.plusSeconds(1));
        check("withReclaimed returns the task to PENDING and clears the worker/lease",
                reclaimed.status() == TaskStatus.PENDING
                        && reclaimed.workerId().isEmpty()
                        && reclaimed.leaseExpiresAt().isEmpty());
        check("reclaiming does not reset the attempt counter (it's still the same attempt, just orphaned)",
                reclaimed.attempt() == running.attempt());

        TaskExecution completed = running.withCompleted(lease);
        check("withCompleted clears the lease and marks COMPLETED",
                completed.status() == TaskStatus.COMPLETED && completed.leaseExpiresAt().isEmpty());

        TaskExecution retryingThenRescheduled = running.withRetrying("boom", lease).withScheduled(lease.plusSeconds(1));
        check("a retry's failure reason survives into the rescheduled SCHEDULED state (stays visible until success)",
                retryingThenRescheduled.status() == TaskStatus.SCHEDULED
                        && retryingThenRescheduled.lastError().equals(Optional.of("boom")));
        check("withCompleted still clears lastError once the retried attempt actually succeeds",
                retryingThenRescheduled.withCompleted(lease.plusSeconds(2)).lastError().isEmpty());
    }

    // ---- tiny assertion helpers ----

    private static void section(String name) {
        System.out.println();
        System.out.println("-- " + name + " --");
    }

    private static void check(String description, boolean condition) {
        checksRun++;
        if (condition) {
            System.out.println("  [PASS] " + description);
        } else {
            checksFailed++;
            System.out.println("  [FAIL] " + description);
        }
    }

    private interface ThrowingRunnable {
        void run();
    }

    private static boolean throwsIllegalArgument(ThrowingRunnable runnable) {
        try {
            runnable.run();
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }
}
