package com.atlasflow.core.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    @Test
    void capGrowsExponentiallyUntilItHitsMaxDelay() {
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), 10);
        assertEquals(Duration.ofSeconds(1), policy.capForAttempt(1));
        assertEquals(Duration.ofSeconds(2), policy.capForAttempt(2));
        assertEquals(Duration.ofSeconds(4), policy.capForAttempt(3));
        assertEquals(Duration.ofSeconds(8), policy.capForAttempt(4));
        assertEquals(Duration.ofSeconds(16), policy.capForAttempt(5));
        // 32s would be next, but that's capped at maxDelay=30s
        assertEquals(Duration.ofSeconds(30), policy.capForAttempt(6));
        assertEquals(Duration.ofSeconds(30), policy.capForAttempt(20));
    }

    @Test
    void shouldRetryIsTrueUntilMaxAttemptsReached() {
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), 3);
        assertTrue(policy.shouldRetry(1));
        assertTrue(policy.shouldRetry(2));
        assertFalse(policy.shouldRetry(3)); // attempt 3 is the last allowed attempt, not a retry candidate
    }

    @Test
    void nextDelayNeverExceedsTheCap() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        for (int attempt = 1; attempt <= 8; attempt++) {
            Duration cap = policy.capForAttempt(attempt);
            for (int i = 0; i < 50; i++) {
                Duration delay = policy.nextDelay(attempt);
                assertTrue(delay.compareTo(cap) <= 0, "delay " + delay + " exceeded cap " + cap);
                assertFalse(delay.isNegative());
            }
        }
    }

    @Test
    void jitterProducesDifferentDelaysAcrossCalls() {
        // With a real random source, repeated calls at the same attempt
        // number should not all be identical (this would fail if jitter
        // were accidentally disabled/hardcoded to a constant).
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(10), Duration.ofSeconds(60), 5);
        long distinctValues = java.util.stream.IntStream.range(0, 30)
                .mapToObj(i -> policy.nextDelay(4))
                .distinct()
                .count();
        assertTrue(distinctValues > 1, "expected jitter to produce varying delays");
    }

    @Test
    void deterministicWithFixedRandomSource() {
        // A RandomGenerator that always returns 0.5 makes the jittered
        // delay exactly half of the cap — useful for pinning down the
        // jitter formula itself, not just its statistical properties.
        RandomGenerator alwaysHalf = new RandomGenerator() {
            public long nextLong() { return 0; }
            public double nextDouble() { return 0.5; }
        };
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(4), Duration.ofSeconds(60), 5, alwaysHalf);
        // cap at attempt 2 is 8s; half of that is 4s
        assertEquals(Duration.ofSeconds(4), policy.nextDelay(2));
    }

    @Test
    void rejectsInvalidConstructorArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(Duration.ZERO, Duration.ofSeconds(30), 5));
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(Duration.ofSeconds(10), Duration.ofSeconds(5), 5));
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), 0));
    }
}
