package com.atlasflow.core.lease;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseEvaluatorTest {

    @Test
    void leaseIsNotExpiredBeforeItsExpirationTime() {
        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(30));
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Instant expiration = now.plusSeconds(30);
        assertFalse(evaluator.isExpired(expiration, now));
        assertFalse(evaluator.isExpired(expiration, expiration.minusSeconds(1)));
    }

    @Test
    void leaseIsExpiredExactlyAtItsExpirationInstant() {
        // Deliberately inclusive: "at or after" counts as expired, so the
        // recovery engine never has to worry about a lease sitting in an
        // ambiguous not-quite-expired state forever due to clock quantization.
        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(30));
        Instant expiration = Instant.parse("2026-01-01T00:00:30Z");
        assertTrue(evaluator.isExpired(expiration, expiration));
    }

    @Test
    void leaseIsExpiredAfterItsExpirationTime() {
        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(30));
        Instant expiration = Instant.parse("2026-01-01T00:00:30Z");
        assertTrue(evaluator.isExpired(expiration, expiration.plusSeconds(1)));
        assertTrue(evaluator.isExpired(expiration, expiration.plus(Duration.ofHours(1))));
    }

    @Test
    void expirationForAddsTheDefaultLeaseDuration() {
        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(45));
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        assertEquals(now.plusSeconds(45), evaluator.expirationFor(now));
    }

    @Test
    void expirationForWithExplicitDurationOverridesDefault() {
        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(45));
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        assertEquals(now.plusSeconds(120), evaluator.expirationFor(now, Duration.ofSeconds(120)));
    }

    @Test
    void timeUntilExpirationIsNegativeAfterExpiry() {
        LeaseEvaluator evaluator = new LeaseEvaluator(Duration.ofSeconds(30));
        Instant expiration = Instant.parse("2026-01-01T00:00:30Z");
        Duration remaining = evaluator.timeUntilExpiration(expiration, expiration.plusSeconds(5));
        assertTrue(remaining.isNegative());
        assertEquals(Duration.ofSeconds(-5), remaining);
    }

    @Test
    void rejectsNonPositiveDefaultLeaseDuration() {
        assertThrows(IllegalArgumentException.class, () -> new LeaseEvaluator(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new LeaseEvaluator(Duration.ofSeconds(-1)));
    }
}
