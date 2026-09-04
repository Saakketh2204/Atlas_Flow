package com.atlasflow.core.lease;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Pure logic for deciding whether a worker's lease on a task has expired.
 *
 * The pattern: when a worker picks up a task, it writes a lease with an
 * expiration timestamp (now + leaseDuration) and must renew it (send a
 * heartbeat that extends the expiration) before that time runs out. If the
 * worker crashes, its heartbeats stop, the lease expires, and — critically
 * — this is how AtlasFlow finds out a worker died *without that worker
 * telling it so*: nobody has to detect the crash directly, the recovery
 * engine just needs to periodically ask "is any lease past its
 * expiration?" and treat "yes" as "assume that task is orphaned, make it
 * available for another worker to claim."
 *
 * This class only answers that yes/no question given a lease's recorded
 * expiration and the current time — it doesn't know about DynamoDB, SQS,
 * or workers at all, which is what makes it independently testable
 * (including the tricky boundary-condition and clock-skew cases) without
 * needing a real clock, a real worker, or a real failure to reproduce.
 */
public final class LeaseEvaluator {

    private final Duration defaultLeaseDuration;

    public LeaseEvaluator(Duration defaultLeaseDuration) {
        if (defaultLeaseDuration.isNegative() || defaultLeaseDuration.isZero()) {
            throw new IllegalArgumentException("defaultLeaseDuration must be positive");
        }
        this.defaultLeaseDuration = defaultLeaseDuration;
    }

    public Duration defaultLeaseDuration() {
        return defaultLeaseDuration;
    }

    /** The expiration timestamp for a lease acquired/renewed at {@code now}. */
    public Instant expirationFor(Instant now) {
        return now.plus(defaultLeaseDuration);
    }

    /** The expiration timestamp for a lease acquired/renewed at {@code now},
     * using a caller-specified duration instead of the default. */
    public Instant expirationFor(Instant now, Duration leaseDuration) {
        return now.plus(leaseDuration);
    }

    /**
     * A lease is expired if the current time is at or after its recorded
     * expiration — "at" is included deliberately (not strictly "after") so
     * the check is unambiguous exactly at the boundary instant, rather
     * than depending on tie-breaking semantics that would differ between
     * {@code isAfter} and a hypothetical {@code isAfterOrEqual}.
     */
    public boolean isExpired(Instant leaseExpiration, Instant now) {
        Objects.requireNonNull(leaseExpiration, "leaseExpiration must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return !now.isBefore(leaseExpiration);
    }

    /** How much time remains before the lease expires (negative if already expired). */
    public Duration timeUntilExpiration(Instant leaseExpiration, Instant now) {
        return Duration.between(now, leaseExpiration);
    }
}
