package com.atlasflow.core.retry;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with full jitter, following the pattern described in
 * the AWS Architecture Blog post "Exponential Backoff And Jitter":
 * delay = random_between(0, min(maxDelay, baseDelay * 2^attempt))
 *
 * "Full jitter" (rather than no jitter, or "equal jitter") is used
 * deliberately: when many clients fail at the same time (e.g. a downstream
 * dependency blips), naive exponential backoff without jitter causes them
 * all to retry in lockstep, producing synchronized retry storms that can
 * re-overwhelm the dependency the moment it recovers. Full jitter spreads
 * retries across the whole backoff window instead.
 *
 * This class has zero external dependencies by design — it is part of
 * AtlasFlow's dependency-free "core" package, which contains the
 * algorithmically interesting distributed-systems logic in isolation from
 * the Spring/AWS SDK framework layer. That separation means this logic
 * compiles and is unit-testable with nothing but a JDK, independent of
 * whether Maven can reach a dependency repository — see
 * scripts/verify-core-logic.sh.
 */
public final class RetryPolicy {

    private final Duration baseDelay;
    private final Duration maxDelay;
    private final int maxAttempts;
    private final RandomGenerator random;

    public RetryPolicy(Duration baseDelay, Duration maxDelay, int maxAttempts) {
        this(baseDelay, maxDelay, maxAttempts, RandomGenerator.getDefault());
    }

    /** Constructor taking an explicit random source, for deterministic testing. */
    public RetryPolicy(Duration baseDelay, Duration maxDelay, int maxAttempts, RandomGenerator random) {
        if (baseDelay.isNegative() || baseDelay.isZero()) {
            throw new IllegalArgumentException("baseDelay must be positive");
        }
        if (maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be >= baseDelay");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.maxAttempts = maxAttempts;
        this.random = random;
    }

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), 5);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public boolean shouldRetry(int attemptNumber) {
        // attemptNumber is 1-indexed: the first attempt is attempt 1, and it
        // is not itself a "retry" — retries are attempts 2..maxAttempts.
        return attemptNumber < maxAttempts;
    }

    /**
     * The upper bound of the backoff window for a given (1-indexed) attempt
     * number, before jitter is applied — i.e. min(maxDelay, baseDelay * 2^(attempt-1)).
     * Exposed separately from {@link #nextDelay} so callers/tests can
     * reason about the deterministic growth curve independent of the
     * random jitter draw.
     */
    public Duration capForAttempt(int attemptNumber) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be >= 1");
        }
        // attempt 1 -> exponent 0 (no backoff yet, this is the first try)
        int exponent = attemptNumber - 1;
        // Guard against overflow for pathologically large attempt numbers —
        // once the exponential term exceeds maxDelay it can only be
        // clamped anyway, so there's no need to compute it exactly.
        if (exponent >= 32) {
            return maxDelay;
        }
        long scaledMillis = baseDelay.toMillis() * (1L << exponent);
        long cappedMillis = Math.min(scaledMillis, maxDelay.toMillis());
        return Duration.ofMillis(cappedMillis);
    }

    /** A jittered delay to wait before making the given (1-indexed) attempt. */
    public Duration nextDelay(int attemptNumber) {
        Duration cap = capForAttempt(attemptNumber);
        long capMillis = cap.toMillis();
        if (capMillis == 0) {
            return Duration.ZERO;
        }
        long jitteredMillis = (long) (random.nextDouble() * capMillis);
        return Duration.ofMillis(jitteredMillis);
    }
}
