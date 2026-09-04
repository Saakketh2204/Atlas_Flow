package com.atlasflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

/**
 * AtlasFlow: a fault-tolerant distributed workflow orchestration platform.
 *
 * This single application runs as one of two roles depending on the
 * active Spring profile (see docker-compose.yml):
 *   - default profile: control plane -- REST API + failure recovery engine
 *   - "worker" profile: task executor -- polls SQS, runs task handlers
 *
 * {@code @EnableScheduling} activates both the worker's SQS-polling loop
 * ({@code worker.TaskEventListener}) and the control plane's lease-
 * expiration scan ({@code recovery.FailureRecoveryEngine}) -- each is
 * gated to run in only one of the two roles via {@code @Profile}.
 */
@SpringBootApplication
@EnableScheduling
public class AtlasFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(AtlasFlowApplication.class, args);
    }

    /**
     * A single shared {@link Clock} bean, injected everywhere `Instant.now()`
     * would otherwise be called directly. This is what makes the lease-
     * expiration and retry-timing logic deterministically testable: unit
     * tests substitute a {@code Clock.fixed(...)} to control "now" exactly,
     * rather than tests needing real (flaky, slow) `Thread.sleep` calls to
     * observe time-dependent behavior.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
