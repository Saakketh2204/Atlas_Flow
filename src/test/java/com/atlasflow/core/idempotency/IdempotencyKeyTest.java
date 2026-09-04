package com.atlasflow.core.idempotency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdempotencyKeyTest {

    @Test
    void asStringRoundTripsThroughParse() {
        IdempotencyKey key = new IdempotencyKey("exec-123", "ReserveInventory", 2);
        IdempotencyKey parsed = IdempotencyKey.parse(key.asString());
        assertEquals(key, parsed);
        assertEquals("exec-123", parsed.workflowExecutionId());
        assertEquals("ReserveInventory", parsed.taskId());
        assertEquals(2, parsed.attempt());
    }

    @Test
    void differentAttemptsProduceDifferentKeys() {
        IdempotencyKey attempt1 = new IdempotencyKey("exec-123", "Ship", 1);
        IdempotencyKey attempt2 = new IdempotencyKey("exec-123", "Ship", 2);
        assertNotEquals(attempt1, attempt2);
        assertNotEquals(attempt1.asString(), attempt2.asString());
    }

    @Test
    void sameExecutionTaskAndAttemptProduceEqualKeys() {
        // This is the property the whole idempotency mechanism relies on:
        // re-delivery of the *same* attempt must collide, not be treated
        // as a new key.
        IdempotencyKey a = new IdempotencyKey("exec-123", "Ship", 1);
        IdempotencyKey b = new IdempotencyKey("exec-123", "Ship", 1);
        assertEquals(a, b);
        assertEquals(a.asString(), b.asString());
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void rejectsComponentsContainingTheDelimiter() {
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey("exec#123", "Ship", 1));
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey("exec-123", "Sh#ip", 1));
    }

    @Test
    void rejectsNonPositiveAttempt() {
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey("exec-123", "Ship", 0));
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey("exec-123", "Ship", -1));
    }

    @Test
    void rejectsMalformedStringOnParse() {
        assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.parse("not-enough-parts"));
        assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.parse("a#b#not-a-number"));
        assertThrows(IllegalArgumentException.class, () -> IdempotencyKey.parse("a#b#c#d"));
    }

    @Test
    void rejectsEmptyComponents() {
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey("", "Ship", 1));
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyKey("exec-123", "", 1));
    }
}
