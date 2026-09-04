package com.atlasflow.core.partition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashPartitionerTest {

    @Test
    void everyKeyIsAssignedToOneOfTheRegisteredOwners() {
        List<String> owners = List.of("scheduler-1", "scheduler-2", "scheduler-3");
        ConsistentHashPartitioner partitioner = new ConsistentHashPartitioner(owners, 100);

        for (int i = 0; i < 500; i++) {
            String owner = partitioner.ownerFor("workflow-" + i);
            assertTrue(owners.contains(owner));
        }
    }

    @Test
    void sameKeyAlwaysMapsToTheSameOwner() {
        ConsistentHashPartitioner partitioner =
                new ConsistentHashPartitioner(List.of("scheduler-1", "scheduler-2"), 50);
        String first = partitioner.ownerFor("workflow-abc-123");
        for (int i = 0; i < 20; i++) {
            assertEquals(first, partitioner.ownerFor("workflow-abc-123"));
        }
    }

    @Test
    void addingAnOwnerOnlyMovesAMinorityOfKeys() {
        // This is the entire point of consistent hashing over hash % N:
        // scaling the scheduler pool should NOT reshuffle every key's
        // owner, only a bounded fraction of them.
        List<String> initialOwners = new ArrayList<>(List.of("scheduler-1", "scheduler-2", "scheduler-3"));
        ConsistentHashPartitioner before = new ConsistentHashPartitioner(initialOwners, 200);

        Map<String, String> ownerBeforeByKey = new HashMap<>();
        int numKeys = 2000;
        for (int i = 0; i < numKeys; i++) {
            String key = "workflow-" + i;
            ownerBeforeByKey.put(key, before.ownerFor(key));
        }

        before.addOwner("scheduler-4");

        int moved = 0;
        for (int i = 0; i < numKeys; i++) {
            String key = "workflow-" + i;
            if (!ownerBeforeByKey.get(key).equals(before.ownerFor(key))) {
                moved++;
            }
        }

        double fractionMoved = moved / (double) numKeys;
        // Adding a 4th owner to a ring of 3 should move roughly 1/4 of
        // keys in expectation. Assert a generous upper bound (well below
        // "almost everything moved", which is what plain modulo hashing
        // would do) rather than pinning an exact percentage, since the
        // precise fraction depends on hash distribution luck.
        assertTrue(fractionMoved < 0.5,
                "expected a minority of keys to move, but " + (fractionMoved * 100) + "% moved");
    }

    @Test
    void removingAnOwnerRedistributesOnlyItsKeys() {
        ConsistentHashPartitioner partitioner =
                new ConsistentHashPartitioner(List.of("scheduler-1", "scheduler-2", "scheduler-3"), 150);

        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            before.put("wf-" + i, partitioner.ownerFor("wf-" + i));
        }

        partitioner.removeOwner("scheduler-2");

        for (int i = 0; i < 1000; i++) {
            String key = "wf-" + i;
            String previousOwner = before.get(key);
            String newOwner = partitioner.ownerFor(key);
            if (!previousOwner.equals("scheduler-2")) {
                // Keys that weren't owned by the removed scheduler should
                // be completely unaffected by its removal.
                assertEquals(previousOwner, newOwner);
            } else {
                assertTrue(List.of("scheduler-1", "scheduler-3").contains(newOwner));
            }
        }
    }

    @Test
    void rejectsEmptyOwnerList() {
        assertThrows(IllegalArgumentException.class, () -> new ConsistentHashPartitioner(List.of(), 10));
    }

    @Test
    void throwsWhenAllOwnersRemoved() {
        ConsistentHashPartitioner partitioner = new ConsistentHashPartitioner(List.of("only-one"), 10);
        partitioner.removeOwner("only-one");
        assertThrows(IllegalStateException.class, () -> partitioner.ownerFor("any-key"));
    }
}
