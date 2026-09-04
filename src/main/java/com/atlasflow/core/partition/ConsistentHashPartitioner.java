package com.atlasflow.core.partition;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hashing ring for assigning workflow IDs to scheduler
 * partitions/owners.
 *
 * Why consistent hashing rather than plain {@code hash(workflowId) % N}:
 * with modulo hashing, adding or removing a single scheduler instance (N
 * changes) reshuffles the owner of almost *every* key, because the modulus
 * itself changed. That would mean every in-flight workflow's ownership
 * moves during a routine scale-up/scale-down or a single node restart.
 * Consistent hashing bounds the disruption: only the keys that fell in the
 * hash range now owned by the joining/leaving node move — on average
 * roughly {@code 1/N} of keys, not all of them. Virtual nodes (multiple
 * ring positions per physical scheduler) smooth out the load distribution
 * that a single hash position per node would otherwise leave lumpy.
 *
 * Zero external dependencies — part of AtlasFlow's dependency-free core
 * package (see RetryPolicy's class-level Javadoc for why that separation
 * exists).
 */
public final class ConsistentHashPartitioner {

    private final SortedMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodesPerOwner;

    public ConsistentHashPartitioner(List<String> owners, int virtualNodesPerOwner) {
        if (owners == null || owners.isEmpty()) {
            throw new IllegalArgumentException("owners must not be empty");
        }
        if (virtualNodesPerOwner < 1) {
            throw new IllegalArgumentException("virtualNodesPerOwner must be >= 1");
        }
        this.virtualNodesPerOwner = virtualNodesPerOwner;
        for (String owner : owners) {
            addOwner(owner);
        }
    }

    public void addOwner(String owner) {
        for (int replica = 0; replica < virtualNodesPerOwner; replica++) {
            long hash = hash(owner + "#" + replica);
            ring.put(hash, owner);
        }
    }

    public void removeOwner(String owner) {
        for (int replica = 0; replica < virtualNodesPerOwner; replica++) {
            long hash = hash(owner + "#" + replica);
            ring.remove(hash);
        }
    }

    /** Which owner is responsible for the given key (e.g. a workflow ID). */
    public String ownerFor(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("no owners registered on the ring");
        }
        long hash = hash(key);
        SortedMap<Long, String> tail = ring.tailMap(hash);
        // Walk clockwise from the key's position; wrap around to the first
        // entry on the ring if the key's hash is past every owner's slot.
        Long ownerSlot = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(ownerSlot);
    }

    public int ringSize() {
        return ring.size();
    }

    private static long hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            // Fold the first 8 bytes of the digest into a long. MD5 is used
            // purely for its fast, well-distributed bit spread here — this
            // is a load-balancing hash, not a security boundary, so MD5's
            // cryptographic weaknesses are irrelevant to this use.
            long result = 0;
            for (int i = 0; i < 8; i++) {
                result = (result << 8) | (bytes[i] & 0xFF);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm unavailable", e);
        }
    }
}
