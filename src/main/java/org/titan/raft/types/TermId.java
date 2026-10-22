package org.titan.raft.types;

import java.util.Objects;

/**
 * Represents a Raft term identifier in the titan-raft-engine.
 * Implemented as a value type wrapping a primitive long to align with the
 * zero-allocation, tightly packed memory layout principles of the system.
 */
public final class TermId implements Comparable<TermId> {

    public static final TermId ZERO = new TermId(0L);
    public static final TermId INITIAL = new TermId(1L);

    private final long value;

    public TermId(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("TermId cannot be negative: " + value);
        }
        this.value = value;
    }

    public long value() {
        return value;
    }

    public TermId increment() {
        if (value == Long.MAX_VALUE) {
            throw new ArithmeticException("TermId overflow");
        }
        return new TermId(value + 1L);
    }

    public boolean isGreaterThan(TermId other) {
        Objects.requireNonNull(other, "other cannot be null");
        return this.value > other.value;
    }

    public boolean isLessThan(TermId other) {
        Objects.requireNonNull(other, "other cannot be null");
        return this.value < other.value;
    }

    public boolean isGreaterThanOrEqual(TermId other) {
        Objects.requireNonNull(other, "other cannot be null");
        return this.value >= other.value;
    }

    public boolean isLessThanOrEqual(TermId other) {
        Objects.requireNonNull(other, "other cannot be null");
        return this.value <= other.value;
    }

    @Override
    public int compareTo(TermId other) {
        Objects.requireNonNull(other, "other cannot be null");
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TermId)) {
            return false;
        }
        TermId other = (TermId) obj;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return "TermId{value=" + value + "}";
    }
}