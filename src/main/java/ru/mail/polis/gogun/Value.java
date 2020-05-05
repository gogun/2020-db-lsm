package ru.mail.polis.gogun;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Optional;

final class Value implements Comparable<Value> {

    private final long timestamp;
    @NotNull
    private final Optional<ByteBuffer> data;

    Value(final long timestamp, @NotNull final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = Optional.of(data);
    }

    Value(final long timestamp) {
        this.timestamp = timestamp;
        this.data = Optional.empty();
    }

    boolean isTompstone() {
        return data.isEmpty();
    }

    @NotNull
    ByteBuffer getData() {
        return data.orElseThrow().asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
