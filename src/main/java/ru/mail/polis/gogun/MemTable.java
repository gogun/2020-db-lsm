package ru.mail.polis.gogun;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

final class MemTable implements Table {

    private final NavigableMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes = 0L;
    private int size;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException {
        return Iterators.transform(
                map.tailMap(from)
                        .entrySet()
                        .iterator(),
                e -> new Row(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        Value value1 = map.get(key);

        if (value1 != null) {
            sizeInBytes += value.remaining() - value1.getData().remaining();
        } else {
            sizeInBytes += key.remaining() + value.remaining() + Long.BYTES;
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
        size = map.size();
    }


    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {

        Value value = map.get(key);

        if (!map.containsKey(key)) {
            sizeInBytes += Long.BYTES + key.remaining();
        }
        if (value != null && !value.isTompstone()) {
            sizeInBytes -= value.getData().remaining();
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis()));
        size = map.size();
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }

    public int getSize() {
        return size;
    }
}
