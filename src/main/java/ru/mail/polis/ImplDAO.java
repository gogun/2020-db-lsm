package ru.mail.polis;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class ImplDAO implements DAO {

    ImplDAO() {
        map = new TreeMap<>();
    }

    private Map<ByteBuffer, ByteBuffer> map;

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return Iterators.transform(
                map.entrySet()
                .stream()
                .dropWhile(e -> !from.equals(e.getKey()) && map.containsKey(from))
                .iterator(),
                e -> new Record(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
