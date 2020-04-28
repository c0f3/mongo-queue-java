package net.c0f3.queuebox.mongo;

import net.c0f3.queuebox.QueueStatistic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MongoRoutedQueueStatistic implements QueueStatistic {

    private final AtomicLong unsetValue = new AtomicLong(-1);
    private final ConcurrentHashMap<String, AtomicLong> values = new ConcurrentHashMap<>();

    @Override
    public void increment(String key) {
        values.computeIfAbsent(
                key,
                (k)->new AtomicLong(0)
        ).incrementAndGet();
    }

    @Override
    public String getValue(String key) {
        return values.getOrDefault(key,unsetValue).toString();
    }
}
