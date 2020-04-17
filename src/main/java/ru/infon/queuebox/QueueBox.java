package ru.infon.queuebox;

import ru.infon.queuebox.common.PropertiesBox;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 23.03.2017
 * @author KostaPC
 * Copyright (c) 2017 Infon. All rights reserved.
 *
 * Object must be singletone
 * TODO: select library name and add root interface for queue
 */
public class QueueBox<T> {

    public static final int PRIORITY_HIGH = 1;
    public static final int PRIORITY_NORMAL = 4; // default priority value from documentation
    public static final int PRIORITY_LOW = 10;
    public static final int PRIORITY_DEFAULT = PRIORITY_NORMAL;

    public static final String PROPERTY_FETCH_DELAY_MILLS = "queue.fetch.delay.mills";

    private QueueEngine<T> queue = null;
    protected QueueBehave<T> behave = null;
    protected ExecutorService executor = null;

    private final PropertiesBox properties;
    private final Class<T> packetClass;

    final AtomicBoolean started = new AtomicBoolean(false);

    public QueueBox(PropertiesBox properties, Class<T> packetCLass) {
        this.properties = properties;
        this.packetClass = packetCLass;
    }

    public QueueBox<T> withExecutorService(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public QueueBox<T> withQueueBehave(QueueBehave<T> queueBehave) {
        this.behave = queueBehave;
        return this;
    }

    public PropertiesBox getProperties() {
        return properties;
    }

    public void start() {
        Objects.requireNonNull(behave);
        Objects.requireNonNull(executor);
        this.queue = new QueueEngine<>(properties, behave, executor);
        started.set(true);
    }

    public void stop() {
        queue.shutdown();
    }

    public void subscribe(QueueConsumer<T> consumer) {
        if(!started.get()) {
            throw new IllegalStateException("QueueBox not started");
        }
        executor.submit(()-> queue.registerConsumer(consumer));
    }

    public Future<T> queue(T message) {
        if(!started.get()) {
            throw new IllegalStateException("QueueBox not started");
        }
        return executor.submit(()->{
            queue.queue(new MessageContainer<>(message));
            return message;
        });
    }

    public Future<T> queue(T message, int priority) {
        if(!started.get()) {
            throw new IllegalStateException("QueueBox not started");
        }
        return executor.submit(()->{
            MessageContainer<T> messageContainer = new MessageContainer<>(message);
            messageContainer.setPriority(priority);
            queue.queue(messageContainer);
            return message;
        });
    }
}
