package ru.infon.queuebox;

import net.c0f3.queuebox.QueueBoxContext;
import net.c0f3.queuebox.QueueStatistic;
import ru.infon.queuebox.common.PropertiesBox;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 23.03.2017
 * @author KostaPC
 * Copyright (c) 2017 Infon. All rights reserved.
 *
 * Object must be singletone
 */
public class QueueBox<T> {

    public static final int PRIORITY_HIGH = 1;
    public static final int PRIORITY_NORMAL = 4; // default priority value from documentation
    public static final int PRIORITY_LOW = 10;
    public static final int PRIORITY_DEFAULT = PRIORITY_NORMAL;

    public static final String PROPERTY_FETCH_DELAY_MILLS = "queue.fetch.delay.mills";

    private QueueEngine<T> queue = null;
    private final QueueBoxContext queueBoxContext;
    protected QueueBehave<T> behave = null;
    protected ExecutorService executor = null;

    private final PropertiesBox properties;
    private final Class<T> packetClass;

    final AtomicBoolean started = new AtomicBoolean(false);

    public QueueBox(PropertiesBox properties, Class<T> packetCLass) {
        this.properties = properties;
        this.packetClass = packetCLass;
        this.queueBoxContext = new QueueBoxContext();
        this.queueBoxContext.setStatistic(QueueStatistic.voidInstance());
    }

    public QueueBox<T> withExecutorService(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public QueueBox<T> withQueueBehave(QueueBehave<T> queueBehave) {
        this.behave = queueBehave;
        this.behave.setContext(queueBoxContext);
        return this;
    }

    public QueueBox<T> withStatistic(QueueStatistic statistic) {
        this.queueBoxContext.setStatistic(statistic);
        return this;
    }

    public QueueStatistic getStatistic() {
        return queueBoxContext.getStatistic();
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

    public void subscribe(String destination, Consumer<T> consumer) {
        subscribe(new QueueConsumer<T>() {
            @Override
            public void onPacket(MessageContainer<T> message) {
                consumer.accept(message.getMessage());
            }

            @Override
            public String getConsumerId() {
                return destination;
            }
        });
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
