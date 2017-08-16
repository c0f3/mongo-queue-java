package ru.infon.queuebox;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

import static ru.infon.queuebox.QueueBox.PROPERTY_FETCH_DELAY_MILLS;

/**
 * 29.03.2017
 * @author KostaPC
 * 2017 Infon ZED
 */
class QueueConsumerThread<T> {

    private static final Log LOG = LogFactory.getLog(QueueConsumerThread.class);

    private static final int DEFAULT_FETCH_DELAY_MILLS = 100;

    private ExecutorService executor;
    private Properties properties;

    private QueueConsumer<T> consumer;
    private QueuePacketHolder<T> packetHolder;
    private Timer timer;
    private int fetchDelayMills = DEFAULT_FETCH_DELAY_MILLS;

    QueueConsumerThread(
            Properties properties,
            QueueConsumer<T> consumer,
            QueuePacketHolder<T> packetHolder,
            ExecutorService executor
    ) {
        this.properties = properties;
        this.executor = executor;
        this.consumer = consumer;
        this.packetHolder = packetHolder;
        try {
            fetchDelayMills = Integer.parseInt(
                    properties.getProperty(PROPERTY_FETCH_DELAY_MILLS)
            );
        } catch (NumberFormatException | NullPointerException ignore) {}
        timer = new Timer("QCT_timer_"+consumer.getConsumerId());
    }

    void start() {
        LOG.info(String.format(
                "starting QueueConsumerThread for %s",
                consumer
        ));
        executor.execute(()-> runTask(this::payload));
    }

    private Collection<MessageContainer<T>> payload() {
        try {
            return packetHolder.fetch(consumer);
        } catch (Throwable e) {
            LOG.debug(e);
            return Collections.EMPTY_LIST;
        }
    }

    private void onComplete(Collection<MessageContainer<T>> result) {
        if(result.size()>0) {
            LOG.info(String.format(
                    "worker received %d events for consumer %s",
                    result.size(), consumer.getConsumerId()
            ));
        }
        if(result.size()==0) {
            schedule(()-> runTask(this::payload), fetchDelayMills);
        } else {
            Iterator<MessageContainer<T>> it = result.iterator();
            while (!result.isEmpty()) {
                if (!it.hasNext()) {
                    it = result.iterator();
                }
                // if consumer has no free threads - process will wait for
                MessageContainer<T> packet = it.next();
                try {
                    executor.execute(() -> {
                        LOG.debug(String.format(
                                "processing message %s with data: \"%s\"",
                                packet.getId(), packet.getMessage()
                        ));
                        packet.setCallback(
                                (me) -> {packetHolder.ack(me);},
                                (me) -> {packetHolder.reset(me);}
                        );
                        consumer.onPacket(packet);
                    });
                    it.remove();
                } catch (RejectedExecutionException rejected) {
                    LOG.warn(String.format(
                            "task {%s} was rejected by threadpool ... trying again",
                            packet.getId()
                    ));
                }
            }
            LOG.info(String.format(
                    "processing events done for %s", consumer
            ));
            runTask(this::payload);
        }
    }

    private void runTask(Supplier<Collection<MessageContainer<T>>> payload) {
        CompletableFuture.supplyAsync(payload,executor).thenAccept(this::onComplete);
    }

    private void schedule(Runnable runnable, long delay) {
        timer.schedule(new LambdaTimerTask(runnable), delay);
    }

    private class LambdaTimerTask extends TimerTask {

        private Runnable runnable;

        LambdaTimerTask(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }
}
