package net.c0f3.queuebox;

import gaillard.mongo.MongoConnectionParams;
import net.c0f3.queuebox.mongo.MongoContainer;
import net.c0f3.queuebox.mongo.MongoTestHelper;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.infon.queue.mongo.JustPojoRouted;
import ru.infon.queuebox.MessageContainer;
import ru.infon.queuebox.QueueBox;
import ru.infon.queuebox.QueueConsumer;
import ru.infon.queuebox.mongo.MongoConnection;
import ru.infon.queuebox.mongo.MongoRoutedQueueBox;
import ru.infon.queuebox.mongo.MongoRoutedQueueBehave;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Testcontainers
public class LongTasksExecutionOverlappingTest {

    private static final Logger log = LoggerFactory.getLogger(LongTasksExecutionOverlappingTest.class);

    @Container
    private static final MongoContainer MONGO = new MongoContainer();

    private static final String DESTINATION = "default-destination";

    private MongoConnectionParams mongoParams;

    @BeforeEach
    public void setup() {
        mongoParams = MongoTestHelper.createMongoParams(MONGO);
        Properties props = mongoParams.getProperties();
        props.put(MongoRoutedQueueBehave.PROPERTY_FETCH_LIMIT, 1);
        MongoConnection boxMongoConnection = new MongoConnection(props);
        boxMongoConnection.getMongoCollection(Document.class).deleteMany(new Document());
    }

    @Test
    public void testExecutionOverlap() throws InterruptedException {
        int tasksCount = 10;
        int delaySeconds = 5;

        QueueBox<JustPojoRouted> queueBox = new MongoRoutedQueueBox<>(
                mongoParams.getProperties(),
                JustPojoRouted.class
        );
        queueBox.start();
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger order = new AtomicInteger(0);

        CountDownLatch door = new CountDownLatch(tasksCount);

        queueBox.subscribe(new QueueConsumer<JustPojoRouted>() {
            @Override
            public void onPacket(MessageContainer<JustPojoRouted> message) {
                int activeTasks = counter.incrementAndGet();
                log.info(
                        "{}) M: {} -> running tasks: {}",
                        order.incrementAndGet(),
                        message.getMessage().getIntValue(),
                        activeTasks
                );
                Assertions.assertEquals(1, activeTasks);
                longTask(delaySeconds);
                counter.decrementAndGet();
                message.done();
                door.countDown();
            }

            @Override
            public String getConsumerId() {
                return DESTINATION;
            }
        });

        for (int i = 0; i < tasksCount; i++) {
            queueBox.queue(randomMessage(i));
        }

        long time = System.currentTimeMillis();
        door.await(delaySeconds * (tasksCount+1), TimeUnit.SECONDS);
        time = System.currentTimeMillis() - time;

        Assertions.assertEquals(0, counter.get());

        log.info("test passed, execution time: " + time);

    }

    private static JustPojoRouted randomMessage(int counter) {
        JustPojoRouted message = new JustPojoRouted();
        message.setDestination(DESTINATION);
        message.setSource("test");
        message.setIntValue(counter);
        message.setStringValue("");
        return message;
    }

    private static void longTask(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }
    }

}
