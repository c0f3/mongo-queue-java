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
import ru.infon.queuebox.QueueBox;
import ru.infon.queuebox.mongo.MongoConnection;
import ru.infon.queuebox.mongo.MongoRoutedQueueBehave;
import ru.infon.queuebox.mongo.MongoRoutedQueueBox;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Testcontainers
public class ConsumerFailToleranceTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFailToleranceTest.class);

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
    public void testConsumerFailedTask() throws InterruptedException {
        QueueBox<JustPojoRouted> queueBox = new MongoRoutedQueueBox<>(
                mongoParams.getProperties(),
                JustPojoRouted.class
        );
        int messagesCount = 3;

        CountDownLatch door = new CountDownLatch(messagesCount);

        queueBox.start();
        queueBox.subscribe(
                DESTINATION,
                (packet)->{
                    log.info("received packet "+packet.getStringValue());
                    door.countDown();
                    throw new IllegalStateException("just exception");
                }
        );

        JustPojoRouted pojo = new JustPojoRouted();
        pojo.setDestination(DESTINATION);
        pojo.setStringValue("some value");
        queueBox.queue(pojo);

        door.await(5, TimeUnit.SECONDS);

        Assertions.assertEquals(0, door.getCount());
    }

}
