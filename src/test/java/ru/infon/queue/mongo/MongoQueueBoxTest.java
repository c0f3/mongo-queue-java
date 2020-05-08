package ru.infon.queue.mongo;

import gaillard.mongo.MongoConnectionParams;
import net.c0f3.queuebox.mongo.MongoContainer;
import net.c0f3.queuebox.mongo.MongoTestHelper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.infon.queuebox.MessageContainer;
import ru.infon.queuebox.QueueConsumer;
import ru.infon.queuebox.mongo.MongoConnection;
import ru.infon.queuebox.mongo.MongoRoutedQueueBox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 07.06.2017
 *
 * @author KostaPC
 * 2017 Infon ZED
 **/
@Testcontainers
public class MongoQueueBoxTest {

    @Container
    private static final MongoContainer MONGO = new MongoContainer();

    private MongoConnectionParams mongoParams;

    @BeforeEach
    public void setup() {
        mongoParams = MongoTestHelper.createMongoParams(MONGO);
        MongoConnection mongoConnection = new MongoConnection(mongoParams.getProperties());
        mongoConnection.getMongoCollection(Document.class).deleteMany(new Document());
    }

    @Test
    public void testPutAndGet() throws InterruptedException {
        final int iterations = 10;
        final String defaultSource = "just_source";
        final String defaultDestination = "just_destination";

        MongoRoutedQueueBox<JustPojoRouted> queueBox = new MongoRoutedQueueBox<>(
                mongoParams.getProperties(),
                JustPojoRouted.class
        );
        queueBox.start();
        Map<Integer, JustPojoRouted> sendPojos = new HashMap<>();
        final List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            JustPojoRouted pojo = new JustPojoRouted(i, "_" + i + "_!");
            pojo.setSource(defaultSource);
            pojo.setDestination(defaultDestination);
            sendPojos.put(i, pojo);
            futures.add(queueBox.queue(pojo));
        }

        long count = 0;
        while (count < futures.size()) {
            count = futures.stream().filter(Future::isDone).count();
        }
        System.out.println("ALL INSERTED!");


        CountDownLatch door = new CountDownLatch(iterations);
        queueBox.subscribe(new QueueConsumer<JustPojoRouted>() {
            @Override
            public void onPacket(MessageContainer<JustPojoRouted> message) {
                JustPojoRouted recvPojo = message.getMessage();
                JustPojoRouted sendPojo = sendPojos.get(recvPojo.getIntValue());
                assertNotNull(recvPojo);
                assertNotNull(sendPojo);
                assertEquals(sendPojo, recvPojo);
                message.done();
                door.countDown();
            }

            @Override
            public String getConsumerId() {
                return defaultDestination;
            }
        });

        door.await(60000, TimeUnit.MILLISECONDS);

        assertEquals(0, door.getCount());
        MongoConnection mongoConnection = new MongoConnection(mongoParams.getProperties());
        assertEquals(
                0,
                mongoConnection.getMongoCollection(Document.class).countDocuments()
        );

        System.out.println("ALL DONE!");

    }

}
