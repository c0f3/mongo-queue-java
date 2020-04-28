package net.c0f3.queuebox;

import com.mongodb.client.MongoCollection;
import gaillard.mongo.MongoConnectionParams;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.infon.queue.mongo.JustPojoRouted;
import ru.infon.queuebox.MessageContainer;
import ru.infon.queuebox.QueueBox;
import ru.infon.queuebox.QueueConsumer;
import ru.infon.queuebox.mongo.MongoConnection;
import ru.infon.queuebox.mongo.MongoRoutedQueueBehave;
import ru.infon.queuebox.mongo.MongoRoutedQueueBox;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Testcontainers
public class MongoIndexUtilizationTest {

    @Container
    private static final MongoContainer MONGO = new MongoContainer();

    private static final String DESTINATION = "default-destination";

    // 10 seconds run with empty queue
    @Test
    public void testIndexUtilization() throws InterruptedException {
        final long fetchDelayMills = 100;
        final long runTime = 10000;

        MongoConnectionParams mongoParams = MongoTestHelper.createMongoParams(MONGO);
        Properties props = mongoParams.getProperties();
        props.put(QueueBox.PROPERTY_FETCH_DELAY_MILLS, fetchDelayMills);
        MongoConnection mongoConnection = new MongoConnection(props);
        mongoConnection.getMongoCollection(Document.class).deleteMany(new Document());
        MongoCollection<Document> collection = mongoConnection.getDatabase()
                .getCollection(MongoTestHelper.COLLECTION_NAME);
        collection.dropIndexes();

        MongoRoutedQueueBox<JustPojoRouted> queueBox = new MongoRoutedQueueBox<>(
                mongoParams.getProperties(),
                JustPojoRouted.class
        );
        queueBox.start();
        CountDownLatch door = new CountDownLatch(1);
        queueBox.subscribe(new QueueConsumer<JustPojoRouted>() {
            @Override
            public void onPacket(MessageContainer<JustPojoRouted> message) {
                System.out.println("packet received");
                door.countDown();
            }

            @Override
            public String getConsumerId() {
                return DESTINATION;
            }
        });
        JustPojoRouted message = new JustPojoRouted(1, "one");
        message.setSource("test");
        message.setDestination(DESTINATION);
        queueBox.queue(message);

        door.await(10000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(0, door.getCount());
        Thread.sleep(runTime);

        long indexOpsCounter = 0;

        for (Document cur : collection.aggregate(
                Collections.singletonList(new Document("$indexStats", new Document()))
        )) {
            if (((Document) cur.get("key")).containsKey("payload.destination")) {
                System.out.println(cur.toJson(JsonWriterSettings.builder().indent(true).build()));
                indexOpsCounter = ((Number) ((Document) cur.get("accesses")).get("ops")).longValue();
            }
        }

        long findOperations = Long.parseLong(
                queueBox.getStatistic().getValue(MongoRoutedQueueBehave.STAT_FIND_COUNTER)
        );
        long expectedOperationsCount = (runTime / fetchDelayMills) + 1; // edge timing
        System.out.println(String.format(
                "find operations: %s; index usages: %s; expected operations count: %s",
                findOperations,
                indexOpsCounter,
                expectedOperationsCount
        ));
        System.out.println("breakpoint");

        Assertions.assertTrue(findOperations <= expectedOperationsCount);
        // update and get
        Assertions.assertTrue(indexOpsCounter <= expectedOperationsCount * 2);

    }

}
