package gaillard.mongo;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.c0f3.queuebox.mongo.MongoContainer;
import net.c0f3.queuebox.mongo.MongoTestHelper;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.infon.queuebox.mongo.MongoConnection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class QueueTest {

    @Container
    private static final MongoContainer MONGO = new MongoContainer();

    private static final String COLLECTION_NAME = "messages";

    private MongoCollection<Document> collection;
    private MongoQueueCore queue;
    private MongoDatabase db = null;
    private AtomicReference<MongoConnection> connectionRef = new AtomicReference<>(null);

    @BeforeEach
    public void setup() {

        MongoConnectionParams mongoParams = MongoTestHelper.createMongoParams(MONGO);
        MongoConnection connection = new MongoConnection(mongoParams.getProperties());
        connectionRef.set(connection);
        try {
            //client = connection.getMongoClient();
            db = connection.getDatabase();
            System.out.println("Using real Mongodb instance");
        } catch (MongoTimeoutException e) {
            System.out.println("MongoTimeoutException caught");
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }
        collection = db.getCollection(COLLECTION_NAME);
        collection.drop();

        queue = new MongoQueueCore(collection);
    }

    @AfterEach
    public void closeConnection() {
        MongoConnection connection = connectionRef.get();
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void construct_nullCollection() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MongoQueueCore(null)
        );
    }

    @Test
    public void ensureGetIndexMultiplyCalls() {
        queue.ensureGetIndex(new Document("type", 1).append("boo", -1));
        queue.ensureGetIndex(new Document("type", 1).append("boo", -1));
        queue.ensureGetIndex(new Document("type", 1).append("boo", -1));

        final ListIndexesIterable<Document> indexes = collection.listIndexes();

        Function<Document, Document> getIndexKeys = (index) -> {
            Document byKey = (Document) index.get("key");
            return (Document) byKey.get("key");
        };
        boolean indexFound = false;

        for (Document doc : indexes) {
            System.out.println(doc);
            Document indexKeys = getIndexKeys.apply(doc);
            if (indexKeys == null) {
                return;
            }
            if (indexKeys.containsKey("payload.type") && indexKeys.containsKey("payload.boo")) {
                indexFound = true;
            }
        }

        Assertions.assertTrue(indexFound);
    }

    @Test
    public void ensureGetIndex() {
        queue.ensureGetIndex(new Document("type", 1).append("boo", -1));
        queue.ensureGetIndex(new Document("another.sub", 1));

        final ListIndexesIterable<Document> indexes = collection.listIndexes();
        List<Document> indexInfo = new ArrayList<>();
        indexInfo = indexes.into(indexInfo);

        assertEquals(4, indexInfo.size());

        final Document expectedOne = new Document("running", 1)
            .append("payload.type", 1)
            .append("priority", 1)
            .append("created", 1)
            .append("payload.boo", -1)
            .append("earliestGet", 1);
        assertEquals(expectedOne, indexInfo.get(1).get("key"));

        final Document expectedTwo = new Document("running", 1).append("resetTimestamp", 1);
        assertEquals(expectedTwo, indexInfo.get(2).get("key"));

        final Document expectedThree = new Document("running", 1)
            .append("payload.another.sub", 1)
            .append("priority", 1)
            .append("created", 1)
            .append("earliestGet", 1);
        assertEquals(expectedThree, indexInfo.get(3).get("key"));
    }

    @Test
    public void ensureGetIndex_noArgs() {
        queue.ensureGetIndex();

        final ListIndexesIterable<Document> indexes = collection.listIndexes();
        List<Document> indexInfo = new ArrayList<>();
        indexInfo = indexes.into(indexInfo);

        assertEquals(3, indexInfo.size());

        final Document expectedOne = new Document("running", 1).append("priority", 1).append("created", 1).append("earliestGet", 1);
        assertEquals(expectedOne, indexInfo.get(1).get("key"));

        final Document expectedTwo = new Document("running", 1).append("resetTimestamp", 1);
        assertEquals(expectedTwo, indexInfo.get(2).get("key"));
    }

    @Test
    public void ensureGetIndex_tooLongCollectionName() {
        //121 chars
        final String collectionName = "messages01234567890123456789012345678901234567890123456789"
            + "012345678901234567890123456789012345678901234567890123456789012";

        queue = new MongoQueueCore(db.getCollection(collectionName));
        Assertions.assertThrows(
            MongoCommandException.class,
            () -> queue.ensureGetIndex()
        );
    }

    @Test
    public void ensureGetIndex_badBeforeSortValue() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.ensureGetIndex(new Document("field", "NotAnInt"))
        );

    }

    @Test
    public void ensureGetIndex_badAfterSortValue() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.ensureGetIndex(new Document(), new Document("field", "NotAnInt"))
        );
    }

    @Test
    public void ensureGetIndex_nullBeforeSort() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ensureGetIndex(null)
        );
    }

    @Test
    public void ensureGetIndex_nullAfterSort() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ensureGetIndex(new Document(), null)
        );

    }

    @Test
    public void ensureCountIndex() {
        queue.ensureCountIndex(new Document("type", 1).append("boo", -1), false);
        queue.ensureCountIndex(new Document("another.sub", 1), true);

        final ListIndexesIterable<Document> indexes = collection.listIndexes();
        List<Document> indexInfo = new ArrayList<>();
        indexInfo = indexes.into(indexInfo);

        assertEquals(3, indexInfo.size());

        final Document expectedOne = new Document("payload.type", 1).append("payload.boo", -1);
        assertEquals(expectedOne, indexInfo.get(1).get("key"));

        final Document expectedTwo = new Document("running", 1).append("payload.another.sub", 1);
        assertEquals(expectedTwo, indexInfo.get(2).get("key"));
    }

    @Test
    public void ensureCountIndex_badValue() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.ensureCountIndex(new Document("field", "NotAnInt"), true)
        );
    }

    @Test
    public void ensureCountIndex_null() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ensureCountIndex(null, true)
        );

    }

    @Test
    public void get_nullQuery() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.get(null, Integer.MAX_VALUE)
        );
    }

    @Test
    public void get_badQuery() {
        queue.send(new Document("key", 0));

        assertNull(queue.get(new Document("key", 1), Integer.MAX_VALUE, 0));
    }

    @Test
    public void get_fullQuery() {
        final Document message = new Document("id", "ID SHOULD BE REMOVED").append("key1", 0).append("key2", true);

        queue.send(message);
        queue.send(new Document());

        final Document result = queue.get(message, Integer.MAX_VALUE);
        assertNotEquals(message.get("id"), result.get("id"));

        message.put("id", result.get("id"));
        assertEquals(message, result);
    }

    @Test
    public void get_subQuery() {
        final Document messageOne = new Document("one", new Document("two", new Document("three", 5)));
        final Document messageTwo = new Document("one", new Document("two", new Document("three", 4)));

        queue.send(messageOne);
        queue.send(messageTwo);

        final Document result = queue.get(new Document("one.two.three", new Document("$gte", 5)), Integer.MAX_VALUE);

        messageOne.put("id", result.get("id"));
        assertEquals(messageOne, result);
    }

    @Test
    public void get_negativeWait() {
        assertNull(queue.get(new Document(), Integer.MAX_VALUE, Integer.MIN_VALUE));

        queue.send(new Document());

        assertNotNull(queue.get(new Document(), Integer.MAX_VALUE, Integer.MIN_VALUE));
    }

    @Test
    public void get_negativePoll() {
        assertNull(queue.get(new Document(), Integer.MAX_VALUE, 100, Long.MIN_VALUE));

        queue.send(new Document());

        assertNotNull(queue.get(new Document(), Integer.MAX_VALUE, 100, Long.MIN_VALUE));
    }

    @Test
    public void get_beforeAck() {
        queue.send(new Document());

        assertNotNull(queue.get(new Document(), Integer.MAX_VALUE));

        //try get message we already have before ack
        assertNull(queue.get(new Document(), Integer.MAX_VALUE, 0));
    }

    @Test
    public void get_customPriority() {
        final Document messageOne = new Document("key", 1);
        final Document messageTwo = new Document("key", 2);
        final Document messageThree = new Document("key", 3);

        queue.send(messageOne, new Date(), 0.5);
        queue.send(messageTwo, new Date(), 0.4);
        queue.send(messageThree, new Date(), 0.3);

        final Document resultOne = queue.get(new Document(), Integer.MAX_VALUE);
        final Document resultTwo = queue.get(new Document(), Integer.MAX_VALUE);
        final Document resultThree = queue.get(new Document(), Integer.MAX_VALUE);

        assertEquals(messageOne.get("key"), resultThree.get("key"));
        assertEquals(messageTwo.get("key"), resultTwo.get("key"));
        assertEquals(messageThree.get("key"), resultOne.get("key"));
    }

    @Test
    public void get_timePriority() {
        final Document messageOne = new Document("key", 1);
        final Document messageTwo = new Document("key", 2);
        final Document messageThree = new Document("key", 3);

        queue.send(messageOne, new Date());
        queue.send(messageTwo, new Date());
        queue.send(messageThree, new Date());

        final Document resultOne = queue.get(new Document(), Integer.MAX_VALUE);
        final Document resultTwo = queue.get(new Document(), Integer.MAX_VALUE);
        final Document resultThree = queue.get(new Document(), Integer.MAX_VALUE);

        assertEquals(messageOne.get("key"), resultOne.get("key"));
        assertEquals(messageTwo.get("key"), resultTwo.get("key"));
        assertEquals(messageThree.get("key"), resultThree.get("key"));
    }

    @Test
    public void get_wait() {
        final long start = System.nanoTime();

        queue.get(new Document(), Integer.MAX_VALUE, 200);

        final long elapsed = (System.nanoTime() - start) / 1000000;

        System.out.println("get_wait elapsed time is: " + elapsed);
        assertTrue(elapsed >= 200);
        assertTrue(elapsed < 400);
    }

    @Test
    public void get_waitWhenMessageExists() {
        final Date start = new Date();

        queue.send(new Document());

        queue.get(new Document(), Integer.MAX_VALUE, 3000);

        assertTrue(new Date().getTime() - start.getTime() < 2000);
    }

    @Test
    public void get_earliestGet() throws InterruptedException {
        queue.send(new Document(), new Date(System.currentTimeMillis() + 200));

        assertNull(queue.get(new Document(), Integer.MAX_VALUE, 0));

        Thread.sleep(200);

        assertNotNull(queue.get(new Document(), Integer.MAX_VALUE));
    }

    @Test
    public void get_resetStuck() {
        queue.send(new Document());

        //sets resetTimestamp on messageOne
        assertNotNull(queue.get(new Document(), 0));

        assertNotNull(queue.get(new Document(), Integer.MAX_VALUE));
    }

    @Test
    public void count_running() {
        assertEquals(0, queue.count(new Document(), true));
        assertEquals(0, queue.count(new Document(), false));
        assertEquals(0, queue.count(new Document()));

        queue.send(new Document("key", 1));

        assertEquals(0, queue.count(new Document(), true));
        assertEquals(1, queue.count(new Document(), false));
        assertEquals(1, queue.count(new Document()));

        queue.get(new Document(), Integer.MAX_VALUE);

        assertEquals(1, queue.count(new Document(), true));
        assertEquals(0, queue.count(new Document(), false));
        assertEquals(1, queue.count(new Document()));
    }

    @Test
    public void count_fullQuery() {
        final Document message = new Document("key", 1);

        queue.send(new Document());
        queue.send(message);

        assertEquals(1, queue.count(message));
    }

    @Test
    public void count_subQuery() {
        final Document messageOne = new Document("one", new Document("two", new Document("three", 4)));
        final Document messageTwo = new Document("one", new Document("two", new Document("three", 5)));

        queue.send(messageOne);
        queue.send(messageTwo);

        assertEquals(1, queue.count(new Document("one.two.three", new Document("$gte", 5))));
    }

    @Test
    public void count_badQuery() {
        queue.send(new Document("key", 0));

        assertEquals(0, queue.count(new Document("key", 1)));
    }

    @Test
    public void count_nullQuery() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.count(null)
        );
    }

    @Test
    public void count_runningNullQuery() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.count(null, true)
        );
    }

    @Test
    public void ack() {
        final Document message = new Document("key", 0);

        queue.send(message);
        queue.send(new Document());

        final Document result = queue.get(message, Integer.MAX_VALUE);
        assertEquals(2, collection.countDocuments());

        queue.ack(result);
        assertEquals(1, collection.countDocuments());
    }

    @Test
    public void ack_wrongIdType() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.ack(new Document("id", false))
        );

    }

    @Test
    public void ack_null() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ack(null)
        );
    }

    @Test
    public void ackSend() {
        final Document message = new Document("key", 0);

        queue.send(message);

        final Document resultOne = queue.get(message, Integer.MAX_VALUE);

        final Date expectedEarliestGet = new Date();
        final double expectedPriority = 0.8;
        final Date timeBeforeAckSend = new Date();
        queue.ackSend(resultOne, new Document("key", 1), expectedEarliestGet, expectedPriority);

        assertEquals(1, collection.countDocuments());

        // find one, i.e. the first document in this collection
        final Document actual = collection.find().first();

        assert actual != null;
        final Date actualCreated = actual.getDate("created");
        assertTrue(actualCreated.compareTo(timeBeforeAckSend) >= 0 && actualCreated.compareTo(new Date()) <= 0);

        final Document expected = new Document("_id", resultOne.get("id"))
            .append("payload", new Document("key", 1))
            .append("running", false)
            .append("resetTimestamp", new Date(Long.MAX_VALUE))
            .append("earliestGet", expectedEarliestGet)
            .append("priority", expectedPriority)
            .append("created", actual.get("created"));

        assertEquals(expected, actual);
    }

    @Test
    public void ackSend_wrongIdType() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.ackSend(new Document("id", 5), new Document())
        );

    }

    @Test
    public void ackSend_nanPriority() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.ackSend(new Document("id", ObjectId.get()), new Document(), new Date(), Double.NaN)
        );

    }

    @Test
    public void ackSend_nullMessage() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ackSend(null, new Document())
        );

    }

    @Test
    public void ackSend_nullPayload() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ackSend(new Document("id", ObjectId.get()), null)
        );

    }

    @Test
    public void ackSend_nullEarliestGet() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.ackSend(new Document("id", ObjectId.get()), new Document(), null)
        );

    }

    @Test
    public void requeue() {
        final Document message = new Document("key", 0);

        queue.send(message);

        final Document resultOne = queue.get(message, Integer.MAX_VALUE);

        final Date expectedEarliestGet = new Date();
        final double expectedPriority = 0.8;
        final Date timeBeforeRequeue = new Date();
        queue.requeue(resultOne, expectedEarliestGet, expectedPriority);

        assertEquals(1, collection.countDocuments());

        // find one, i.e. the first document in this collection
        final Document actual = collection.find().first();

        assert actual != null;
        final Date actualCreated = actual.getDate("created");
        assertTrue(actualCreated.compareTo(timeBeforeRequeue) >= 0 && actualCreated.compareTo(new Date()) <= 0);

        final Document expected = new Document("_id", resultOne.get("id"))
            .append("payload", new Document("key", 0))
            .append("running", false)
            .append("resetTimestamp", new Date(Long.MAX_VALUE))
            .append("earliestGet", expectedEarliestGet)
            .append("priority", expectedPriority)
            .append("created", actual.get("created"));

        assertEquals(expected, actual);
    }

    @Test
    public void requeue_wrongIdType() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.requeue(new Document("id", new Document()))
        );

    }

    @Test
    public void requeue_nanPriority() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.requeue(new Document("id", ObjectId.get()), new Date(), Double.NaN)
        );

    }

    @Test
    public void requeue_nullMessage() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.requeue(null)
        );

    }

    @Test
    public void requeue_nullEarliestGet() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.requeue(new Document("id", ObjectId.get()), null)
        );

    }

    @Test
    public void send() {
        final Document message = new Document("key", 0);

        final Date expectedEarliestGet = new Date();
        final double expectedPriority = 0.8;
        final Date timeBeforeSend = new Date();
        queue.send(message, expectedEarliestGet, expectedPriority);

        assertEquals(1, collection.countDocuments());

        // find one, i.e. the first document in this collection
        final Document actual = collection.find().first();

        assert actual != null;
        final Date actualCreated = actual.getDate("created");
        assertTrue(actualCreated.compareTo(timeBeforeSend) >= 0 && actualCreated.compareTo(new Date()) <= 0);

        final Document expected = new Document("_id", actual.get("_id"))
            .append("payload", new Document("key", 0))
            .append("running", false)
            .append("resetTimestamp", new Date(Long.MAX_VALUE))
            .append("earliestGet", expectedEarliestGet)
            .append("priority", expectedPriority)
            .append("created", actual.get("created"));

        assertEquals(expected, actual);
    }

    @Test
    public void send_nanPriority() {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> queue.send(new Document("id", ObjectId.get()), new Date(), Double.NaN)
        );

    }

    @Test
    public void send_nullMessage() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.send(null)
        );
    }

    @Test
    public void send_nullEarliestGet() {
        Assertions.assertThrows(
            NullPointerException.class,
            () -> queue.send(new Document("id", ObjectId.get()), null)
        );
    }
}
