package net.c0f3.queuebox;

import com.mongodb.MongoClient;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.infon.queuebox.mongo.MongoConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Testcontainers
public class MongoAnonymousConnectionTest {

    @Container
    private static final MongoAnonymousContainer MONGO = new MongoAnonymousContainer();

    @Test
    public void anonymousAuthTest() {
        final String collectionName = "queue_box";

        Properties props = new Properties();
        props.put(MongoConnection.MONGO_DB_DB, MongoContainer.DATABASE);
        props.put(MongoConnection.MONGO_QUEUE_COLLECTION_NAME, collectionName);
        props.put(MongoConnection.MONGO_DB_URL, String.format("%s:%s",
                MONGO.getContainerIpAddress(),
                MONGO.getMappedPort(MongoContainer.ORIGINAL_PORT)
        ));

        MongoConnection connection = new MongoConnection(props);
        try {
            MongoDatabase db = connection.getDatabase();
            MongoCollection<Document> collection = db.getCollection(collectionName);
            Document newDoc = new Document();
            newDoc.put("key", "value");
            collection.insertOne(newDoc);
            List<Document> docs = new ArrayList<>();
            for(Document doc: collection.find()){
                docs.add(doc);
            }
            Assertions.assertEquals(1,docs.size());
            Assertions.assertEquals("value",docs.get(0).get("key"));

        } catch (MongoTimeoutException e) {
            System.out.println("MongoTimeoutException caught");
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }

    }

}
