package ru.infon.queuebox.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import ru.infon.queuebox.QueueBox;
import ru.infon.queuebox.RoutedMessage;
import ru.infon.queuebox.common.PropertiesBox;

import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * 07.06.2017
 *
 * @author KostaPC
 * 2017 Infon ZED
 **/
public class MongoRoutedQueueBox<T extends RoutedMessage> extends QueueBox<T> {

    public static final String PROPERTY_THREADS_COUNT = "queue.threads.count";
    private static final int DEFAULT_THREADS_COUNT = 10;

    private final MongoCollection<Document> collection;
    private final int threadsCount;
    private final Class<T> packetClass;

    public MongoRoutedQueueBox(Properties properties, Class<T> packetCLass) {
        super(new PropertiesBox(properties), packetCLass);
        this.packetClass = packetCLass;
        MongoConnection connection = new MongoConnection(properties);
        this.collection = connection.getMongoCollection(Document.class);
        this.threadsCount = getProperties().tryGetIntProperty(
                PROPERTY_THREADS_COUNT,
                connection.getMongoClient().getMongoClientOptions().getConnectionsPerHost()
        );
    }

    public MongoRoutedQueueBox(MongoDatabase mongoDatabase, Properties properties, Class<T> packetCLass) {
        super(new PropertiesBox(properties), packetCLass);
        this.packetClass = packetCLass;
        properties.put(MongoConnection.MONGO_DB_DB, "ignored-value");
        MongoConnection connection = new MongoConnection(properties);
        // un till getClient() or getDatabase() called - connection not attempted to create.
        this.collection = mongoDatabase.getCollection(connection.getMongoCollectionName());
        this.threadsCount = getProperties().tryGetIntProperty(
            PROPERTY_THREADS_COUNT,
            DEFAULT_THREADS_COUNT
        );
    }

    @Override
    public void start() {
        if (behave == null) {
            this.withQueueBehave(new MongoRoutedQueueBehave<>(collection, getProperties(), packetClass));
        }
        if (this.executor == null) {
            // additional thread for timer and common tasks
            this.executor = Executors.newFixedThreadPool(threadsCount + 1);
        }
        super.start();
    }
}
