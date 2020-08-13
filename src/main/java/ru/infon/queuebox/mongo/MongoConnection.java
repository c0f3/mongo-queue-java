package ru.infon.queuebox.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 14.10.2016
 *
 * @author kostapc
 * 2016 Infon
 * 2020 c0f3
 */
public class MongoConnection {

    public static final String MONGO_DB_URL = "default.mongodb.uri";
    public static final String MONGO_DB_DB = "default.mongodb.database";
    public static final String MONGO_DB_USER = "default.mongodb.user";
    public static final String MONGO_DB_PASSWORD = "default.mongodb.password";
    public static final String MONGO_QUEUE_COLLECTION_NAME = "mongodb.queue.collection";

    private static final int DEFAULT_CONNECTIONS_COUNT = 10;

    private final AtomicReference<MongoClient> client = new AtomicReference<>();
    private final MongoClientSettings.Builder optionsBuilder;

    private String mongoDBName;
    private String mongoDBUser;
    private String mongoCollectionName;
    private char[] mongoDBPassword;
    private MongoClientSettings clientSettings;

    public MongoConnection(Properties properties) {

        optionsBuilder = MongoClientSettings.builder();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (key.equals(MONGO_DB_URL)) {
                ConnectionString connectionString = new ConnectionString(value);
                optionsBuilder.applyConnectionString(connectionString);
            } else if (key.equals(MONGO_DB_DB)) {
                mongoDBName = value;
            } else if (key.equals(MONGO_DB_USER)) {
                mongoDBUser = value;
            } else if (key.startsWith(MONGO_DB_PASSWORD)) {
                mongoDBPassword = value.toCharArray();
            } else if (key.startsWith(MONGO_QUEUE_COLLECTION_NAME)) {
                mongoCollectionName = value;
            }
        }

        if (mongoDBName == null) {
            throw new RuntimeException("Mandatory property \"database\" not found");
        }

        WriteConcern writeConcern = WriteConcern.W1;
        writeConcern.withJournal(true);
        writeConcern.withWTimeout(0, TimeUnit.MILLISECONDS);
        optionsBuilder.writeConcern(writeConcern);
    }

    private MongoClient initMongoClient() {
        if (mongoDBUser != null && mongoDBPassword != null) {
            optionsBuilder.credential(MongoCredential.createCredential(
                mongoDBUser, mongoDBName, mongoDBPassword
            ));
        }
        MongoDriverInformation driverInfo = MongoDriverInformation.builder().build();
        clientSettings = optionsBuilder.build();
        return new MongoClientImpl(
            clientSettings,
            driverInfo
        );
    }

    private final Object syncFlag = new Object();

    private MongoClient lazyInitMongoClient() {
        MongoClient result = client.get();
        if (result == null) {
            synchronized (syncFlag) {
                result = client.get();
                if (result != null) {
                    return result;
                }
                result = initMongoClient();
                if (!client.compareAndSet(null, result)) {
                    throw new IllegalStateException("lazy MongoClient initialization failed");
                }
            }
        }
        return result;
    }

    /*===========================================[ CLASS METHODS ]==============*/

    public void close() {
        getMongoClient().close();
    }

    public int getConnectionPoolSize() {
        if (clientSettings != null) {
            return clientSettings.getConnectionPoolSettings().getMaxSize();
        } else {
            return DEFAULT_CONNECTIONS_COUNT;
        }
    }

    private MongoClient getMongoClient() {
        return lazyInitMongoClient();
    }

    public String getDatabaseName() {
        return mongoDBName;
    }

    public String getMongoCollectionName() {
        return mongoCollectionName;
    }

    public MongoDatabase getDatabase() {
        return getMongoClient().getDatabase(getDatabaseName());
    }

    public <D> MongoCollection<D> getMongoCollection(Class<D> documentCLass) {
        return getDatabase().getCollection(getMongoCollectionName(), documentCLass);
    }

}
