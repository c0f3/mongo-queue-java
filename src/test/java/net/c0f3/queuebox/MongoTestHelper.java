package net.c0f3.queuebox;

import gaillard.mongo.MongoConnectionParams;
import ru.infon.queuebox.mongo.MongoConnection;

import java.util.Properties;

public class MongoTestHelper {

    public static String COLLECTION_NAME = "queue_box";

    public static MongoConnectionParams createMongoParams(MongoContainer container) {
        Properties props = new Properties();
        props.put(MongoConnection.MONGO_DB_DB, MongoContainer.DATABASE);
        props.put(MongoConnection.MONGO_DB_USER, MongoContainer.USERNAME);
        props.put(MongoConnection.MONGO_DB_PASSWORD, MongoContainer.PASSWORD);
        props.put(MongoConnection.MONGO_QUEUE_COLLECTION_NAME, COLLECTION_NAME);
        props.put(MongoConnection.MONGO_DB_URL, String.format("%s:%s",
                container.getContainerIpAddress(),
                container.getMappedPort(MongoContainer.ORIGINAL_PORT)
        ));
        return new MongoConnectionParams(props);
    }

}
