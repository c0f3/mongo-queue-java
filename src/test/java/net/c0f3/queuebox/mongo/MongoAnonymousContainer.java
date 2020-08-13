package net.c0f3.queuebox.mongo;

import org.testcontainers.containers.GenericContainer;

public class MongoAnonymousContainer extends GenericContainer<MongoAnonymousContainer> {

    public static final String DATABASE = "test-db";
    public static final int ORIGINAL_PORT = 27017;

    public MongoAnonymousContainer() {
        super("mongo:4.2");
        addEnv("MONGO_INITDB_DATABASE", DATABASE);
        addExposedPort(ORIGINAL_PORT);
    }
}
