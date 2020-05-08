package net.c0f3.queuebox.mongo;

import org.testcontainers.containers.GenericContainer;

public class MongoContainer extends GenericContainer<MongoContainer> {

    public static final String DATABASE = "admin";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "toor";
    public static final int ORIGINAL_PORT = 27017;

    public MongoContainer() {
        super("mongo:4.2");
        addEnv("MONGO_INITDB_DATABASE", DATABASE);
        addEnv("MONGO_INITDB_ROOT_USERNAME", USERNAME);
        addEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD);
        addExposedPort(ORIGINAL_PORT);
    }
}
