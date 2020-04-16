package ru.infon.queuebox.mongo;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.beanutils.ConvertUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 14.10.2016
 *
 * @author kostapc
 * 2016 Infon
 */
public class MongoConnection {

    public static final String MONGO_DB_URL = "default.mongodb.uri";
    public static final String MONGO_DB_DB = "default.mongodb.database";
    public static final String MONGO_DB_USER = "default.mongodb.user";
    public static final String MONGO_DB_PASSWORD = "default.mongodb.password";
    public static final String MONGO_QUEUE_COLLECTION_NAME = "mongodb.queue.collection";


    private static final Logger LOGGER = Logger.getLogger("javax.cache");

    private static final MongoClientOptions defaultOptions = MongoClientOptions.builder().build();
    private static final Map<String, Method> optionsBuilderMap;

    static {
        optionsBuilderMap = new HashMap<>();
        for (Method method : MongoClientOptions.Builder.class.getMethods()) {
            if (method.getParameterTypes().length != 1) {
                continue;
            }
            int access = method.getModifiers();
            if (!Modifier.isPublic(access) || Modifier.isStatic(access)) {
                continue;
            }

            String optionName = method.getName();
            Class<?> paramType = method.getParameterTypes()[0];
            if (
                    !paramType.equals(Boolean.class) &&
                            !paramType.equals(Boolean.TYPE) &&
                            !paramType.equals(Integer.class) &&
                            !paramType.equals(Integer.TYPE) &&
                            !paramType.equals(String.class)
            ) {
                continue;
            }
            String prefix =
                    (
                            paramType.equals(Boolean.class) ||
                                    paramType.equals(Boolean.TYPE)
                    ) ? "is" : "get";
            String getterName = prefix + optionName.substring(0, 1).toUpperCase() + optionName.substring(1);

            Object defaultValue;
            try {
                Method getter;
                getter = MongoClientOptions.class.getDeclaredMethod(getterName);
                defaultValue = getter.invoke(defaultOptions);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                LOGGER.warning(String.format(
                        "reflection error while checking: %s; with getter: %s",
                        optionName, getterName
                ));
                continue;
            }

            optionsBuilderMap.put(optionName, method);

            String infoMessage = String.format(
                    "MongoOptions param \"%s\" = \"%s\" (default, getter: %s)",
                    optionName, defaultValue, getterName
            );

            LOGGER.info(infoMessage);
        }
    }

    private final AtomicReference<MongoClient> client = new AtomicReference<>();
    private final MongoClientOptions.Builder optionsBuilder;

    private String mongoDBName;
    private String mongoDBUser;
    private String mongoCollectionName;
    private char[] mongoDBPassword;
    private final List<ServerAddress> propertiesAddresses = new ArrayList<>();

    public MongoConnection(Properties properties) {

        optionsBuilder = MongoClientOptions.builder();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (key.startsWith(MONGO_DB_URL)) {
                propertiesAddresses.add(new ServerAddress(value));
            } else if (key.equals(MONGO_DB_DB)) {
                mongoDBName = value;
            } else if (key.equals(MONGO_DB_USER)) {
                mongoDBUser = value;
            } else if (key.startsWith(MONGO_DB_PASSWORD)) {
                mongoDBPassword = value.toCharArray();
            } else if (key.startsWith(MONGO_QUEUE_COLLECTION_NAME)) {
                mongoCollectionName = value;
            } else {
                try {
                    LOGGER.fine(MessageFormat.format("Set \"{0}\" value {1}", key, value));
                    Method method = optionsBuilderMap.get(key);
                    if (method == null) {
                        LOGGER.warning(String.format(
                                "MongoClientOptions parameter %s => %s not found in configuration class; skipping...",
                                key, value
                        ));
                        continue;
                    }
                    Object methodParam = ConvertUtils.convert(value, method.getParameterTypes()[0]);
                    method.invoke(optionsBuilder, methodParam);
                    LOGGER.info(String.format(
                            "MongoClientOptions parameter set: %s => %s",
                            key, value
                    ));
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, e.getMessage(), e);
                }
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
        if (mongoDBUser == null || mongoDBPassword == null) {
            return new MongoClient(propertiesAddresses, optionsBuilder.build());
        } else {
            MongoCredential credential = MongoCredential.createCredential(
                    mongoDBUser, mongoDBName, mongoDBPassword
            );
            return new MongoClient(
                    propertiesAddresses,
                    credential, optionsBuilder.build()
            );
        }
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

    public MongoClient getMongoClient() {
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
