package net.c0f3.queuebox.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Logger;

public class MongoQueueCoreIndexes<T> {

    public static final int INDEX_CREATION_ATTEMPTS = 5;

    private static final Logger LOGGER = Logger.getLogger(MongoQueueCoreIndexes.class.getCanonicalName());

    private final MongoCollection<T> collection;

    public MongoQueueCoreIndexes(MongoCollection<T> mongoCollection) {
        this.collection = mongoCollection;
    }

    private String generateIndexName() {
        return "queuebox-index-" + UUID.randomUUID().toString();
    }

    /**
     * Ensure index for get() method with no fields before or after sort fields
     */
    public void ensureGetIndex() {
        ensureGetIndex(new Document());
    }

    /**
     * Ensure index for get() method with no fields after sort fields
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     */
    public void ensureGetIndex(final Document beforeSort) {
        ensureGetIndex(beforeSort, new Document());
    }

    /**
     * Ensure index for get() method
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     * @param afterSort  fields in get() call that should be after the sort fields in the index. Should not be null
     */
    public void ensureGetIndex(final Document beforeSort, final Document afterSort) {
        Objects.requireNonNull(beforeSort);
        Objects.requireNonNull(afterSort);

        //using general rule: equality, sort, range or more equality tests in that order for index
        final Document completeIndex = new Document("running", 1);

        for (final Map.Entry<String, Object> field : beforeSort.entrySet()) {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }

            completeIndex.append("payload." + field.getKey(), field.getValue());
        }

        completeIndex.append("priority", 1).append("created", 1);

        for (final Map.Entry<String, Object> field : afterSort.entrySet()) {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }

            completeIndex.append("payload." + field.getKey(), field.getValue());
        }

        completeIndex.append("earliestGet", 1);

        ensureIndex(completeIndex);//main query in Get()
        ensureIndex(new Document("running", 1).append("resetTimestamp", 1));//for the stuck messages query in Get()
    }

    /**
     * Ensure index for count() method
     *
     * @param index          fields in count() call. Should not be null
     * @param includeRunning whether running was given to count() or not
     */
    public void ensureCountIndex(final Document index, final boolean includeRunning) {
        Objects.requireNonNull(index);

        final Document completeIndex = new Document();

        if (includeRunning) {
            completeIndex.append("running", 1);
        }

        for (final Map.Entry<String, Object> field : index.entrySet()) {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }

            completeIndex.append("payload." + field.getKey(), field.getValue());
        }

        ensureIndex(completeIndex);
    }

    private void ensureIndex(final Document indexDoc) {
        for (int i = 0; i < INDEX_CREATION_ATTEMPTS; ++i) {
            for (final Document existingIndex : collection.listIndexes()) {
                if (existingIndex.get("key").equals(indexDoc)) {
                    LOGGER.info(String.format(
                            "found just created index %s",
                            existingIndex.get("name")
                    ));
                    return;
                }
            }
            String name = generateIndexName();
            LOGGER.info(String.format(
                    "created index %s at %s attempt",
                    name, i + 1
            ));
            IndexOptions iOpts = new IndexOptions().background(true).name(name);
            collection.createIndex(indexDoc, iOpts);
        }

        throw new RuntimeException("could not create index after 5 attempts");
    }
}
