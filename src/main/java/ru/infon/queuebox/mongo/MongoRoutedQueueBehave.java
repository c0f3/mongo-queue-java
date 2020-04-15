package ru.infon.queuebox.mongo;

import com.mongodb.client.MongoCollection;
import gaillard.mongo.MongoQueueCore;
import org.bson.Document;
import ru.infon.queuebox.*;
import ru.infon.queuebox.common.PropertiesBox;

import java.util.*;

/**
 * 06.06.2017
 *
 * @author KostaPC
 * 2017 Infon ZED
 **/
public class MongoRoutedQueueBehave<T extends RoutedMessage> implements QueueBehave<T> {

    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_DESTINATION = "destination";
    private static final String FIELD_ID = "id";

    public static final String PROPERTY_FETCH_LIMIT = "queue.fetch.limit";
    public static final String PROPERTY_RESET_TIMEOUT = "queue.message.timeout";

    private static final int DEFAULT_FETCH_LIMIT = 100;
    private static final int DEFAULT_RESET_TIMEOUT_SEC = 5 * 60;
    private static final int RETRY_IMMEDIATELY = 0;
    private static final int WAIT_FOR_RETRY_DEFAULT_MILLS = 100;

    private final QueueSerializer<T> serializer;
    private final MongoQueueCore mongoQueueCore;

    private int fetchLimit = DEFAULT_FETCH_LIMIT;
    private int resetTimeout = DEFAULT_RESET_TIMEOUT_SEC;

    public MongoRoutedQueueBehave(MongoCollection<Document> collection, PropertiesBox properties, Class<T> packetClass) {
        this.serializer = new MongoJacksonSerializer<>(packetClass);
        this.mongoQueueCore = new MongoQueueCore(collection);
        Document indexDocument = new Document();
        indexDocument.append(FIELD_DESTINATION, 1);
        mongoQueueCore.ensureGetIndex(indexDocument);
        this.fetchLimit = properties.tryGetIntProperty(PROPERTY_FETCH_LIMIT, DEFAULT_FETCH_LIMIT);
        this.resetTimeout = properties.tryGetIntProperty(PROPERTY_RESET_TIMEOUT, DEFAULT_RESET_TIMEOUT_SEC);
    }

    @Override
    public int getFetchLimit() {
        return this.fetchLimit;
    }

    @Override
    public void put(MessageContainer<T> event) {
        T message = event.getMessage();
        Document queueMessage = serializer.serialize(message);
        queueMessage.append(FIELD_SOURCE, message.getSource());
        queueMessage.append(FIELD_DESTINATION, message.getDestination());
        this.mongoQueueCore.send(
                serializer.serialize(event.getMessage()),
                new Date(),
                event.getPriority()
        );
    }

    @Override
    public Collection<MessageContainer<T>> find(QueueConsumer<T> consumer) {
        Document query = new Document();
        query.append(FIELD_DESTINATION, consumer.getConsumerId());
        List<MessageContainer<T>> resultList = new LinkedList<>();
        int limit = fetchLimit;
        while (limit-- > 0) {
            Document queueMessage = mongoQueueCore.get(
                    query,
                    resetTimeout, WAIT_FOR_RETRY_DEFAULT_MILLS, RETRY_IMMEDIATELY
            );
            if (queueMessage == null) {
                break;
            }
            Object id = queueMessage.get(FIELD_ID);
            queueMessage.remove(FIELD_ID);
            String destination = queueMessage.getString(FIELD_DESTINATION);
            String source = queueMessage.getString(FIELD_SOURCE);
            queueMessage.remove(FIELD_DESTINATION);
            queueMessage.remove(FIELD_SOURCE);
            T message = serializer.deserialize(queueMessage);
            message.setSource(source);
            message.setDestination(destination);
            MessageContainer<T> messageContainer = new MessageContainer<>(message);
            messageContainer.setId(id);
            resultList.add(messageContainer);
        }
        return resultList;
    }

    @Override
    public void remove(MessageContainer<T> packet) {
        Document query = new Document();
        query.append(FIELD_ID, packet.getId());
        mongoQueueCore.ack(query);
    }

    @Override
    public void reset(MessageContainer<T> event) {
        T message = event.getMessage();
        Document queueMessage = serializer.serialize(message);
        queueMessage.append(FIELD_SOURCE, message.getSource());
        queueMessage.append(FIELD_DESTINATION, message.getDestination());
        queueMessage.append(FIELD_ID, event.getId());
        mongoQueueCore.requeue(queueMessage);
    }
}
