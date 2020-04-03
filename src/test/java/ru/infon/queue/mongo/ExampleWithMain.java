package ru.infon.queue.mongo;

import ru.infon.queuebox.MessageContainer;
import ru.infon.queuebox.QueueConsumer;
import ru.infon.queuebox.mongo.MongoRoutedQueueBox;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 21.06.2017
 * @author KostaPC
 * 2017 Infon ZED
 **/
public class ExampleWithMain {

    public static void main(String[] args) throws InterruptedException, IOException {
        final String defaultSource = "just_source";
        final String defaultDestination = "just_destination";

        Properties properties = new Properties();
        properties.load(ExampleWithMain.class.getResourceAsStream("mongodb.properties"));
        MongoRoutedQueueBox<JustPojoRouted> queueBox = new MongoRoutedQueueBox<>(
                properties,
                JustPojoRouted.class
        );
        queueBox.start(); // init internal thread pool ant begin periodic query to db

        final JustPojoRouted pojo = new JustPojoRouted(13, "string message for 13");
        pojo.setSource(defaultSource);
        pojo.setDestination(defaultDestination);

        queueBox.subscribe(new QueueConsumer<JustPojoRouted>() {
            @Override
            public void onPacket(MessageContainer<JustPojoRouted> message) {
                JustPojoRouted recvPojo = message.getMessage();
                System.out.println("received packet:"+recvPojo);
                message.done(); // accepting message
            }

            @Override
            public String getConsumerId() {
                return defaultDestination; // destinations that this consumer accepts
            }
        });

        Future<JustPojoRouted> future = queueBox.queue(pojo);

        while (!future.isDone()) {
            Thread.sleep(5);
        }

        System.out.println("send packet: "+pojo);

    }
}
