# QueueBox - async queue engine based on MongoDB

Async java message queue using MongoDB as a backend.    

This fork use the latest MongoDB version with the latest Java Driver and contains wrapper that hide MongoDB driver API and allow using plain java objects in queue.

Unit tests based on real MongoDB with testcontainers.

## issues and feature requests

 * security issues - [hackerone](https://hackerone.com/c0f3-labs)
 * other issues and features - [github issues](https://github.com/c0f3/mongo-queue-java/issues)

## Features

 * totally async and non-blocking multithreading (maybe used in clustered software)
 * Message selection and/or count via MongoDB query
 * Distributes across machines via MongoDB
 * Message priority
 * Delayed messages
 * Message routing
 * Running message timeout and redeliver
 * Atomic acknowledge and send together
 * Easy index creation based only on payload
 * work with the latest MongoDB (4.2)
 * you can use any other storage system by implementing interface
 
## Jar

To add the library as a jar simply [Build](#project-build) the project and use the `queue-box-0.0.1.jar` from the created
`target` directory!

## Maven

To add the library as a local, per-project dependency use [Maven](http://maven.apache.org)! Simply add a dependency on
to your project's `pom.xml` file such as:

```xml

<dependency>
	<groupId>net.c0f3.labs</groupId>
	<artifactId>queue-box</artifactId>
	<version>0.1.4</version>
</dependency>

```

## Usage example

 * starting QueueBox instance
 * creating listener for specific "destination"
 * creating and sending some simple message presented as POJO

```java
public final class Main {
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

        Future future = queueBox.queue(pojo);

        while (!future.isDone()) {
            Thread.sleep(5);
        }

        System.out.println("send packet: "+pojo);

    }
}
```

Also you can use just core library without wrapper, as it described in [original README](https://github.com/gaillard/mongo-queue-java).

```java
public final class Main {

    public static void main(final String[] args) throws UnknownHostException {
        final Queue queue = new Queue(new MongoClient().getDB("testing").getCollection("messages"));
        queue.send(new BasicDBObject());
        final BasicDBObject message = queue.get(new BasicDBObject(), 60);
        queue.ack(message);
    }
}
```

## Documentation

Found in the [source](/src/main/java/gaillard/mongo/MongoQueueCore.java) itself, take a look!

## Project Build

For testing install docker.

```bash
mvn clean install
```

## We must know our heroes!

This version based on the original version authored by [Gaillard](https://github.com/gaillard) from [here](https://github.com/gaillard/mongo-queue-java) and impoved by [Uromahn](https://github.com/uromahn/mongo-queue-java)
