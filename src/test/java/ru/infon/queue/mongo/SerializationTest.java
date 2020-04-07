package ru.infon.queue.mongo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.infon.queuebox.mongo.MongoJacksonSerializer;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 27.03.2017
 *
 * @author KostaPC
 * Copyright (c) 2017 Infon. All rights reserved.
 */
public class SerializationTest {

    @Test
    public void testDefaultSerializer() {
        JustPojo pojo = new JustPojo(13, "_1_3_!");
        MongoJacksonSerializer<JustPojo> serializer = new MongoJacksonSerializer<>(JustPojo.class);
        Document bsonDocument = serializer.serialize(pojo);

        System.out.println(bsonDocument);

        JustPojo processedPojo = serializer.deserialize(bsonDocument);

        System.out.println("processed \n:" + processedPojo);

        assertEquals(pojo, processedPojo);
    }

    private double testConvert(int iterations) {
        long testTime = 0;
        IncrementalAverage mean = new IncrementalAverage();

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(JustPojo.class);

        for (int i = 0; i < iterations; i++) {
            long time = System.nanoTime();
            JustPojo pojo = new JustPojo(i, "_" + i + "_!");
            JsonNode node = mapper.valueToTree(pojo);
            Document document = mapper.convertValue(node, Document.class);
            JustPojo result = mapper.convertValue(document, JustPojo.class);
            testTime = System.nanoTime() - time;
            mean.increment(testTime);
            assertEquals(pojo, result);
        }

        return mean.getResult();
    }

    private double testReparse(int iterations) {
        long testTime = 0;
        IncrementalAverage mean = new IncrementalAverage();

        ObjectMapper mapper = new ObjectMapper();

        for (int i = 0; i < iterations; i++) {
            long time = System.nanoTime();
            JustPojo pojo = new JustPojo(i, "_" + i + "_!");
            JsonNode node = mapper.valueToTree(pojo);
            Document document = Document.parse(node.toString());
            JustPojo result;
            try {
                result = mapper.readValue(document.toJson(), JustPojo.class);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            testTime = System.nanoTime() - time;
            mean.increment(testTime);
            assertEquals(pojo, result);
        }

        return mean.getResult();
    }

    @Test
    public void conversionSpeedTest() throws IOException {

        int iterations = 100000;
        long warmupTime = 20000;

        if (warmupTime > 0) {
            System.out.println("with warmup");
        } else {
            System.out.println("without warmup");
        }

        while (warmupTime > 0) {
            long time = System.currentTimeMillis();
            testConvert(iterations);
            testReparse(iterations);
            time = System.currentTimeMillis() - time;
            warmupTime -= time;
        }

        {
            double result = testConvert(iterations);
            System.out.println("avg nano time convert " + result + "( " + Math.round(result / 1000) + " )");
            Assertions.assertTrue(result < 1400);
        }

        {
            double result = testReparse(iterations);
            System.out.println("avg nano time reparse " + result + "( " + Math.round(result / 1000) + " )");
            Assertions.assertTrue(result < 80000);
        }
    }


    static class IncrementalAverage {

        int count = 0;
        double prevAvg = 0;

        void increment(double value) {
            count++;
            prevAvg = getAvg(prevAvg, value, count);
        }

        private double getAvg(double prev_avg, double x, int n) {
            return (prev_avg * n + x) / (n + 1);
        }

        double getResult() {
            return prevAvg;
        }

    }
}
