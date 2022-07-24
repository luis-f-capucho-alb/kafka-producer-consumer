package org.oaksoft;

import org.apache.kafka.clients.producer.Callback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;

@Testcontainers
class ProducerTest {

    private final Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    @Container
    private final KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));

    private Producer producer;

    @BeforeEach
    void setUp() {
        producer = new Producer("localhost:9092");
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void sendRecord() {
        final var message = "Hello, World";
        producer.sendRecord("demo_java", message);
    }

    @Test
    void sendRecordWithCallback() {
        final var message = "Hello, World";
        AtomicBoolean conditionMet = new AtomicBoolean(false);
        producer.sendRecord("demo_java", message, (recordMetadata, e) -> {
            if (e != null) {
                fail(e);
            } else {
                conditionMet.set(true);
            }
        });

        await("sendRecordWithCallback").atMost(10000, TimeUnit.MILLISECONDS).untilTrue(conditionMet);
    }

    @Test
    void sendBatchRecords() {
        final var message = "Hello, World";
        AtomicLong conditionMet = new AtomicLong(0);

        for (int i = 0; i < 10; i++) {
            producer.sendRecord("demo_java", message, (recordMetadata, e) -> {
                if (e != null) {
                    fail(e);
                } else {
                    logger.info("Partition: {}", recordMetadata.partition());
                    conditionMet.incrementAndGet();
                }
            });
        }

        await("sendBatchRecords").atMost(15000, TimeUnit.MILLISECONDS).untilAtomic(conditionMet, equalTo(10L));
    }

    @Test
    void sendRecordWithKey() {
        final var key = "key1";
        final var message = "Hello, World";
        AtomicReference<List<Integer>> reference = new AtomicReference<>(new ArrayList<>());

        Callback callback = (recordMetadata, e) -> {
            if (e != null) {
                fail(e);
            } else {
                logger.info("Partition: {}", recordMetadata.partition());
                reference.get().add(recordMetadata.partition());
            }
        };

        producer.sendRecord("demo_java", key, message, callback);
        producer.sendRecord("demo_java", key, message + " 2", callback);

        await("sendRecordWithKey").atMost(15000, TimeUnit.MILLISECONDS).until(() -> reference.get().size() == 2 && reference.get().stream().distinct().count() == 1);
    }

    @Test
    void sendRecordWithDifferentKey(){
        final var key = "key";
        final var message = "Hello, World";
        AtomicReference<List<Integer>> reference = new AtomicReference<>(new ArrayList<>());

        Callback callback = (recordMetadata, e) -> {
            if (e != null) {
                fail(e);
            } else {
                logger.info("Partition: {}", recordMetadata.partition());
                reference.get().add(recordMetadata.partition());
            }
        };

        producer.sendRecord("demo_java", key + "1234", message, callback);
        producer.sendRecord("demo_java", "atLeast2098", message + " 2", callback);

        await("sendRecordWithKey").atMost(15000, TimeUnit.MILLISECONDS).until(() -> reference.get().size() == 2 && reference.get().stream().distinct().count() == 2);
    }
}
