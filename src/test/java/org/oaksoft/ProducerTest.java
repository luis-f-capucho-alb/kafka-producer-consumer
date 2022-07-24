package org.oaksoft;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ProducerTest {

    @Container
    private final KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"));

    @Test
    void sendRecord() {
        final var message = "Hello, World";
        Producer.sendRecord(container.getBootstrapServers(), "demo_java", message);
    }
}