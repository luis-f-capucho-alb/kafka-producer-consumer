package org.oaksoft.simple;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;

public class Producer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<String, String> kafkaProducer;

    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    public Producer(String brokers) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) throws InterruptedException {
        var producer = new Producer("localhost:9092");
        var i = 0;
        while (true) {
            producer.sendRecord("demo_java", "Hello, World " + (++i));
            Thread.sleep(1500);
        }
    }

    public void sendRecord(String topic, String message) {
        sendRecord(topic, message, null);
    }

    public void sendRecord(String topic, String message, Callback callback) {
        var record = new ProducerRecord<String, String>(topic, message);

        final Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(record, callback);

        executorService.submit(() -> this.acquireRecordMetadata(recordMetadataFuture));
    }

    public void sendRecord(String topic, String key, String message, Callback callback) {
        var record = new ProducerRecord<>(topic, key, message);

        final Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(record, callback);

        executorService.submit(() -> this.acquireRecordMetadata(recordMetadataFuture));
    }

    public void close() {
        if (kafkaProducer != null) {
            // flush and close the producer
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    private void acquireRecordMetadata(Future<RecordMetadata> metadataFuture) {
        try {
            this.printMetadataRecord(metadataFuture.get());
        } catch (ExecutionException | InterruptedException ex) {
            logger.error(ex.getMessage());
        }
    }

    private void printMetadataRecord(RecordMetadata metadata) {
        var partition = metadata.partition();
        var offset = metadata.offset();
        var timestamp = metadata.timestamp();
        logger.info("With time {} the partiton {} has an offset {}", timestamp, partition, offset);
    }
}
