package org.oaksoft;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        sendRecord("localhost:9092", "demo_java", "Hello, World!");
    }

    public static void sendRecord(String brokers, String topic, String message) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        try (var kafkaProducer = new KafkaProducer<String, String>(properties)) {
            // send data
            var record = new ProducerRecord<String, String>(topic, message);
            kafkaProducer.send(record);

            // flush and close the producer
            kafkaProducer.flush();
        }

        logger.info("Record sent!");
    }
}
