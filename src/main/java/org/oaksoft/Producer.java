package org.oaksoft;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<String, String> kafkaProducer;

    public Producer(String brokers) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void sendRecord(String topic, String message) {
        sendRecord(topic, message, null);
    }

    public void sendRecord(String topic, String message, Callback callback) {
        var record = new ProducerRecord<String, String>(topic, message);

        kafkaProducer.send(record, callback);

        logger.info("Record sent!");
    }

    public void sendRecord(String topic, String key, String message, Callback callback) {
        var record = new ProducerRecord<>(topic, key, message);

        kafkaProducer.send(record, callback);

        logger.info("Record sent!");
    }

    public void close() {
        if (kafkaProducer != null) {
            // flush and close the producer
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
