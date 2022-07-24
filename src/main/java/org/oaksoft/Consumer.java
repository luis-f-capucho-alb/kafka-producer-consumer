package org.oaksoft;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private static final AtomicBoolean interrupted = new AtomicBoolean(false);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-second_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Thread mainThread = Thread.currentThread();

        final var kafkaConsumer = new KafkaConsumer<String, String>(properties);

        Thread shutdownHook = new Thread(() -> {
            interrupted.set(true);
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            kafkaConsumer.subscribe(Collections.singletonList("demo_java"));

            while (!interrupted.get()) {
                logger.info("Polling...");

                var records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key {}, Value {}", record.key(), record.value());
                    logger.info("Partition {}, Offset {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException ex) {
            logger.info("Wakeup");
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
