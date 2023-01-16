package org.oaksoft;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
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
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "group-instance-id-" + args[0]);
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofMinutes(4).toMillis()));
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final Thread mainThread = Thread.currentThread();

        final var kafkaConsumer = new KafkaConsumer<String, String>(properties);

        final var listener = new ConsumerRebalanceListenerImpl(kafkaConsumer);

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
            kafkaConsumer.subscribe(Collections.singletonList("demo_java"), listener);

            while (!interrupted.get()) {
                logger.debug("Polling...");

                var records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key {}, Value {}", record.key(), record.value());
                    logger.info("Partition {}, Offset {}", record.partition(), record.offset());

                    listener.addOffsetToTrack(record.topic(), record.partition(), record.offset());
                }

                kafkaConsumer.commitAsync();
            }
        } catch (WakeupException ex) {
            logger.info("Wakeup");
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            try {
                kafkaConsumer.commitSync(listener.getCurrentOffsets());
            } catch (KafkaException ex) {
                logger.error(ex.getMessage());
            } finally {
               try {
                   kafkaConsumer.close();
                   logger.info("The consumer is now gracefully closed.");
               } catch (RuntimeException ex) {
                   logger.error(ex.getMessage());
               }
            }
        }
    }
}
