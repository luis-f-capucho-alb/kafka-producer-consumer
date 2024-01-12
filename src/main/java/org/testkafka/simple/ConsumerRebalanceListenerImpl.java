package org.testkafka.simple;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

    private final Logger logger = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private final KafkaConsumer<String, String> consumer;

    public ConsumerRebalanceListenerImpl(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void addOffsetToTrack(String topic, int partition, long offset) {
        currentOffsets.put(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1, null));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsRevoked callback triggered : {}", partitions);
        logger.info("Committing offsets: " + currentOffsets);

     //   consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsAssigned callback triggered : {}",partitions);
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }
}
