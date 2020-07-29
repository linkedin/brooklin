/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

/**
 * implementation for {@link CustomCheckpointProvider}
 * uses kafka topic as the datastore
 */
public class KafkaCustomCheckpointProvider implements CustomCheckpointProvider<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCustomCheckpointProvider.class);

    private final String _taskId;
    private final String _topic;
    private final Consumer<String, Long> _consumer;
    private final Producer<String, Long> _producer;
    private final TopicPartition _topicPartition;

    private Long _checkpoint;
    private Long _previousCommittedCheckpoint;

    /**
     * Constructor for KafkaCustomCheckpointProvider
     * @param taskId id that uniquely identifies a task
     * @param bootstrapServers the checkpoint datastore
     * @param topic the checkpoint store topic
     */
    public KafkaCustomCheckpointProvider(String taskId,
                                         String bootstrapServers,
                                         String topic) {
        this._taskId = taskId;
        this._topic = topic;

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        this._consumer = new KafkaConsumer<>(consumerProperties);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 5);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        this._producer = new KafkaProducer<>(producerProperties);


        Properties adminProperties = new Properties();
        adminProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put("cleanup.policy", "compact");
            topicConfig.put("delete.retention.ms", "3600000");
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1).configs(topicConfig);

            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.values().get(topic).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }

        this._consumer.assign(singletonList(new TopicPartition(topic, 0)));

        this._consumer.poll(Duration.ZERO); // poll the first time

        this._topicPartition = new TopicPartition(_topic, 0);

        this._checkpoint = null;

        this._previousCommittedCheckpoint = Long.MIN_VALUE;
    }

    @Override
    public void updateCheckpoint(Long checkpoint) {
        _checkpoint = checkpoint;
    }

    @Override
    public void rewindTo(Long checkpoint) {
        _checkpoint = checkpoint;
        commit(checkpoint);
    }

    @Override
    public void commit(Long checkpoint) {
        if (!_previousCommittedCheckpoint.equals(checkpoint)) {
            LOG.info("Commit call for task {} with checkpoint {}", _taskId, checkpoint);
            try {
                _producer.send(new ProducerRecord<>(_topic, _taskId, checkpoint)).get();
                _producer.flush();
                _previousCommittedCheckpoint = checkpoint;
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        _consumer.close();
        _producer.close();
    }

    @Override
    public Long getSafeCheckpoint() {
        _checkpoint = (_checkpoint == null) ? getCommitted() : _checkpoint;
        return _checkpoint;
    }

    @Override
    public Long getCommitted() {
        Long checkpoint = null;
        long endOffset = -1;

        Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(Collections.singletonList(_topicPartition));
        for (Long offset : endOffsets.values()) {
            endOffset = offset;
        }

        _consumer.seekToBeginning(Collections.singletonList(new TopicPartition(_topic, 0)));
        ConsumerRecords<String, Long> records = _consumer.poll(Duration.ofMillis(30));

        long currentOffset = -1;
        while (currentOffset < endOffset - 1) {
            for (ConsumerRecord<String, Long> record : records) {
                if (record.key().equals(_taskId)) {
                    checkpoint = record.value();
                }
                currentOffset = record.offset();
            }
            records = _consumer.poll(Duration.ofMillis(30));
        }
        return checkpoint;
    }
}

