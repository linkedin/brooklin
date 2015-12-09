package com.linkedin.datastream.kafka;

/*
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.AvroUtils;
import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.TransportProvider;
import com.linkedin.datastream.server.DatastreamEventRecord;

import kafka.admin.AdminUtils;


/**
 * This is Kafka Transport provider that writes events to kafka.
 */
public class KafkaTransportProvider implements TransportProvider {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransportProvider.class);

  private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String VAL_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String DEFAULT_REPLICATION_FACTOR = "3";
  private final KafkaProducer<byte[], byte[]> _producer;
  private final String _brokers;
  private final String _zkAddress;
  private static final String DESTINATION_URI_FORMAT = "kafka://%s/%s";
  private final ZkClient _zkClient;

  public KafkaTransportProvider(Properties props) {

    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new RuntimeException("Bootstrap servers are not set");
    }

    _brokers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    _zkAddress = props.getProperty("zookeeper.connect");
    _zkClient = new ZkClient(_zkAddress);

    // Assign mandatory arguments
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SERIALIZER);

    _producer = new KafkaProducer<>(props);
  }

  private ProducerRecord<byte[], byte[]> convertToProducerRecord(DatastreamEventRecord record)
      throws DatastreamException {

    Integer partition = record.getPartition();

    byte[] payload;
    try {
      payload = AvroUtils.encodeAvroSpecificRecord(DatastreamEvent.class, record.getEvent());
    } catch (IOException e) {
      throw new DatastreamException("Failed to encode event in Avro, event=" + record.getEvent(), e);
    }

    if (partition >= 0) {
      return new ProducerRecord<>(record.getDestination(), partition, null, payload);
    } else {
      return new ProducerRecord<>(record.getDestination(), null, payload);
    }
  }

  @Override
  public String createTopic(String topicName, int numberOfPartitions, Properties topicConfig) {
    Validate.notNull(topicName, "topicName should not be null");
    Validate.notNull(topicConfig, "topicConfig should not be null");

    int replicationFactor = Integer.parseInt(topicConfig.getProperty("replicationFactor", DEFAULT_REPLICATION_FACTOR));
    LOG.info(String.format("Creating topic with name %s  partitions %d with properties %s",
        topicConfig, numberOfPartitions, topicConfig));

    try {
      // Create only if it doesn't exist.
      if (!AdminUtils.topicExists(_zkClient, topicName)) {
        AdminUtils.createTopic(_zkClient, topicName, numberOfPartitions, replicationFactor, topicConfig);
      } else {
        LOG.warn(String.format("Topic with name %s already exists", topicName));
      }
    } catch (Throwable e) {
      LOG.error(String.format("Creating topic %s failed with exception %s ", topicName, e));
      throw e;
    }

    return String.format(DESTINATION_URI_FORMAT, _zkAddress, topicName);
  }

  @Override
  public void dropTopic(String destinationUri) {
    Validate.notNull(destinationUri, "destinationuri should not null");
    String topicName = URI.create(destinationUri).getPath();

    try {
      // Delete only if it exist.
      if(AdminUtils.topicExists(_zkClient, topicName)) {
        AdminUtils.deleteTopic(_zkClient, topicName);
      } else {
        LOG.warn(String.format("Trying to delete topic %s that doesn't exist", topicName));
      }
    } catch(Throwable e) {
      LOG.error(String.format("Deleting topic %s failed with exception %s", topicName, e));
      throw e;
    }
  }

  @Override
  public void send(DatastreamEventRecord record) {
    try {
      Validate.notNull(record, "invalid event record.");
      Validate.notNull(record.getEvent(), "invalid datastream event.");
      Validate.notNull(record.getEvent().metadata, "Metadata cannot be null");
      Validate.notNull(record.getEvent().key, "Key cannot be null");
      Validate.notNull(record.getEvent().payload, "Payload cannot be null");
      Validate.notNull(record.getEvent().previous_payload, "Payload cannot be null");

      LOG.info(String
          .format("Sending Datastream event %s to topic %s and partition %d", record.toString(), record.getDestination(),
              record.getPartition()));

      ProducerRecord<byte[], byte[]> outgoing;
      try {
        outgoing = convertToProducerRecord(record);
      } catch (Exception ex) {
        LOG.error("Failed to convert DatastreamEvent to ProducerRecord.", ex);
        // TODO: Error handling
        return;
      }
      _producer.send(outgoing);
    } catch (Exception e) {
      LOG.error(String
          .format("Sending event (%s) to topic %s and Kafka cluster (Metadata brokers) %s failed with exception %s ",
              record.getEvent(), record.getDestination(), _brokers, e));
      throw new RuntimeException(String.format("Send of the datastream record %s failed", record.toString()), e);
    }
  }

  @Override
  public void flush() {
    // TODO current kafka open source released version (0.8.2.2) doesn't have this capability. unreleased 0.9 should have this.
  }
}
