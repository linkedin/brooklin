/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * A very trivial implementation of DatastreamEventCollector which
 * simply stores whatever is passed in and returns all history as a
 * list when queried.
 */
public class ListBackedTransportProvider implements TransportProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ListBackedTransportProvider.class);
  /**
   * Each ProducerRecord holds a list of events of to the same transaction.
   * This is a list of the event lists of all transactions.
   */
  private final List<List<BrooklinEnvelope>> _allEvents = new ArrayList<>();
  private final List<DatastreamProducerRecord> _allRecords = new ArrayList<>();

  @Override
  public synchronized void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
    LOG.info("send called on destination {} with {} events", destination, record.getEvents().size());
    _allEvents.add(record.getEvents());
    _allRecords.add(record);
  }

  @Override
  public void close() {

  }

  @Override
  public void flush() {

  }

  public synchronized List<List<BrooklinEnvelope>> getAllEvents() {
    return _allEvents;
  }

  public synchronized List<DatastreamProducerRecord> getAllRecords() {
    return _allRecords;
  }
}
