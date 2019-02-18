/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.server.api.transport.SendCallback;
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
  private List<List<BrooklinEnvelope>> _allEvents = new ArrayList<>();
  private List<DatastreamProducerRecord> _allRecords = new ArrayList<>();

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
