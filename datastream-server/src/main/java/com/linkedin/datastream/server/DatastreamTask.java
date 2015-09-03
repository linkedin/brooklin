package com.linkedin.datastream.server;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.linkedin.datastream.common.Datastream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DatastreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

  // connector type. Type of the connector to be used for reading the change capture events
  // from the source, e.g. Oracle-Change, Espresso-Change, Oracle-Bootstrap, Espresso-Bootstrap,
  // Mysql-Change etc..
  // All datastreams wrapped in one assignable DatastreamTask must belong to the same connector type.
  private String _connectorType;

  private List<DatastreamChannel> _streams = new ArrayList<>();

  /**
   * Simple class pairing the Datastream with its channel
   */
  public static class DatastreamChannel {
    private final Datastream _datastream;
    private final DatastreamWritableChannel _channel;

    public DatastreamChannel(Datastream stream, DatastreamWritableChannel channel) {
      _datastream = stream;
      _channel = channel;
    }

    public DatastreamWritableChannel getChannel() {
      return _channel;
    }

    public Datastream getDatastream() {
      return _datastream;
    }
  }

  public void addStream(Datastream datastream, DatastreamWritableChannel channel) {
    if (_connectorType == null) {
      _connectorType = datastream.getConnectorType();
    } else if (_connectorType != datastream.getConnectorType()) {
      LOG.error(String.format("Adding %s datastream into % task",
        datastream.getConnectorType(), _connectorType));
      throw new IllegalArgumentException();
    }
      _streams.add(new DatastreamChannel(datastream, channel));
  }

  public String getConnectorType() {
      return _connectorType;
  }

  public List<DatastreamChannel> getDatastreamChannels() {
      return _streams;
  }
}