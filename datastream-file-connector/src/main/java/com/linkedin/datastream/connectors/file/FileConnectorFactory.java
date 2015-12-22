package com.linkedin.datastream.connectors.file;

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

import java.util.Properties;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class FileConnectorFactory implements ConnectorFactory {
  @Override
  public Connector createConnector(Properties config) {
    try {
      return new FileConnector(config);
    } catch (DatastreamException e) {
      throw new RuntimeException("File connector instantiation failed with error", e);
    }
  }
}
