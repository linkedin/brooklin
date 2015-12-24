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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;

import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamEventRecord;
import com.linkedin.datastream.server.api.transport.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.server.DatastreamTask;


public class FileProcessor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FileConnector.class);

  private final DatastreamTask _task;
  private final String _fileName;
  private final BufferedReader _fileReader;
  private final DatastreamEventProducer _producer;
  private boolean _cancelRequested;
  private boolean _isStopped;

  public FileProcessor(DatastreamTask datastreamTask, DatastreamEventProducer producer) throws FileNotFoundException {
    _task = datastreamTask;
    _fileName = datastreamTask.getDatastreamSource().getConnectionString();
    _fileReader = new BufferedReader(new InputStreamReader(new FileInputStream(_fileName)));
    _producer = producer;
    _isStopped = false;
    _cancelRequested = false;
  }

  @Override
  public void run() {
    try {
      Integer lineNo = 1;
      while (!_cancelRequested) {
        String text;
        try {
          text = _fileReader.readLine();
        } catch (IOException e) {
          throw new RuntimeException("Reading file failed.", e);
        }
        if (text != null) {
          // TODO this is bad, We need better experience to create a datastream event record.
          DatastreamEvent event = new DatastreamEvent();
          event.payload = ByteBuffer.wrap(text.getBytes());
          event.key = ByteBuffer.allocate(0);
          event.metadata = new HashMap<>();
          event.previous_payload = ByteBuffer.allocate(0);
          LOG.info("sending event " + text);
          _producer.send(new DatastreamEventRecord(event, 0, lineNo.toString(), _task));
          LOG.info("Sending event succeeded");
          ++lineNo;
        } else {
          try {
            // If there is no data yet, check every second.
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            _isStopped = true;
            Thread.currentThread().interrupt();
          }
        }
      }
      _isStopped = true;
    } catch (Throwable e) {
      LOG.error("File processor is quitting with exception ", e);
      throw e;
    }

  }

  public boolean isStopped() {
    return _isStopped;
  }

  public void stop() {
    _cancelRequested = true;
  }
}
