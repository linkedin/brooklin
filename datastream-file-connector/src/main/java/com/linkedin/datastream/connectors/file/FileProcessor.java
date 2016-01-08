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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamEventRecord;
import com.linkedin.datastream.server.DatastreamTask;


class FileProcessor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FileConnector.class);

  private static final int PARTITION = 0;
  private static final int POLL_WAIT_MS = 100;

  private final DatastreamTask _task;
  private final String _fileName;
  private BufferedReader _fileReader;
  private final DatastreamEventProducer _producer;
  private final boolean _checkpointing;
  private boolean _cancelRequested;
  private boolean _isStopped;

  public FileProcessor(DatastreamTask datastreamTask, DatastreamEventProducer producer, boolean checkpointing) throws FileNotFoundException {
    _task = datastreamTask;
    _fileName = datastreamTask.getDatastreamSource().getConnectionString();
    _fileReader = new BufferedReader(new InputStreamReader(new FileInputStream(_fileName)));
    _producer = producer;
    _checkpointing = checkpointing;
    _isStopped = false;
    _cancelRequested = false;
    LOG.info("Created FileProcessor for " + datastreamTask);
  }

  private int loadCheckpoint() throws IOException {
    int lineNo = 0;
    Map<Integer, String> savedCheckpoints = _task.getCheckpoints();
    String cpString = savedCheckpoints.getOrDefault(PARTITION, null);
    if (cpString != null && !cpString.isEmpty()) {
      // Resume from last saved line number
      lineNo = Integer.valueOf(cpString);

      // Must skip line by line as we can't get the actual file position of the
      // last newline character due to the buffering done by BufferedReader.
      for (int i = 0; i < lineNo; i++) {
        _fileReader.readLine();
      }

      LOG.info("Resumed from line " + lineNo);
    } else {
      LOG.info("Resumed from beginning");
    }

    return lineNo;
  }

  @Override
  public void run() {
    try {
      Integer lineNo = _checkpointing ? loadCheckpoint() : 0;
      while (!_cancelRequested) {
        String text;
        try {
          text = _fileReader.readLine();
        } catch (IOException e) {
          throw new RuntimeException("Reading file failed.", e);
        }
        if (text != null) {
          DatastreamEvent event = new DatastreamEvent();
          event.payload = ByteBuffer.wrap(text.getBytes());
          event.key = ByteBuffer.allocate(0);
          event.metadata = new HashMap<>();
          event.previous_payload = ByteBuffer.allocate(0);
          LOG.info("sending event " + text);
          _producer.send(new DatastreamEventRecord(event, 0, lineNo.toString()));
          LOG.info("Sending event succeeded");
          ++lineNo;
        } else {
          try {
            // Wait for new data
            Thread.sleep(POLL_WAIT_MS);
          } catch (InterruptedException e) {
            _isStopped = true;
            LOG.info("Stopped at line " + lineNo);
          }
        }
      }
      
      _isStopped = true;
      LOG.info("Stopped at line " + lineNo);
    } catch (Throwable e) {
      LOG.error("File processor is quitting with exception ", e);
    }
  }

  public boolean isStopped() {
    return _isStopped;
  }

  public void stop() {
    _cancelRequested = true;
  }
}
