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
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;


class FileProcessor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FileConnector.class);

  private static final int PARTITION = 0;
  private static final int POLL_WAIT_MS = 100;

  private final DatastreamTask _task;
  private final String _fileName;
  private BufferedReader _fileReader;
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

    return lineNo + 1;
  }

  @Override
  public void run() {
    try {
      _task.acquire();

      Integer lineNo = loadCheckpoint();
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

          // Using the line# as the key
          event.key = ByteBuffer.wrap(lineNo.toString().getBytes());
          event.metadata = new HashMap<>();
          //Registering a null schema just for testing using the MockSchemaRegistryProvider
          event.metadata.put("PayloadSchemaId", _producer.registerSchema(null, null));
          event.previous_payload = ByteBuffer.allocate(0);
          LOG.info("sending event " + text);
          DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
          builder.addEvent(event);

          // If the destination is user managed, we will use the key to decide the partition.
          if (!_task.isUserManagedDestination()) {
            builder.setPartition(0);
          }

          builder.setSourceCheckpoint(lineNo.toString());
          _producer.send(builder.build(),
              (metadata, exception) -> {
                if(exception == null) {
                  LOG.info(String.format("Sending event:{%s} succeeded, metadata:{%s}", text, metadata));
                } else {
                  LOG.error(String.format("Sending event:{%s} failed, metadata:{%s}", text, metadata), exception);
                }
              });
          ++lineNo;
        } else {
          try {
            // Wait for new data
            Thread.sleep(POLL_WAIT_MS);
          } catch (InterruptedException e) {
            LOG.info("Interrupted");
            break;
          }
        }
      }

      _task.release();
      _isStopped = true;
      LOG.info("Stopped at line " + lineNo + " task=" + _task);
    } catch (Throwable e) {
      LOG.error("File processor is quitting with exception, task=" + _task, e);
    }
  }

  public boolean isStopped() {
    return _isStopped;
  }

  public void stop() {
    _cancelRequested = true;
  }
}
