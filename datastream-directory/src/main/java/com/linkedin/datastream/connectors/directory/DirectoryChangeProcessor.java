/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.directory;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.nio.file.SensitivityWatchEventModifier;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;


/**
 * Encapsulates the logic for watching for change events in a source directory
 * in the file system, and propagating them to a {@link DatastreamEventProducer}.
 */
public class DirectoryChangeProcessor implements Runnable, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryChangeProcessor.class);
  private static final Duration ACQUIRE_TIMEOUT = Duration.ofMinutes(5);

  private final DatastreamTask _task;
  private final DatastreamEventProducer _producer;
  private final Path _dirPath;
  private final WatchService _watchService;
  private final WatchKey _watchKey;

  /**
   * Directory change event types
   */
  public enum DirectoryEvent {
    /**
     * A file or directory is created
     */
    ENTRY_CREATED,

    /**
     * A file or directory is modified
     */
    ENTRY_MODIFIED,

    /**
     * A file or directory is deleted
     */
    ENTRY_DELETED
  }

  /**
   * Constructor for DirectoryChangeProcessor
   * @param datastreamTask The datastream task this processor is responsible for
   * @param producer The event producer this connector uses to send change events
   *                 to the underlying {@link com.linkedin.datastream.server.api.transport.TransportProvider}.
   * @throws IOException if an I/O error occurs
   */
  public DirectoryChangeProcessor(DatastreamTask datastreamTask, DatastreamEventProducer producer) throws IOException {
    Validate.notNull(datastreamTask);
    Validate.notNull(producer);

    final String path = datastreamTask.getDatastreamSource().getConnectionString();
    Validate.isTrue(isDirectory(path), "path does not refer to a valid directory");

    _task = datastreamTask;
    _producer = producer;
    _dirPath = Paths.get(path);
    _watchService = FileSystems.getDefault().newWatchService();
    _watchKey = _dirPath.register(_watchService, new WatchEvent.Kind<?>[] {ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE},
        SensitivityWatchEventModifier.HIGH);
  }

  /**
   * Tests whether a path is a directory.
   * @param path the path to test
   * @return  {@code true} if the file is a directory; {@code false} if
   *          the file does not exist, is not a directory, or it cannot
   *          be determined if the file is a directory or not.
   */
  public static boolean isDirectory(String path) {
    Validate.notEmpty(path);
    return Files.isDirectory(Paths.get(path));
  }

  @Override
  public void run() {
    try {
      _task.acquire(ACQUIRE_TIMEOUT);

      boolean isWatchKeyValid = true;
      for (WatchKey key = pollWatchService(); isWatchKeyValid; key = pollWatchService()) {
        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();

          /*
           * We may get an OVERFLOW event even though we have not registered for it.
           * https://docs.oracle.com/javase/tutorial/essential/io/notification.html#register
           */
          if (kind != OVERFLOW) {
            @SuppressWarnings("unchecked")
            Path filename = ((WatchEvent<Path>) event).context();
            Path absolutePath = _dirPath.resolve(filename).toAbsolutePath();

            BrooklinEnvelope envelope = new BrooklinEnvelope(absolutePath, getCorrespondingDirectoryEvent(kind), null,
                Collections.emptyMap());

            DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
            builder.addEvent(envelope);
            builder.setEventsSourceTimestamp(System.currentTimeMillis());
            builder.setPartition(0);

           _producer.send(builder.build(), ((metadata, exception) -> {
              if (exception == null) {
                LOG.info("Sending event succeeded");
              } else {
                LOG.error("Sending event failed", exception);
              }
            }));
          }
        }
        isWatchKeyValid = key.reset();
      }

      LOG.warn("Watch key no longer valid. Path {} might have been altered or removed.", _dirPath);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ClosedWatchServiceException e) {
      LOG.info("WatchService closed");
    } finally {
      _task.release();
    }
  }

  @Override
  public void close() {
    _watchKey.cancel();
    try {
      _watchService.close();
    } catch (IOException e) {
      LOG.error(String.format("Encountered an error during closing watch service for path %s", _dirPath), e);
    }
  }

  private WatchKey pollWatchService() throws InterruptedException {
    return _watchService.poll(1, TimeUnit.MINUTES);
  }

  private static DirectoryEvent getCorrespondingDirectoryEvent(WatchEvent.Kind<?> kind) {
    if (kind == ENTRY_CREATE) {
      return DirectoryEvent.ENTRY_CREATED;
    }

    if (kind == ENTRY_MODIFY) {
      return DirectoryEvent.ENTRY_MODIFIED;
    }

    if (kind == ENTRY_DELETE) {
      return DirectoryEvent.ENTRY_DELETED;
    }

    throw new IllegalArgumentException(String.format("Unsupported event kind %s", kind));
  }
}
