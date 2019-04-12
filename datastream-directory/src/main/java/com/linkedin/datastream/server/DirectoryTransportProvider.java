/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;

import static com.linkedin.datastream.connectors.directory.DirectoryChangeProcessor.DirectoryEvent;


/**
 * A {@link TransportProvider} implementation that responds to change events
 * from a source directory in the file system by reflecting them to a destination
 * directory to keep it in sync.
 *
 * However, this does not cover copying the initial contents of the source directory
 * or wiping the initial contents of the destination directory.
 * <br/>
 * <strong>This transport provider is for demonstration purposes only.</strong>
 */
public class DirectoryTransportProvider implements TransportProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryTransportProvider.class);

  @Override
  public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
    Validate.notEmpty(destination);
    Validate.notNull(record);

    Path destinationPath = Paths.get(destination);
    Validate.isTrue(destinationPath.isAbsolute(), "Destination path must be absolute");

    for (BrooklinEnvelope envelope : record.getEvents()) {
      Path sourcePath = (Path) envelope.key().get();
      DirectoryEvent changeEvent = (DirectoryEvent) envelope.value().get();
      LOG.info("Received change event {} in path {}", changeEvent, sourcePath);

      switch (changeEvent) {
        case ENTRY_CREATED:
          copyPathToDir(sourcePath, destinationPath);
          break;
        case ENTRY_MODIFIED:
          deleteSubPath(destinationPath, sourcePath.getFileName());
          copyPathToDir(sourcePath, destinationPath);
          break;
        case ENTRY_DELETED:
          deleteSubPath(destinationPath, sourcePath.getFileName());
          break;
        default:
          throw new IllegalArgumentException(String.format("Unrecognized DirectoryEvent: %s", changeEvent));
      }
    }

    // Deliberately not invoking onComplete because there is no progress info to checkpoint
  }

  private static void copyPathToDir(Path sourcePath, Path destinationDir) {
    ThrowingBinFunction<File, File> copyFn = Files.isDirectory(sourcePath) ?
        FileUtils::copyDirectoryToDirectory :
        FileUtils::copyFileToDirectory;

    try {
      copyFn.apply(sourcePath.toFile(), destinationDir.toFile());
      LOG.info("Successfully copied {} to {}", sourcePath, destinationDir);
    } catch (IOException e) {
      LOG.error("Encountered an error while copying {} to {}: ", sourcePath, destinationDir, e.getMessage());
    }
  }

  private void deleteSubPath(Path parentPath, Path subPath) {
    Path deletePath = parentPath.resolve(subPath);
    if (FileUtils.deleteQuietly(deletePath.toFile())) {
      LOG.info("Successfully deleted {}", deletePath);
    } else {
      LOG.warn("Path '{}' did not exist in destination directory", deletePath);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void flush() {
  }

  @FunctionalInterface
  private interface ThrowingBinFunction<T, U> {
    void apply(T t, U u) throws IOException;
  }
}
