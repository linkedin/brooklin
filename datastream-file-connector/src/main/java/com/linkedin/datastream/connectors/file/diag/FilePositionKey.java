/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.file.diag;

import java.time.Instant;

import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.jetbrains.annotations.NotNull;

import com.linkedin.datastream.common.JsonUtils.InstantDeserializer;
import com.linkedin.datastream.common.JsonUtils.InstantSerializer;
import com.linkedin.datastream.common.diag.PositionKey;


/**
 * A FilePositionKey uniquely identifies the instantiation of a FileProcessor (in a FileConnector) within a Brooklin
 * cluster.
 *
 * @see com.linkedin.datastream.connectors.file.FileConnector
 */
public class FilePositionKey implements PositionKey {
  private static final long serialVersionUID = 1L;

  /**
   * A String that matches the DatastreamTask's task prefix.
   */
  private final String datastreamTaskPrefix;

  /**
   * A String that uniquely identifies the DatastreamTask which is running the FileProcessor.
   */
  private final String datastreamTaskName;

  /**
   * The time at which consumption started.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private final Instant connectorTaskStartTime;

  /**
   * The file name of the file being consumed.
   */
  private final String fileName;

  /**
   * Constructs a FilePositionKey.
   *
   * @param datastreamTaskPrefix the task prefix for the DatastreamTask running on the FileConnector
   * @param datastreamTaskName a unique identifier for the DatastreamTask running on the FileConnector
   * @param connectorTaskStartTime time time at which consumption started
   * @param fileName the file name of the file being consumed
   */
  public FilePositionKey(
      @JsonProperty("datastreamTaskPrefix") @NotNull final String datastreamTaskPrefix,
      @JsonProperty("datastreamTaskName") @NotNull final String datastreamTaskName,
      @JsonProperty("connectorTaskStartTime") @NotNull final Instant connectorTaskStartTime,
      @JsonProperty("fileName") @NotNull final String fileName) {
    this.datastreamTaskPrefix = datastreamTaskPrefix;
    this.datastreamTaskName = datastreamTaskName;
    this.connectorTaskStartTime = connectorTaskStartTime;
    this.fileName = fileName;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public String getDatastreamTaskPrefix() {
    return datastreamTaskPrefix;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public String getDatastreamTaskName() {
    return datastreamTaskName;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public Instant getConnectorTaskStartTime() {
    return connectorTaskStartTime;
  }

  /**
   * The file name we are reading from.
   * @return the file name
   */
  @NotNull
  public String getFileName() {
    return fileName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilePositionKey that = (FilePositionKey) o;
    return Objects.equals(datastreamTaskPrefix, that.datastreamTaskPrefix)
        && Objects.equals(datastreamTaskName, that.datastreamTaskName)
        && Objects.equals(connectorTaskStartTime, that.connectorTaskStartTime)
        && Objects.equals(fileName, that.fileName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Objects.hash(datastreamTaskPrefix, datastreamTaskName, connectorTaskStartTime, fileName);
  }
}