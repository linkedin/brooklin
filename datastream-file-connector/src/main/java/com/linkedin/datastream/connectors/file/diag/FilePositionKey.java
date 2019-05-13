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

  /**
   * The position type.
   */
  private static final String FILE_POSITION_TYPE = "File";

  /**
   * The Brooklin server's cluster instance that is running the FileConnector.
   */
  private final String brooklinInstance;

  /**
   * A String that matches the DatastreamTask's task prefix.
   */
  private final String brooklinTaskPrefix;

  /**
   * A String that uniquely identifies the DatastreamTask which is running the FileProcessor.
   */
  private final String brooklinTaskId;

  /**
   * The time at which consumption started.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  private final Instant taskStartTime;

  /**
   * The file name of the file being consumed.
   */
  private final String fileName;

  /**
   * Constructs a FilePositionKey.
   *
   * @param brooklinInstance the Brooklin server's cluster instance
   * @param brooklinTaskPrefix the task prefix for the DatastreamTask running on the FileConnector
   * @param brooklinTaskId a unique identifier for the DatastreamTask running on the FileConnector
   * @param taskStartTime time time at which consumption started
   * @param fileName the file name of the file being consumed
   */
  public FilePositionKey(
      @JsonProperty("brooklinInstance") @NotNull final String brooklinInstance,
      @JsonProperty("brooklinTaskPrefix") @NotNull final String brooklinTaskPrefix,
      @JsonProperty("brooklinTaskId") @NotNull final String brooklinTaskId,
      @JsonProperty("taskStartTime") @NotNull final Instant taskStartTime,
      @JsonProperty("fileName") @NotNull final String fileName) {
    this.brooklinInstance = brooklinInstance;
    this.brooklinTaskPrefix = brooklinTaskPrefix;
    this.brooklinTaskId = brooklinTaskId;
    this.taskStartTime = taskStartTime;
    this.fileName = fileName;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public String getType() {
    return FILE_POSITION_TYPE;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public String getBrooklinInstance() {
    return brooklinInstance;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public String getBrooklinTaskPrefix() {
    return brooklinTaskPrefix;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public String getBrooklinTaskId() {
    return brooklinTaskId;
  }

  /**
   * {@inheritDoc}
   */
  @NotNull
  @Override
  public Instant getTaskStartTime() {
    return taskStartTime;
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
    return Objects.equals(brooklinInstance, that.brooklinInstance)
        && Objects.equals(brooklinTaskPrefix, that.brooklinTaskPrefix)
        && Objects.equals(brooklinTaskId, that.brooklinTaskId)
        && Objects.equals(taskStartTime, that.taskStartTime)
        && Objects.equals(fileName, that.fileName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Objects.hash(brooklinInstance, brooklinTaskPrefix, brooklinTaskId, taskStartTime, fileName);
  }
}