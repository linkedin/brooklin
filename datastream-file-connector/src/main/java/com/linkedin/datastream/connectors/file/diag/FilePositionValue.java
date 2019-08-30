/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.file.diag;

import org.jetbrains.annotations.Nullable;

import com.linkedin.datastream.common.diag.PositionValue;


/**
 * A FilePositionValue provides information about a FileProcessor's current position when reading from a specific file.
 */
public class FilePositionValue implements PositionValue {
  private static final long serialVersionUID = 1L;

  /**
   * The number of bytes read from the file so far.
   */
  private Long bytesRead;

  /**
   * The number of lines read from the file so far.
   */
  private Long linesRead;

  /**
   * The size of the file in bytes.
   */
  private Long fileLengthBytes;

  /**
   * Constructor for FilePositionValue.
   */
  public FilePositionValue() {
  }

  /**
   * Gets the number of bytes read from the file so far.
   * @return the number of bytes read
   */
  @Nullable
  public Long getBytesRead() {
    return bytesRead;
  }

  /**
   * Sets the number of bytes read from the file so far.
   * @param bytes the number of bytes read
   */
  public void setBytesRead(final Long bytes) {
    this.bytesRead = bytes;
  }

  /**
   * Gets the number of lines read from the file so far.
   * @return the number of lines read
   */
  @Nullable
  public Long getLinesRead() {
    return linesRead;
  }

  /**
   * Sets the number of lines read from the file so far.
   * @param lines the number of lines read
   */
  public void setLinesRead(final Long lines) {
    this.linesRead = lines;
  }

  /**
   * Gets the size of the file in bytes.
   * @return the size of the file
   */
  @Nullable
  public Long getFileLengthBytes() {
    return fileLengthBytes;
  }

  /**
   * Sets the size of the file in bytes.
   * @param bytes the size of the file
   */
  public void setFileLengthBytes(final Long bytes) {
    this.fileLengthBytes = bytes;
  }
}