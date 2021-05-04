/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;


/**
 * A structure for partition throughput information.
 */
public class PartitionThroughputInfo {
  private final int _bytesInRate;
  private final int _messagesInRate;
  private final String _partitionName;

  /**
   * Creates an instance of {@link PartitionThroughputInfo}
   * @param bytesInRate Bytes in rate for the partition
   * @param messagesInRate Messages in rate for the partition
   */
  public PartitionThroughputInfo(int bytesInRate, int messagesInRate, String partitionName) {
    _bytesInRate = bytesInRate;
    _messagesInRate = messagesInRate;
    _partitionName = partitionName;
  }

  /**
   * Gets the bytes in rate (in KB)
   */
  public int getBytesInRate() {
    return _bytesInRate;
  }

  /**
   * Gets the messages in rate
   */
  public int getMessagesInRate() {
    return _messagesInRate;
  }

  /**
   * Gets the partition name
   */
  public String getPartitionName() {
    return _partitionName;
  }
}
