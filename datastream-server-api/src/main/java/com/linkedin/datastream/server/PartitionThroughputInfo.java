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
  private final int _bytesInKBRate;
  private final int _messagesInRate;
  private final String _partitionName;

  /**
   * Creates an instance of {@link PartitionThroughputInfo}
   * @param bytesInKBRate Bytes in rate for the partition
   * @param messagesInRate Messages in rate for the partition
   */
  public PartitionThroughputInfo(int bytesInKBRate, int messagesInRate, String partitionName) {
    _bytesInKBRate = bytesInKBRate;
    _messagesInRate = messagesInRate;
    _partitionName = partitionName;
  }

  /**
   * Gets the bytes in rate (in KB)
   * @return Bytes in rate (in KB)
   */
  public int getBytesInKBRate() {
    return _bytesInKBRate;
  }

  /**
   * Gets the messages in rate
   * @return Messages in rate
   */
  public int getMessagesInRate() {
    return _messagesInRate;
  }

  /**
   * Gets the partition name
   * @return Partition name
   */
  public String getPartitionName() {
    return _partitionName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return String.format("[PartitionName: %s, BytesInKBRate: %s, MessagesInRate: %s]",
        _partitionName, _bytesInKBRate, _messagesInRate);
  }
}
