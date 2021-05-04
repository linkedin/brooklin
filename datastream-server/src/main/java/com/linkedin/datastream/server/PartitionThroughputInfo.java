package com.linkedin.datastream.server;


/**
 * A structure for partition throughput information.
 */
public class PartitionThroughputInfo {
  private final int _bytesInRate;
  private final int _messagesInRate;

  /**
   * Creates an instance of {@link PartitionThroughputInfo}
   * @param bytesInRate Bytes in rate for the partition
   * @param messagesInRate Messages in rate for the partition
   */
  public PartitionThroughputInfo(int bytesInRate, int messagesInRate) {
    _bytesInRate = bytesInRate;
    _messagesInRate = messagesInRate;
  }

  /**
   * Gets the bytes in rate
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
}