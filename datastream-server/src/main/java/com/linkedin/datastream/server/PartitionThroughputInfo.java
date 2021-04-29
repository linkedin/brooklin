package com.linkedin.datastream.server;


/**
 * A structure for partition throughput information.
 */
public class PartitionThroughputInfo {
  private final Long _bytesInRate;
  private final Long _messagesInRate;

  /**
   * Creates an instance of {@link PartitionThroughputInfo}
   * @param bytesInRate Bytes in rate for the partition
   * @param messagesInRate Messages in rate for the partition
   */
  public PartitionThroughputInfo(Long bytesInRate, Long messagesInRate) {
    _bytesInRate = bytesInRate;
    _messagesInRate = messagesInRate;
  }

  /**
   * Gets the bytes in rate
   */
  public Long getBytesInRate() {
    return _bytesInRate;
  }

  /**
   * Gets the messages in rate
   */
  public Long getMessagesInRate() {
    return _messagesInRate;
  }
}
