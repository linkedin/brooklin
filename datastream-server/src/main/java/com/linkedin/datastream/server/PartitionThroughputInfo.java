package com.linkedin.datastream.server;

public class PartitionThroughputInfo {
  private final Long _bytesInRate;
  private final Long _messagesInRate;

  public PartitionThroughputInfo(Long bytesInRate, Long messagesInRate) {
    _bytesInRate = bytesInRate;
    _messagesInRate = messagesInRate;
  }

  public Long getBytesInRate() {
    return _bytesInRate;
  }

  public Long getMessagesInRate() {
    return _messagesInRate;
  }

}
