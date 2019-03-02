/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

/**
 * A datastream's consumer reads messages sequentially from a physical source. For diagnostic and analytic information,
 * we may want to know our position in the stream (e.g. Do we have more messages to consume? Are we making progress?),
 * or how the event time of the message we're currently processing compares to the current time.
 *
 * For that, we introduce the concept of a position.
 *
 * A message's position can be described generally in two ways:
 * - In terms of a timestamp, the messages's event timestamp.
 * - In terms of a sequence, which refers to a value which uniquely identifies the sequential position of the message on
 *                           the physical source (e.g. in Kafka, an offset).
 *
 * For example, on a physical source with a constant rate of 1 message/second, if the datastream's consumer has been
 * delayed in processing events for 30 minutes, then:
 * - In terms of a timestamp, the consumer's position will be 30 minutes behind the source's position.
 * - In terms of a sequence, the consumer's position will be sequentially before than the source's position. In the case
 *                           of Kafka offsets, it will be a number at least 1800 less.
 *
 * Suggested useful sequences for different actual and hypothetical connectors are as follows:
 * - For Kafka, use the message's offset.
 * - For Oracle SQL, use the message's SCN.
 * - For Azure's EventHub, use the message's SequenceNumber.
 * - For MySQL, use a tuple of binary log name and byte position.
 * - For an Espresso snapshot, use total number of messages processed/available.
 *
 * To avoid ambiguity, this class provides a setting (see {@link #setPositionType(String)}) which lets users of this
 * data know about the type of position being used.
 *
 * This class stores position information for both:
 *  - the datastream's consumer's last processed message
 *  - the source's latest/newest available message
 * as well as the freshness of that position data.
 */
public class PhysicalSourcePosition {

  /**
   * Position type representing an event's time.
   */
  public static final String EVENT_TIME_POSTIION_TYPE = "eventTime";

  /**
   * Position type representing a Kafka offset.
   */
  public static final String KAFKA_OFFSET_POSITION_TYPE = "kafkaOffset";

  /**
   * The time that the position of the latest/newest available message was queried from the physical source as
   * milliseconds from the Unix epoch.
   */
  private long _sourceQueriedTimeMs;

  /**
   * The time that the consumer processed data from this physical source as milliseconds from the Unix epoch.
   */
  private long _consumerProcessedTimeMs;

  /**
   * The position type, which describes what the position data represents.
   */
  private String _positionType;

  /**
   * The position of the latest/newest available message on the physical source.
   */
  private String _sourcePosition;

  /**
   * The position of the datastream's consumer's last processed message.
   */
  private String _consumerPosition;

  /**
   * Default constructor.
   */
  public PhysicalSourcePosition() {
  }

  /**
   * Returns the time that the position of the latest/newest available message was queried from the physical source as
   * milliseconds from the Unix epoch.
   * @return the time that the position of the latest/newest available message was queried from the physical source
   */
  public long getSourceQueriedTimeMs() {
    return _sourceQueriedTimeMs;
  }

  /**
   * Sets the time that the position of the latest/newest available message was queried from the physical source as
   * milliseconds from the Unix epoch.
   * @param sourceQueriedTimeMs the time that the position of the latest/newest available message was queried from the
   *                          physical source
   */
  public void setSourceQueriedTimeMs(final long sourceQueriedTimeMs) {
    _sourceQueriedTimeMs = sourceQueriedTimeMs;
  }

  /**
   * Returns the time that the consumer processed data from this physical source as milliseconds from the Unix epoch.
   * @return the time that the consumer processed data from this physical source
   */
  public long getConsumerProcessedTimeMs() {
    return _consumerProcessedTimeMs;
  }

  /**
   * Sets the time that the consumer processed data from this physical source as milliseconds from the Unix epoch.
   * @param consumerProcessedTimeMs the time that the consumer processed data from this physical source
   */
  public void setConsumerProcessedTimeMs(final long consumerProcessedTimeMs) {
    _consumerProcessedTimeMs = consumerProcessedTimeMs;
  }

  /**
   * Returns the position type, which describes what the position data represents.
   * @return the position type
   */
  public String getPositionType() {
    return _positionType;
  }

  /**
   * Sets the position type, which describes what the position data represents.
   * @param positionType the position type
   */
  public void setPositionType(final String positionType) {
    _positionType = positionType;
  }

  /**
   * Returns the position of the latest/newest available message on the physical source.
   * @return the position of the latest/newest available message on the physical source
   */
  public String getSourcePosition() {
    return _sourcePosition;
  }

  /**
   * Sets the position of the latest/newest available message on the physical source.
   * @param sourcePosition the position of the latest/newest available message on the physical source
   */
  public void setSourcePosition(final String sourcePosition) {
    _sourcePosition = sourcePosition;
  }

  /**
   * Returns the position of the datastream's consumer's last processed message.
   * @return the position of the datastream's consumer's last processed message
   */
  public String getConsumerPosition() {
    return _consumerPosition;
  }

  /**
   * Sets the position of the datastream's consumer's last processed message.
   * @param consumerPosition the position of the datastream's consumer's last processed message
   */
  public void setConsumerPosition(final String consumerPosition) {
    _consumerPosition = consumerPosition;
  }


  /**
   * Returns a copy of the provided PhysicalSourcePosition object.
   * @param toCopy the PhysicalSourcePosition object to copy
   * @return a copy of the provided PhysicalSourcePosition object, or null if the provided object is null
   */
  public static PhysicalSourcePosition copy(PhysicalSourcePosition toCopy) {
    if (toCopy == null) {
      return null;
    }
    PhysicalSourcePosition position = new PhysicalSourcePosition();
    position.setSourceQueriedTimeMs(toCopy.getSourceQueriedTimeMs());
    position.setConsumerProcessedTimeMs(toCopy.getConsumerProcessedTimeMs());
    position.setPositionType(toCopy.getPositionType());
    position.setSourcePosition(toCopy.getSourcePosition());
    position.setConsumerPosition(toCopy.getConsumerPosition());
    return position;
  }

  /**
   * A simple String representation of this object suitable for use in debugging or logging.
   * @return a simple String representation of this object
   */
  @Override
  public String toString() {
    return "PhysicalSourcePosition{" + "_sourceQueriedTimeMs=" + _sourceQueriedTimeMs + ", _consumerProcessedTimeMs="
        + _consumerProcessedTimeMs + ", _positionType='" + _positionType + '\'' + ", _sourcePosition='" + _sourcePosition
        + '\'' + ", _consumerPosition='" + _consumerPosition + '\'' + '}';
  }
}