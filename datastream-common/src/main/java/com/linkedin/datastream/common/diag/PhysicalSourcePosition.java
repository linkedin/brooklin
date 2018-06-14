package com.linkedin.datastream.common.diag;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;


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
  private long _sourceQueriedTime;

  /**
   * The time that the consumer processed data from this physical source as milliseconds from the Unix epoch.
   */
  private long _consumerProcessedTime;

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
  public long getSourceQueriedTime() {
    return _sourceQueriedTime;
  }

  /**
   * Sets the time that the position of the latest/newest available message was queried from the physical source as
   * milliseconds from the Unix epoch.
   * @param sourceQueriedTime the time that the position of the latest/newest available message was queried from the
   *                          physical source
   */
  public void setSourceQueriedTime(long sourceQueriedTime) {
    _sourceQueriedTime = sourceQueriedTime;
  }

  /**
   * Returns the time that the consumer processed data from this physical source as milliseconds from the Unix epoch.
   * @return the time that the consumer processed data from this physical source
   */
  public long getConsumerProcessedTime() {
    return _consumerProcessedTime;
  }

  /**
   * Sets the time that the consumer processed data from this physical source as milliseconds from the Unix epoch.
   * @param consumerProcessedTime the time that the consumer processed data from this physical source
   */
  public void setConsumerProcessedTime(long consumerProcessedTime) {
    _consumerProcessedTime = consumerProcessedTime;
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
  public void setPositionType(String positionType) {
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
  public void setSourcePosition(String sourcePosition) {
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
  public void setConsumerPosition(String consumerPosition) {
    _consumerPosition = consumerPosition;
  }

  /**
   * Merges two PhysicalSourcePosition objects into a single object with the freshest position data available from both.
   * If two positions do not have the same position type, then they cannot be merged. Instead, a copy of the new
   * position data will be returned.
   *
   * @param existingPosition the existing position data
   * @param newPosition the new position data
   * @return a merged PhysicalSourcePosition, or null if both provided positions are null
   */
  public static PhysicalSourcePosition merge(PhysicalSourcePosition existingPosition, PhysicalSourcePosition newPosition) {
    // If there is no existing position, use the new position.
    if (existingPosition == null) {
      return copy(newPosition);
    }

    // If there is no new position, use the existing position.
    if (newPosition == null) {
      return copy(existingPosition);
    }

    // We can only merge positions if they contain the same position type.
    boolean positionTypesMatch = Optional.ofNullable(existingPosition.getPositionType())
        .filter(positionType -> positionType.equals(newPosition.getPositionType()))
        .isPresent();

    if (!positionTypesMatch) {
      // If they don't match, then just use the new position.
      return copy(newPosition);
    }

    // If they do match, then let's merge the data.
    PhysicalSourcePosition result = new PhysicalSourcePosition();
    result.setPositionType(existingPosition.getPositionType());

    // Use the freshest data for source position in the merge.
    Stream.of(newPosition, existingPosition)
        .max(Comparator.comparingLong(PhysicalSourcePosition::getSourceQueriedTime))
        .ifPresent(position -> {
          result.setSourceQueriedTime(position.getSourceQueriedTime());
          result.setSourcePosition(position.getSourcePosition());
        });

    // Use the freshest data for consumer position in the merge.
    Stream.of(newPosition, existingPosition)
        .max(Comparator.comparingLong(PhysicalSourcePosition::getConsumerProcessedTime))
        .ifPresent(position -> {
          result.setConsumerProcessedTime(position.getConsumerProcessedTime());
          result.setConsumerPosition(position.getConsumerPosition());
        });

    return result;
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
    position.setSourceQueriedTime(toCopy.getSourceQueriedTime());
    position.setConsumerProcessedTime(toCopy.getConsumerProcessedTime());
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
    return "PhysicalSourcePosition{" + "_sourceQueriedTime=" + _sourceQueriedTime + ", _consumerProcessedTime="
        + _consumerProcessedTime + ", _positionType='" + _positionType + '\'' + ", _sourcePosition='" + _sourcePosition
        + '\'' + ", _consumerPosition='" + _consumerPosition + '\'' + '}';
  }
}