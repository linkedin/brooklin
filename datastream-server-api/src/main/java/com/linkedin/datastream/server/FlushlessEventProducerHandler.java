package com.linkedin.datastream.server;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for Sending events to the corresponding EventProducer and keeping track of the in flight messages,
 * and the acknowledge checkpoints for each destination partition.
 *
 * The main assumption of this class is that: For each  destination/partition tuple, the send method should
 * be called with monotonically ascending checkpoints.
 *
 * @param <T> Type of the checkpoint object internally used by the connector.
 */

class FlushlessEventProducerHandler<T> {
  private static final Logger LOG = LoggerFactory.getLogger(FlushlessEventProducerHandler.class);
  private ConcurrentHashMap<DestinationPartition, CallbackStatus> _callbackStatusMap = new ConcurrentHashMap<>();

  private final DatastreamEventProducer _eventProducer;

  FlushlessEventProducerHandler(DatastreamEventProducer eventProducer) {
    _eventProducer = eventProducer;
  }

  /**
   * Reset all the inflight status counter, and metadata stored for this eventProducer.
   * This should be used, after calling flush and getting partition reassignments.
   */
  public void clear() {
    _callbackStatusMap.clear();
  }

  /**
   * Sends event to the transport, using the passed EventProducer.
   *
   * NOTE: This method should be called with monotonically increasing checkpoints for a giving DestinationPartition.
   * @param record the event to send
   * @param checkpoint the checkpoint associated with this event.
   *                   Multiple events could share the same checkpoint.
   */
  public void send(DatastreamProducerRecord record, T checkpoint) {
    DestinationPartition dp =
        new DestinationPartition(record.getDestination().orElse(""), record.getPartition().orElse(0));
    CallbackStatus status = _callbackStatusMap.computeIfAbsent(dp, d -> new CallbackStatus());
    status.register(checkpoint);
    _eventProducer.send(record, ((metadata, exception) -> {
      if (exception != null) {
        LOG.error("Failed to send datastream record: " + metadata, exception);
      }
      status.ack(checkpoint);
    }));
  }

  /**
   * @return the latest safe checkpoint acknowledge by a destination Partition. Null if not event has been acknowledged.
   */
  @Nullable
  public T getAckCheckpoint(String destination, int partition) {
    if (destination == null) {
      destination = "";
    }
    CallbackStatus status = _callbackStatusMap.get(new DestinationPartition(destination, partition));
    return status != null ? status.getAckCheckpoint() : null;
  }

  public long getInFlightCount(String destination, int partition) {
    if (destination == null) {
      destination = "";
    }
    CallbackStatus status = _callbackStatusMap.get(new DestinationPartition(destination, partition));
    return status != null ? status.getInFlightCount() : 0;
  }

  /**
   * @return the smallest checkpoint acknowledged by all destinations. Null if not event has been acknowledged.
   *         If all tasks are up to date, returns the passed {@code currentCheckpoint}
   *         NOTE: This method assume that the checkpoints are monotonically increasing across DestinationPartition.
   *               For example, for a connector reading from a source with a global monotonic SCN (e.g. Espresso)
   *               this function will work correctly.
   */
  @Nullable
  public T getAckCheckpoint(T currentCheckpoint, Comparator<T> checkpointComparator) {
    T lowWaterMark = null;

    for (CallbackStatus status : _callbackStatusMap.values()) {
      if (status.getInFlightCount() > 0) {
        T checkpoint = status.getAckCheckpoint();
        if (checkpoint == null) {
          return null; // no events ack yet for this topic partition
        }
        if (lowWaterMark == null || checkpointComparator.compare(checkpoint, lowWaterMark) < 0) {
          lowWaterMark = checkpoint;
        }
      }
    }

    return lowWaterMark != null ? lowWaterMark : currentCheckpoint;
  }

  /**
   * Helper class to store the callback status of the inFlight events.
   */
  private class CallbackStatus {
    private T _currentCheckpoint = null;
    private T _prevCheckpoint = null;
    private AtomicLong _inFlight = new AtomicLong(0);

    public T getAckCheckpoint() {
      if (_inFlight.get() > 0) {
        // If messages are in flight, the current checkpoint could be partial in case of multiple
        // messages for the same checkpoint. It is safer to return the previous one in this case.
        return _prevCheckpoint;
      }
      return _currentCheckpoint;
    }

    public long getInFlightCount() {
      return _inFlight.get();
    }

    public void register(T checkpoint) {
      _inFlight.incrementAndGet();
    }

    /**
     * The checkpoints should acknowledgement comes in order for a Given DestinationPartition
     */
    public synchronized void ack(T checkpoint) {
      _inFlight.decrementAndGet();
      if (!Objects.equals(_currentCheckpoint, checkpoint)) {
        _prevCheckpoint = _currentCheckpoint;
      }
      _currentCheckpoint = checkpoint;
    }
  }

  public static final class DestinationPartition extends Pair<String, Integer> {
    public DestinationPartition(String destination, int partition) {
      super(destination, partition);
    }

    public String getDestination() {
      return getKey();
    }

    public int getPartition() {
      return getValue();
    }
  }
}