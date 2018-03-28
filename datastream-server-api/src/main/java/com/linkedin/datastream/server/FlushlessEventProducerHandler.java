package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for sending events to the corresponding EventProducer and keeping track of the in flight messages,
 * and the acknowledged checkpoints for each source partition.
 *
 * The main assumption of this class is that: For each source/partition tuple, the send method should
 * be called with monotonically ascending UNIQUE checkpoints.
 *
 * @param <T> Type of the comparable checkpoint object internally used by the connector.
 */
public class FlushlessEventProducerHandler<T extends Comparable<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlushlessEventProducerHandler.class);
  private ConcurrentHashMap<SourcePartition, CallbackStatus> _callbackStatusMap = new ConcurrentHashMap<>();

  private final DatastreamEventProducer _eventProducer;

  public FlushlessEventProducerHandler(DatastreamEventProducer eventProducer) {
    _eventProducer = eventProducer;
  }

  /**
   * Reset all the in-flight status counters, and metadata stored for this eventProducer.
   * This should be used after calling flush and getting partition reassignments.
   */
  public void clear() {
    _callbackStatusMap.clear();
  }

  /**
   * Sends event to the transport.
   *
   * NOTE: This method should be called with monotonically increasing checkpoints for a given source and sourcePartition.
   * @param record the event to send
   * @param sourceCheckpoint the sourceCheckpoint associated with this event. Multiple events could share the same sourceCheckpoint.
   */
  public void send(DatastreamProducerRecord record, String source, int sourcePartition, T sourceCheckpoint) {
    SourcePartition sp = new SourcePartition(source, sourcePartition);
    CallbackStatus status = _callbackStatusMap.computeIfAbsent(sp, d -> new CallbackStatus());
    status.register(sourceCheckpoint);
    _eventProducer.send(record, ((metadata, exception) -> {
      if (exception != null) {
        LOG.error("Failed to send datastream record: " + metadata, exception);
      } else {
        status.ack(sourceCheckpoint);
      }
    }));
  }

  /**
   * @return the latest safe checkpoint acknowledged by a sourcePartition, or an empty optional if no event has been
   * acknowledged.
   */
  public Optional<T> getAckCheckpoint(String source, int sourcePartition) {
    CallbackStatus status = _callbackStatusMap.get(new SourcePartition(source, sourcePartition));
    return Optional.ofNullable(status).map(CallbackStatus::getAckCheckpoint);
  }

  /**
   * @return the inflight count of messages yet to be acknowledged for a given source and sourcePartition.
   */
  public long getInFlightCount(String source, int sourcePartition) {
    CallbackStatus status = _callbackStatusMap.get(new SourcePartition(source, sourcePartition));
    return status != null ? status.getInFlightCount() : 0;
  }

  /**
   * @return a map of all source partitions to their inflight message counts
   */
  public Map<SourcePartition, Long> getInFlightMessagesCounts() {
    return _callbackStatusMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getInFlightCount()));
  }

  /**
   * @return the smallest checkpoint acknowledged by all destinations, or an empty if no event has been acknowledged.
   *         If all tasks are up to date, returns the passed {@code currentCheckpoint}
   *         NOTE: This method assume that the checkpoints are monotonically increasing across DestinationPartition.
   *               For example, for a connector reading from a source with a global monotonic SCN (e.g. Espresso)
   *               this function will work correctly.
   */
  public Optional<T> getAckCheckpoint(T currentCheckpoint, Comparator<T> checkpointComparator) {
    T lowWaterMark = null;

    for (CallbackStatus status : _callbackStatusMap.values()) {
      if (status.getInFlightCount() > 0) {
        T checkpoint = status.getAckCheckpoint();
        if (checkpoint == null) {
          return Optional.empty(); // no events ack yet for this topic partition
        }
        if (lowWaterMark == null || checkpointComparator.compare(checkpoint, lowWaterMark) < 0) {
          lowWaterMark = checkpoint;
        }
      }
    }

    return lowWaterMark != null ? Optional.of(lowWaterMark) : Optional.ofNullable(currentCheckpoint);
  }

  /**
   * Helper class to store the callback status of the inFlight events.
   */
  private class CallbackStatus {
    private T _currentCheckpoint = null;
    private T _highWaterMark = null;

    private Queue<T> _acked = new PriorityQueue<>();
    private Set<T> _inFlight = Collections.synchronizedSet(new LinkedHashSet<>());

    public T getAckCheckpoint() {
      return _currentCheckpoint;
    }

    public long getInFlightCount() {
      return _inFlight.size();
    }

    public void register(T checkpoint) {
      _inFlight.add(checkpoint);
    }

    /**
     * The checkpoints acknowledgement can be received out of order. In that case we need to keep track
     * of the high watermark, and only update the ackCheckpoint when we are sure all events before it has
     * been received.
     */
    public synchronized void ack(T checkpoint) {
      if (!_inFlight.remove(checkpoint)) {
        LOG.error("Internal state error; could not remove checkpoint {}", checkpoint);
      }
      _acked.add(checkpoint);

      if (_highWaterMark == null || _highWaterMark.compareTo(checkpoint) < 0) {
        _highWaterMark = checkpoint;
      }

      if (_inFlight.isEmpty()) {
        // Queue is empty, update to high water mark.
        _currentCheckpoint = _highWaterMark;
        _acked.clear();
      } else {
        // Update the checkpoint to the largest acked message that is still smaller than the first inflight message
        T max = null;
        T first = _inFlight.iterator().next();
        while (!_acked.isEmpty() && _acked.peek().compareTo(first) < 0) {
          max = _acked.poll();
        }
        if (max != null) {
          if (_currentCheckpoint != null && max.compareTo(_currentCheckpoint) < 0) {
            // max is less than current checkpoint, should not happen
            LOG.error(
                "Internal error: checkpoints should progress in increasing order. Resolved checkpoint as {} which is less than current checkpoint of {}",
                max, _currentCheckpoint);
          }
          _currentCheckpoint = max;
        }
      }
    }

  }

  public static final class SourcePartition extends Pair<String, Integer> {
    public SourcePartition(String source, int partition) {
      super(source, partition);
    }

    public String getSource() {
      return getKey();
    }

    public int getPartition() {
      return getValue();
    }

    @Override
    public String toString() {
      return getSource() + "-" + getPartition();
    }
  }
}
