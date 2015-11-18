package com.linkedin.datastream.server;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamEventRecord;
import com.linkedin.datastream.common.DatastreamVerifier;
import com.linkedin.datastream.common.VerifiableProperties;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DatastreamEventProducer {

  interface FlushPolicy {
    /**
     * Allow policy to update its internal stats with
     * the latest event
     * @param eventRecord current event record
     */
    void update(DatastreamEventRecord eventRecord);

    /**
     * @return if flush condition is met based on the policy
     */
    boolean isFlushNeeded();

    /**
     * Free up any resources
     */
    void shutdown();
  }

  /**
   * Base class for asynchrounous flushing policies.
   */
  abstract class AsyncFlushPolicy implements FlushPolicy {
    @Override
    public void update(DatastreamEventRecord eventRecord) {
    }
    @Override
    public boolean isFlushNeeded() {
      return false;
    }
  }

  /**
   * This policy toggles flush condition based on a scheduled period.
   * When flush condition is met, it triggers flush directly.
   */
  class PeriodicFlushPolicy extends AsyncFlushPolicy implements Runnable {
    private final Long _flushPeriodMs;
    private final ScheduledThreadPoolExecutor _flushExecutor;
    private volatile boolean _shutdownRequested = false;

    public PeriodicFlushPolicy(StringMap metadata) {
      String str = metadata.getOrDefault("flushThreshold", "1000000");
      _flushPeriodMs = Long.parseLong(str);
      _flushExecutor = new ScheduledThreadPoolExecutor(1);
      _flushExecutor.schedule(this, _flushPeriodMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void update(DatastreamEventRecord eventRecord) {
      if ((_flushExecutor.isShutdown() || _flushExecutor.isTerminated() || _flushExecutor.isTerminating())
          && !_shutdownRequested) {
        throw new RuntimeException("Flush thread unexpected died for " + _task);
      }
    }

    @Override
    public void shutdown() {
      _flushExecutor.shutdown();
    }

    @Override
    public void run() {
      if (_shutdownRequested) {
        return;
      }
      flush();
    }
  }

  /**
   * Base class for synchrounous flushing policies.
   */
  abstract class SyncFlushPolicyBase implements FlushPolicy {
    protected long _flushThreshold;
    protected long _currentValue;

    public SyncFlushPolicyBase(StringMap metadata, String defVal) {
      String str = metadata.getOrDefault("flushThreshold", defVal);
      _flushThreshold = Long.parseLong(str);
      _currentValue = 0L;
    }

    @Override
    public void update(DatastreamEventRecord eventRecord) {
      if (isFlushNeeded()) {
        _currentValue = 0L;
      }
    }

    @Override
    public boolean isFlushNeeded() {
      return _currentValue >= _flushThreshold;
    }

    @Override
    public void shutdown() {
    }
  }

  class BytesFlushPolicy extends SyncFlushPolicyBase {
    public BytesFlushPolicy(StringMap metadata) {
      super(metadata, "1000000");
    }

    @Override
    public void update(DatastreamEventRecord eventRecord) {
      super.update(eventRecord);
      _currentValue += eventRecord.getEventSize();
    }
  }

  class EventsFlushPolicy extends SyncFlushPolicyBase {
    public EventsFlushPolicy(StringMap metadata) {
      super(metadata, "1000");
    }

    @Override
    public void update(DatastreamEventRecord eventRecord) {
      super.update(eventRecord);
      ++_currentValue;
    }
  }

  class TxnsFlushPolicy extends SyncFlushPolicyBase {
    public TxnsFlushPolicy(StringMap metadata) {
      super(metadata, "10");
    }

    @Override
    public void update(DatastreamEventRecord eventRecord) {
      super.update(eventRecord);
      if (eventRecord.isTxnEnd()) {
        ++_currentValue;
      }
    }
  }

  public static final String POLICY_PERIODIC = "PERIOD";
  public static final String POLICY_BYTES = "BYTES";
  public static final String POLICY_EVENTS = "EVENTS";
  public static final String POLICY_TXNS = "TXNS";

  private final DatastreamTask _task;
  private final String _taskName;
  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final String _initialCheckpoint;

  private FlushPolicy _flushPolicy;

  private volatile String _lastSourceOffset;

  public DatastreamEventProducer(DatastreamTask task, TransportProvider transportProvider,
      CheckpointProvider checkpointProvider, VerifiableProperties config) {
    Objects.requireNonNull(task, "null task");
    Objects.requireNonNull(task.getDatastream(), "null datastream");
    Objects.requireNonNull(transportProvider, "null transport provider");
    Objects.requireNonNull(checkpointProvider, "null checkpoint provider");
    Objects.requireNonNull(config, "null config");

    DatastreamVerifier.checkExisting(task.getDatastream());

    _task = task;
    _taskName = task.getDatastreamTaskName();
    _transportProvider = transportProvider;
    _checkpointProvider = checkpointProvider;

    initFlushPolicy(task.getDatastream());

    // Parse configs

    _initialCheckpoint = _checkpointProvider.load(_taskName);
  }

  private void initFlushPolicy(Datastream datastream) {
    StringMap metadata = datastream.getMetadata();
    String policyType = metadata.getOrDefault("flushPolicy", "PERIODIC");
    policyType = policyType.toUpperCase();
    switch (policyType) {
      case POLICY_PERIODIC:
        _flushPolicy = new PeriodicFlushPolicy(metadata);
        break;
      case POLICY_BYTES:
        _flushPolicy = new BytesFlushPolicy(metadata);
        break;
      case POLICY_EVENTS:
        _flushPolicy = new EventsFlushPolicy(metadata);
        break;
      case POLICY_TXNS:
        _flushPolicy = new TxnsFlushPolicy(metadata);
        break;
      default:
        throw new RuntimeException("unknown flush policy: " + policyType);
    }
  }

  /**
   * Trigger a flush on the underlying transport
   * and save the checkpoints after completion
   */
  private synchronized void flush() {
    String currSourceOffset = getLatestCheckpoint();
    _transportProvider.flush();
    _checkpointProvider.save(_taskName, currSourceOffset);
  }

  /**
   * @return the initial checkpoint loaded from CheckpointProvider
   */
  public String getInitialCheckpoint() {
    return new String(_initialCheckpoint);
  }

  /**
   * @return the checkpoint of the latest sent event
   */
  public String getLatestCheckpoint() {
    return new String(_lastSourceOffset);
  }

  /**
   * Send the event onto the underlying transport. This method can be
   * unblocking or blocking for asynchronous flushing and synchronous
   * flushing respectively. For the latter, it blocks when the flush
   * condition has met.
   *
   * @param eventRecord
   */
  public synchronized void send(DatastreamEventRecord eventRecord) {
    Objects.requireNonNull(eventRecord);
    Objects.requireNonNull(eventRecord.event());

    _transportProvider.send(eventRecord);

    _lastSourceOffset = eventRecord.getSourceOffset();

    _flushPolicy.update(eventRecord);

    if (_flushPolicy.isFlushNeeded()) {
      flush();
    }
  }

  public void shutdown() {
    _flushPolicy.shutdown();
    flush();
  }
}
