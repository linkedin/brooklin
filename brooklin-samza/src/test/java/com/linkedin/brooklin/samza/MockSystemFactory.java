package com.linkedin.brooklin.samza;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;


public class MockSystemFactory implements SystemFactory {

  public static class MockSystemProducer implements SystemProducer {

    private final String _systemName;
    private final Config _config;
    private List<String> _sources = new ArrayList<>();
    private boolean _isStopped;
    private boolean _isStarted;
    private List<OutgoingMessageEnvelope> _sentEnvelopes = new ArrayList<>();

    private List<OutgoingMessageEnvelope> _flushedEnvelopes = new ArrayList<>();

    public MockSystemProducer(String systemName, Config config, MetricsRegistry registry) {
      _systemName = systemName;
      _config = config;
    }

    @Override
    public void start() {
      _isStarted = true;
    }

    @Override
    public void stop() {
      _isStopped = true;
    }

    @Override
    public void register(String source) {
      _sources.add(source);
    }

    @Override
    public void send(String destination, OutgoingMessageEnvelope envelope) {
      _sentEnvelopes.add(envelope);
    }

    @Override
    public void flush(String source) {
      _flushedEnvelopes.addAll(_sentEnvelopes);
      _sentEnvelopes.clear();
    }

    public String getSystemName() {
      return _systemName;
    }

    public Config getConfig() {
      return _config;
    }

    public List<String> getSources() {
      return _sources;
    }

    public boolean isStopped() {
      return _isStopped;
    }

    public boolean isStarted() {
      return _isStarted;
    }

    public List<OutgoingMessageEnvelope> getSentEnvelopes() {
      return _sentEnvelopes;
    }

    public List<OutgoingMessageEnvelope> getFlushedEnvelopes() {
      return _flushedEnvelopes;
    }
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return null;
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new MockSystemProducer(systemName, config, registry);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SystemAdmin() {
      @Override
      public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        return null;
      }

      @Override
      public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        return null;
      }

      @Override
      public void createChangelogStream(String streamName, int numOfPartitions) {

      }

      @Override
      public void validateChangelogStream(String streamName, int numOfPartitions) {

      }

      @Override
      public void createCoordinatorStream(String streamName) {

      }

      @Override
      public Integer offsetComparator(String offset1, String offset2) {
        return null;
      }
    };
  }
}
