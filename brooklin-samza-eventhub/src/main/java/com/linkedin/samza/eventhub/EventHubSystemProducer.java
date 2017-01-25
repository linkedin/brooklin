package com.linkedin.samza.eventhub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;


public class EventHubSystemProducer implements SystemProducer {

  private Throwable _sendExceptionOnCallback;
  private boolean _isStarted;

  public enum PartitioningMethod {
    EVENT_HUB_HASHING,

    PARTITION_KEY_AS_PARTITION,
  }

  public final static String CONFIG_EVENT_HUB_NAMESPACE = "eventHubNamespace";
  public final static String CONFIG_EVENT_HUB_SHARED_KEY_NAME = "sharedKeyName";
  public final static String CONFIG_EVENT_HUB_SHARED_KEY = "sharedKey";
  public final static String CONFIG_PARTITIONING_METHOD = "partitioningMethod";
  public final static String DEFAULT_PARTITIONING_METHOD = PartitioningMethod.EVENT_HUB_HASHING.toString();

  private final PartitioningMethod _partitioningMethod;

  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemProducer.class.getName());

  private final String _systemName;
  private final MetricsRegistry _registry;
  private String _eventHubNamespace;
  private Set<String> _eventHubs = new HashSet<>();
  // Map of the system name to the event hub client.
  private Map<String, EventHubClientWrapper> _eventHubClients = new HashMap<>();

  private String _sharedKeyName;
  private String _sharedKey;

  private List<CompletableFuture<Void>> _pendingFutures = new ArrayList<>();

  public EventHubSystemProducer(String systemName, Config config, MetricsRegistry registry) {
    _systemName = systemName;
    _registry = registry;
    _partitioningMethod =
        PartitioningMethod.valueOf(getConfigValue(config, CONFIG_PARTITIONING_METHOD, DEFAULT_PARTITIONING_METHOD));
    _eventHubNamespace = getConfigValue(config, CONFIG_EVENT_HUB_NAMESPACE);
    _sharedKeyName = getConfigValue(config, CONFIG_EVENT_HUB_SHARED_KEY_NAME);
    _sharedKey = getConfigValue(config, CONFIG_EVENT_HUB_SHARED_KEY);
  }

  private String getConfigValue(Config config, String configKey) {
    return getConfigValue(config, configKey, null);
  }

  private String getConfigValue(Config config, String configKey, String defaultValue) {
    String configValue = config.get(configKey, defaultValue);

    if (configValue == null) {
      throw new SamzaException(configKey + " is not configured.");
    }

    return configValue;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting system producer.");
    for (String eventHub : _eventHubs) {
      ConnectionStringBuilder csBuilder =
          new ConnectionStringBuilder(_eventHubNamespace, eventHub, _sharedKeyName, _sharedKey);
      try {
        EventHubClient ehClient = EventHubClient.createFromConnectionString(csBuilder.toString()).get();
        _eventHubClients.put(eventHub, new EventHubClientWrapper(_partitioningMethod, ehClient));
      } catch (InterruptedException | ExecutionException | IOException | ServiceBusException e) {
        String msg = String.format("Creation of event hub client failed for eventHub %s with exception", eventHub);
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    _isStarted = true;
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping system producer.");
    _eventHubClients.values().forEach(EventHubClientWrapper::closeSync);
    _eventHubClients.clear();
  }

  @Override
  public synchronized void register(String destination) {
    LOG.info("Trying to register {}.", destination);
    if (_isStarted) {
      String msg = "Cannot register once the producer is started.";
      LOG.error(msg);
      throw new SamzaException(msg);
    }
    _eventHubs.add(destination);
  }

  @Override
  public synchronized void send(String destination, OutgoingMessageEnvelope envelope) {
    if (!_isStarted) {
      throw new SamzaException("Trying to call send before the producer is started.");
    }

    if (!_eventHubClients.containsKey(destination)) {
      String msg = String.format("Trying to send event to a destination {%s} that is not registered.", destination);
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    if (_sendExceptionOnCallback != null) {
      SamzaException e = new SamzaException(_sendExceptionOnCallback);
      _sendExceptionOnCallback = null;
      _pendingFutures.clear();
      LOG.error("One of the previous sends failed.");
      throw e;
    }

    EventData eventData = createEventData(envelope);

    EventHubClientWrapper ehClient = _eventHubClients.get(destination);
    CompletableFuture<Void> sendResult = ehClient.send(eventData, envelope.getPartitionKey());

    _pendingFutures.add(sendResult);

    // Auto remove the future from the list when they are complete.
    sendResult.handle(((aVoid, throwable) -> {
      if (throwable != null) {
        LOG.error("Send message failed with exception", throwable);
        _sendExceptionOnCallback = throwable;
      }
      _pendingFutures.remove(sendResult);
      return aVoid;
    }));
  }

  private EventData createEventData(OutgoingMessageEnvelope envelope) {
    if (!(envelope.getMessage() instanceof byte[])) {
      String msg =
          "EventHub system producer doesn't support message type:" + envelope.getMessage().getClass().toString();
      LOG.error(msg);
      throw new SamzaException(msg);
    }
    byte[] eventValue = (byte[]) envelope.getMessage();
    return new EventData(eventValue);
  }

  @Override
  public synchronized void flush(String source) {
    LOG.info("Trying to flush pending {} sends.", _pendingFutures.size());
    // Wait till all the pending sends are complete.
    while (!_pendingFutures.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        String msg = "Flush failed with error";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    if (_sendExceptionOnCallback != null) {
      String msg = "Sending one of the message failed during flush";
      Throwable throwable = _sendExceptionOnCallback;
      _sendExceptionOnCallback = null;
      LOG.error(msg, throwable);
      throw new SamzaException(msg, throwable);
    }
  }

  List<CompletableFuture<Void>> getPendingFutures() {
    return _pendingFutures;
  }
}

