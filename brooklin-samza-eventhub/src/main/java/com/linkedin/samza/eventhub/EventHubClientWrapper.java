package com.linkedin.samza.eventhub;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.servicebus.ServiceBusException;


public class EventHubClientWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(EventHubClientWrapper.class.getName());

  private final EventHubSystemProducer.PartitioningMethod _partitioningMethod;
  private EventHubClient _eventHubClient;
  private Map<Integer, PartitionSender> _partitionSenders = new HashMap<>();

  public EventHubClientWrapper(EventHubSystemProducer.PartitioningMethod partitioningMethod,
      EventHubClient eventHubClient) {
    _eventHubClient = eventHubClient;
    _partitioningMethod = partitioningMethod;
  }

  public void closeSync() {
    _partitionSenders.entrySet().stream().forEach(x -> {
      try {
        x.getValue().closeSync();
      } catch (ServiceBusException e) {
        String msg = "Closing the partition sender failed for partition " + x.getKey();
        LOG.warn(msg, e);
      }
    });

    try {
      _eventHubClient.closeSync();
    } catch (ServiceBusException e) {
      String msg = "Closing the event hub client failed.";
      LOG.warn(msg, e);
    }
  }

  public CompletableFuture<Void> send(EventData eventData, Object partitionKey) {
    if (_partitioningMethod == EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING) {
      return _eventHubClient.send(eventData, convertPartitionKeyToString(partitionKey));
    } else if (_partitioningMethod == EventHubSystemProducer.PartitioningMethod.PARTITION_KEY_AS_PARTITION) {
      if (!(partitionKey instanceof Integer)) {
        String msg = "Partition key should be of type Integer";
        LOG.error(msg);
        throw new SamzaException(msg);
      }
      PartitionSender sender = getPartitionSender((int) partitionKey);
      return sender.send(eventData);
    } else {
      throw new SamzaException("Unknown partitioning method " + _partitioningMethod);
    }
  }

  private String convertPartitionKeyToString(Object partitionKey) {
    if (partitionKey instanceof String) {
      return (String) partitionKey;
    } else if (partitionKey instanceof byte[]) {
      return new String((byte[]) partitionKey);
    } else {
      throw new SamzaException("Unsupported key type: " + partitionKey.getClass().toString());
    }
  }

  private PartitionSender getPartitionSender(int partition) {
    if (!_partitionSenders.containsKey(partition)) {
      try {
        PartitionSender partitionSender = _eventHubClient.createPartitionSenderSync(String.valueOf(partition));
        _partitionSenders.put(partition, partitionSender);
      } catch (ServiceBusException e) {
        String msg = "Creation of partition sender failed with exception";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    return _partitionSenders.get(partition);
  }
}
