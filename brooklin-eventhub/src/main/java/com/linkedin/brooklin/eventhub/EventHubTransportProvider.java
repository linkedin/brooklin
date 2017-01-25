package com.linkedin.brooklin.eventhub;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

import com.linkedin.datastream.common.AvroUtils;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class EventHubTransportProvider implements TransportProvider {

  private HashSet<CompletableFuture<Void>> _pendingFutures = new HashSet<>();
  private static final Logger LOG = LoggerFactory.getLogger(EventHubTransportProvider.class.getName());
  private final EventHubDestination _destination;
  private EventHubClient _ehClient;
  private Map<Integer, PartitionSender> _partitionSenders = new HashMap<>();

  private Error _unrecoverableError = null;

  public EventHubTransportProvider(EventHubDestination destination, List<Integer> partitions) {
    _destination = destination;

    ConnectionStringBuilder csBuilder =
        new ConnectionStringBuilder(_destination.getEventHubNamespace(), _destination.getEventHubName(),
            _destination.getSharedAccessKeyName(), _destination.getSharedAccessKey());
    try {
      _ehClient = EventHubClient.createFromConnectionString(csBuilder.toString()).get();
    } catch (InterruptedException | ExecutionException | IOException | ServiceBusException e) {
      String msg = "Creation of event hub client failed with exception";
      LOG.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }

    for (Integer partition : partitions) {
      try {
        _partitionSenders.put(partition, _ehClient.createPartitionSenderSync(String.valueOf(partition)));
      } catch (ServiceBusException e) {
        String msg = "Unable to create a eventHub partitionSender for partition " + partition;
        LOG.error(msg, e);
        throw new DatastreamRuntimeException(msg, e);
      }
    }
  }

  @Override
  public synchronized void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {

    if (_unrecoverableError != null) {
      throw _unrecoverableError;
    }

    List<EventData> events =
        record.getEvents().stream().map(x -> createEventData(x.getValue())).collect(Collectors.toList());

    if (!record.getPartition().isPresent()) {
      String msg = "Event hub transport provider requires partition to be specified in DatastreamProducerRecord";
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
    int partition = record.getPartition().get();
    List<CompletableFuture<Void>> sendResults =
        events.stream().map(x -> sendToPartition(x, partition)).collect(Collectors.toList());
    DatastreamRecordMetadata recordMetadata = new DatastreamRecordMetadata(record.getCheckpoint(), "", partition);

    CompletableFuture<?>[] allsendResults = new CompletableFuture<?>[events.size()];
    sendResults.toArray(allsendResults);

    CompletableFuture<Void> pendingFuture = CompletableFuture.allOf(allsendResults).handle((aVoid, throwable) -> {
      if (throwable instanceof Error) {
        LOG.error("send failed with unrecoverable error", throwable);
        _unrecoverableError = (Error) throwable;
        close();
        _ehClient = null;
      } else {
        onComplete.onCompletion(recordMetadata, (Exception) throwable);
      }

      return aVoid;
    });

    _pendingFutures.add(pendingFuture);
    pendingFuture.thenRun(() -> _pendingFutures.remove(pendingFuture));
  }

  private CompletableFuture<Void> sendToPartition(EventData event, int partition) {

    if (!_partitionSenders.containsKey(partition)) {
      String msg = "Cannot send to unknown partition: " + partition;
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    return _partitionSenders.get(partition).send(event);
  }

  private EventData createEventData(Object value) {
    byte[] eventBinaryData;
    if (value instanceof IndexedRecord) {
      IndexedRecord payloadRecord = (IndexedRecord) value;
      try {
        eventBinaryData = AvroUtils.encodeAvroIndexedRecord(payloadRecord.getSchema(), payloadRecord);
      } catch (IOException e) {
        String msg = "Failed to encode event in Avro, event=" + payloadRecord;
        LOG.error(msg, e);
        throw new DatastreamRuntimeException(msg, e);
      }
    } else if (value instanceof String) {
      eventBinaryData = ((String) value).getBytes();
    } else {
      String msg = String.format(
          "Transport provider supports payloads of type String or AvroRecords, Record {%s} is of type: %s",
          value.toString(), value.getClass().toString());
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    return new EventData(eventBinaryData);
  }

  @Override
  public synchronized void close() {
    if (!_partitionSenders.isEmpty()) {
      _partitionSenders.values().forEach(x -> {
        try {
          x.closeSync();
        } catch (ServiceBusException e) {
          LOG.warn("Closing the partition sender failed with exception.", e);
        }
      });
    }

    if (_ehClient != null) {
      try {
        _ehClient.closeSync();
      } catch (ServiceBusException e) {
        LOG.warn("Closing the client failed with exception", e);
      }
    }
  }

  @Override
  public synchronized void flush() {

    if (_unrecoverableError != null) {
      throw _unrecoverableError;
    }

    // Event hub java APIs doesn't provide flush semantics. Hence we need to wait till all the pending sends are complete
    // and then return.
    // Wait till all the futures corresponding to pending sends are complete.
    for (CompletableFuture<Void> f : _pendingFutures) {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        String msg = "Flush failed with error";
        LOG.error(msg, e);
        throw new DatastreamRuntimeException(msg, e);
      }
    }

    // Pending futures ideally should be empty here, because they are removed automatically when the future is complete.
    // This is just for extra protection.
    _pendingFutures.clear();
  }
}
