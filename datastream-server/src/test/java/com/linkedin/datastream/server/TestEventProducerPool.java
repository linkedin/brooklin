package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;
import com.linkedin.datastream.server.providers.CheckpointProvider;

import static org.mockito.Mockito.mock;

/**
 * Tests to validate Message Pool producer
 */
public class TestEventProducerPool {

  private EventProducerPool _eventProducerPool;

  public void setup(boolean throwOnSend) throws Exception {
    CheckpointProvider checkpointProvider = mock(CheckpointProvider.class);
    TransportProviderFactory factory = new DummyTransportProviderFactory(throwOnSend);
    SchemaRegistryProvider schemaReg = mock(SchemaRegistryProvider.class);
    Properties transportConfig = new Properties();
    Properties producerConfig = new Properties();
    _eventProducerPool = new EventProducerPool(checkpointProvider, schemaReg,
            factory, transportConfig, producerConfig);
  }



  @AfterMethod
    public void cleanup() {
    if (_eventProducerPool != null) {
      _eventProducerPool.shutdown();
    }
  }

  @Test
  /**
   * Validates if producers are created  when the pool is empty
   */
  public void testEmptyPool()
      throws Exception {
    setup(false);
    List<DatastreamTask> connectorTasks = new ArrayList<>();
    String connectorType = "connectortype";
    connectorTasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    connectorTasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));

    _eventProducerPool.assignEventProducers(connectorType, connectorTasks, new ArrayList<>(), false);

    // All the tasks that were passed in have a corresponding producer
    connectorTasks.forEach(task -> Assert.assertNotNull(task.getEventProducer()));

    // The producers are unique for different tasks
    Assert.assertNotEquals(connectorTasks.get(0).getEventProducer(), connectorTasks.get(1).getEventProducer());
  }

  @Test
  public void testEventProducerPoolCreatesNewProducerOnUnrecoverableError()
      throws Exception {

    List<DatastreamRecordMetadata> metadata = new ArrayList<>();
    List<Exception> exceptions = new ArrayList<>();
    setup(true);
    DatastreamTaskImpl task = new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true));
    _eventProducerPool.assignEventProducers(task.getConnectorType(), Collections.singletonList(task), new ArrayList<>(), false);
    Assert.assertNotNull(task.getEventProducer());
    DatastreamEventProducerImpl oldDatastreamEventProducer = (DatastreamEventProducerImpl) task.getEventProducer();
    EventProducer oldEventProducer = oldDatastreamEventProducer.getEventProducer();
    task.getEventProducer().send(createEventRecord(0), (m, e) -> {
      metadata.add(m);
      if (e != null) {
        exceptions.add(e);
      }
    });

    DatastreamEventProducerImpl newDatastreamEventProducer = (DatastreamEventProducerImpl) task.getEventProducer();

    // Check whether the DatastreamEventProducer is the same object
    Assert.assertEquals(oldDatastreamEventProducer, newDatastreamEventProducer);
    Assert.assertNotEquals(oldEventProducer, newDatastreamEventProducer.getEventProducer());
    Assert.assertEquals(metadata.size(), 1);
    Assert.assertEquals(exceptions.size(), 1);
  }

  private DatastreamProducerRecord createEventRecord(Integer partition) {
    DatastreamEvent event = new DatastreamEvent();
    event.key = null;
    event.payload = null;
    event.previous_payload = null;
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(event);
    builder.setPartition(partition);
    builder.setSourceCheckpoint("new dummy checkpoint");
    builder.setEventsTimestamp(System.currentTimeMillis());
    return builder.build();
  }

  @Test
  /**
   * Validates that producers are not shared across connector types
   */
  public void testProducersNotSharedForDifferentConnectorTypes()
      throws Exception {
    setup(false);
    // Create tasks for a different connector type
    List<DatastreamTask> connector1tasks = new ArrayList<>();
    List<DatastreamTask> connector2tasks = new ArrayList<>();

    connector1tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    connector1tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    String connectorType1 = "connectortype1";

    connector2tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    connector2tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    String connectorType2 = "connectortype2";

    _eventProducerPool.assignEventProducers(connectorType1, connector1tasks, new ArrayList<>(), false);
    _eventProducerPool.assignEventProducers(connectorType2, connector2tasks, new ArrayList<>(), false);

    // Check that the producers are not shared
    connector1tasks
        .stream()
        .map(DatastreamTask::getEventProducer)
        .anyMatch(p -> connector2tasks.stream().anyMatch(x -> x.getEventProducer().equals(p)));
  }
}
