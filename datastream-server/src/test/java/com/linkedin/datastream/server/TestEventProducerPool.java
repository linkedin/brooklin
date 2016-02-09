package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderFactory;

import static org.mockito.Mockito.mock;

/**
 * Tests to validate Message Pool producer
 */
public class TestEventProducerPool {

  private EventProducerPool _eventProducerPool;

  @BeforeMethod
  public void setUp() throws Exception {
    CheckpointProvider checkpointProvider = mock(CheckpointProvider.class);
    TransportProviderFactory factory = new DummyTransportProviderFactory();
    SchemaRegistryProvider schemaReg = mock(SchemaRegistryProvider.class);
    Properties transportConfig = new Properties();
    Properties producerConfig = new Properties();
    _eventProducerPool = new EventProducerPool(checkpointProvider, schemaReg,
            factory, transportConfig, producerConfig);
  }

  @Test
  /**
   * Validates if producers are created  when the pool is empty
   */
  public void testEmptyPool() {
    List<DatastreamTask> connectorTasks = new ArrayList<>();
    String connectorType = "connectortype";
    connectorTasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    connectorTasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));

    Map<DatastreamTask, DatastreamEventProducer> taskProducerMapConnectorType =
        _eventProducerPool.getEventProducers(connectorTasks, connectorType, false, new ArrayList<>());

    // Number of tasks is same as the number of tasks passed in
    Assert.assertEquals(taskProducerMapConnectorType.size(), 2);

    // All the tasks that were passed in have a corresponding producer
    connectorTasks.forEach(task -> Assert.assertNotNull(taskProducerMapConnectorType.get(task)));

    // The producers are unique for different tasks
    Assert.assertTrue(taskProducerMapConnectorType.get(connectorTasks.get(0)) != taskProducerMapConnectorType
        .get(connectorTasks.get(1)));
  }

  @Test
  /**
   * Validates that producers are not shared across connector types
   */
  public void testProducersNotSharedForDifferentConnectorTypes() {

    // Create tasks for a different connector type
    List<DatastreamTask> connector1tasks = new ArrayList<>();
    List<DatastreamTask> connector2tasks = new ArrayList<>();

    connector1tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    connector1tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    String connectorType1 = "connectortype1";

    connector2tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    connector2tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    String connectorType2 = "connectortype2";

    Map<DatastreamTask, DatastreamEventProducer> taskProducerMapConnectorType1 =
        _eventProducerPool.getEventProducers(connector1tasks, connectorType1, false, new ArrayList<>());
    Map<DatastreamTask, DatastreamEventProducer> taskProducerMapConnectorType2 =
        _eventProducerPool.getEventProducers(connector2tasks, connectorType2, false, new ArrayList<>());

    // Check that the producers are not shared
    for (DatastreamEventProducer producer1 : taskProducerMapConnectorType1.values()) {
      for (DatastreamEventProducer producer2 : taskProducerMapConnectorType2.values()) {
        Assert.assertNotEquals(producer1, producer2);
      }
    }
  }

  @Test
  /**
   * Verifies if producer pool reuses producers across multiple calls
   */
  public void testProducerCreationMultipleTimes() {

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    String connectorType = "connectorType";

    Map<DatastreamTask, DatastreamEventProducer> taskProducerMap1 =
        _eventProducerPool.getEventProducers(tasks, connectorType, false, new ArrayList<>());

    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(3, true)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(4, true)));

    Map<DatastreamTask, DatastreamEventProducer> taskProducerMap2 =
        _eventProducerPool.getEventProducers(tasks, connectorType, false, new ArrayList<>());

    // Check if producers are reused
    Assert.assertEquals(taskProducerMap1.get(tasks.get(0)), taskProducerMap2.get(tasks.get(0)));
    Assert.assertEquals(taskProducerMap1.get(tasks.get(1)), taskProducerMap2.get(tasks.get(1)));

    // Check if new producers are generated for the new tasks.
    Set<DatastreamEventProducer> uniqueProducers = new HashSet<>();
    taskProducerMap2.forEach((k, v) -> uniqueProducers.add(v));
    Assert.assertEquals(uniqueProducers.size(), 4);
  }

  @Test
  /**
   * Verify if producers are shared for tasks with same destinations(bootstrap scenario)
   */
  public void testProducerSharedForTasksWithSameDestination() {

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true), "1a", Collections.singletonList(1)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true), "1b", Collections.singletonList(2)));
    String connectorType = "connectorType";

    Map<DatastreamTask, DatastreamEventProducer> taskProducerMap =
        _eventProducerPool.getEventProducers(tasks, connectorType, false, new ArrayList<>());

    // Check if producers are reused
    Assert.assertEquals(((DatastreamEventProducerImpl) taskProducerMap.get(tasks.get(0))).getEventProducer(),
            ((DatastreamEventProducerImpl) taskProducerMap.get(tasks.get(2))).getEventProducer());
  }

  @Test
  /**
   * Verifies if producer pool correctly returns the unused producers
   */
  public void testUnusedProducers() {

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1, true)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2, true)));
    String connectorType = "connectorType";

    List<EventProducer> unusedProducers = new ArrayList<>();
    Map<DatastreamTask, DatastreamEventProducer> taskProducerMap1 =
            _eventProducerPool.getEventProducers(tasks, connectorType, false, unusedProducers);

    // Make sure there is no unused producers at this time
    Assert.assertEquals(unusedProducers.size(), 0);

    tasks.remove(1);

    Map<DatastreamTask, DatastreamEventProducer> taskProducerMap2 =
            _eventProducerPool.getEventProducers(tasks, connectorType, false, unusedProducers);

    // Check if producer for tasks[0] is reused
    Assert.assertEquals(taskProducerMap1.get(tasks.get(0)), taskProducerMap2.get(tasks.get(0)));

    // Make sure there is exactly one unused producer
    Assert.assertEquals(unusedProducers.size(), 1);
  }
}
