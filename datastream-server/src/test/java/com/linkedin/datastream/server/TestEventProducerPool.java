package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.*;


/**
 * Tests to validate Message Pool producer
 */
public class TestEventProducerPool {

  @Test
  /**
   * Validates if producers are created  when the pool is empty
   */
  public void testEmptyPool() {
    EventProducerPool pool = new EventProducerPool();
    List<DatastreamTask> connectorTasks = new ArrayList<DatastreamTask>();
    connectorTasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1)));
    connectorTasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2)));
    String connectorType = "connectortype";

    Map<DatastreamTask, EventProducer> taskProducerMapConnectorType =
        pool.getEventProducers(connectorTasks, connectorType);

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
    EventProducerPool pool = new EventProducerPool();

    // Create tasks for a different connector type
    List<DatastreamTask> connector1tasks = new ArrayList<DatastreamTask>();
    List<DatastreamTask> connector2tasks = new ArrayList<DatastreamTask>();

    connector1tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1)));
    connector1tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2)));
    String connectorType1 = "connectortype1";

    connector2tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1)));
    connector2tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2)));
    String connectorType2 = "connectortype2";

    Map<DatastreamTask, EventProducer> taskProducerMapConnectorType1 =
        pool.getEventProducers(connector1tasks, connectorType1);
    Map<DatastreamTask, EventProducer> taskProducerMapConnectorType2 =
        pool.getEventProducers(connector2tasks, connectorType2);

    // Check that the producers are not shared
    for (EventProducer producer1 : taskProducerMapConnectorType1.values()) {
      for (EventProducer producer2: taskProducerMapConnectorType2.values()) {
        Assert.assertNotEquals(producer1, producer2);
      }
    }
  }

  @Test
  /**
   * Verifies if producer pool reuses producers across multiple calls
   */
  public void testProducerCreationMultipleTimes() {

    EventProducerPool pool = new EventProducerPool();
    List<DatastreamTask> tasks = new ArrayList<DatastreamTask>();
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2)));
    String connectorType = "connectorType";

    Map<DatastreamTask, EventProducer> taskProducerMap1 = pool.getEventProducers(tasks, connectorType);

    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(3)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(4)));

    Map<DatastreamTask, EventProducer> taskProducerMap2 = pool.getEventProducers(tasks, connectorType);

    // Check if producers are reused
    Assert.assertTrue(taskProducerMap1.get(tasks.get(0)) == taskProducerMap2.get(tasks.get(0)));
    Assert.assertTrue(taskProducerMap1.get(tasks.get(1)) == taskProducerMap2.get(tasks.get(1)));

    // Check if new producers are generated for the new tasks.
    Set<EventProducer> uniqueProducers = new HashSet<>();
    taskProducerMap2.forEach((k, v) -> uniqueProducers.add(v));
    Assert.assertEquals(uniqueProducers.size(), 4);
  }

  @Test
  /**
   * Verify if producers are shared for tasks with same destinations(bootstrap scenario)
   */
  public void testProducerSharedForTasksWithSameDestination() {

    EventProducerPool pool = new EventProducerPool();
    List<DatastreamTask> tasks = new ArrayList<DatastreamTask>();
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2)));
    tasks.add(new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1)));
    String connectorType = "connectorType";

    Map<DatastreamTask, EventProducer> taskProducerMap = pool.getEventProducers(tasks, connectorType);

    // Check if producers are reused
    Assert.assertTrue(taskProducerMap.get(tasks.get(0)) == taskProducerMap.get(tasks.get(2)));
  }
}
