package com.linkedin.datastream.server;

import java.util.*;

import com.linkedin.datastream.common.Datastream;
import org.testng.Assert;
import org.testng.annotations.*;


/**
 * Tests to validate Message Pool producer
 */
public class TestEventProducerPool {
    private DummyDatastreamEventProducerFactory _eventProducerFactory = new DummyDatastreamEventProducerFactory();;


    @Test
    /**
     * Validates if producers are created  when the pool is empty
     */
    public void testEmptyPool(){
        EventProducerPool pool = new EventProducerPool(_eventProducerFactory);
        List<DatastreamTask> tasks = new ArrayList<DatastreamTask>();
        tasks.add(new DatastreamTask(TestDestinationManager.generateDatastream(1)));
        tasks.add(new DatastreamTask(TestDestinationManager.generateDatastream(2)));

        Map<DatastreamTask, EventProducer> taskProducerMap = pool.getEventProducers(tasks);
        // Number of tasks is same as the number of tasks passed in
        Assert.assertEquals(taskProducerMap.size(), 2);

        // All the tasks that were passed in have a corresponding producer
        tasks.stream().forEach(task -> Assert.assertNotNull(taskProducerMap.get(task)));

        // The producers are unique for different tasks
        Assert.assertTrue(taskProducerMap.get(tasks.get(0)) != taskProducerMap.get(tasks.get(1)));

    }


    @Test
    /**
     * Verifies if producer pool reuses producers across multiple calls
     */
    public void testProducerCreationMultipleTimes(){

        EventProducerPool pool = new EventProducerPool(_eventProducerFactory);
        List<DatastreamTask> tasks = new ArrayList<DatastreamTask>();
        tasks.add(new DatastreamTask(TestDestinationManager.generateDatastream(1)));
        tasks.add(new DatastreamTask(TestDestinationManager.generateDatastream(2)));

        Map<DatastreamTask, EventProducer> taskProducerMap1 = pool.getEventProducers(tasks);

        tasks.add(new DatastreamTask(TestDestinationManager.generateDatastream(3)));
        tasks.add(new DatastreamTask(TestDestinationManager.generateDatastream(4)));

        Map<DatastreamTask, EventProducer> taskProducerMap2 = pool.getEventProducers(tasks);

        // Check if producers are reused
        Assert.assertTrue(taskProducerMap1.get(tasks.get(0)) == taskProducerMap2.get(tasks.get(0)));
        Assert.assertTrue(taskProducerMap1.get(tasks.get(1)) == taskProducerMap2.get(tasks.get(1)));

        // Check if new producers are generated.
        Set<EventProducer> uniqueProducers = new HashSet<>();
        taskProducerMap2.forEach((k, v) -> uniqueProducers.add(v));
        Assert.assertEquals(uniqueProducers.size(), 4);
    }
}


