/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.testutil.MetricsTestUtils;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractKafkaConnector}
 */
public class TestAbstractKafkaConnector {

  private static final Logger LOG = LoggerFactory.getLogger(TestAbstractKafkaConnector.class);

  @BeforeMethod
  public void setup(Method method) {
    DynamicMetricsManager.createInstance(new MetricRegistry(), method.getName());
  }

  @Test
  public void testConnectorRestartCalled() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "1");
    TestKafkaConnector connector = new TestKafkaConnector(false, props, false);
    DatastreamTask datastreamTask = new DatastreamTaskImpl();
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    connector.start(null);
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertTrue(connector.getCreateTaskCalled() >= 3);

    // Verify metric for nanny task restarts get incremented
    Gauge<Long> metric = DynamicMetricsManager.getInstance()
        .getMetric("test" + "." + TestKafkaConnector.class.getSimpleName() + "." + "numTaskRestarts");
    Assert.assertNotNull(metric);
    Assert.assertEquals(metric.getValue(), Long.valueOf(1));

    // Verify that metrics created through DynamicMetricsManager match those returned by getMetricInfos() given the
    // connector name of interest.
    MetricsTestUtils.verifyMetrics(connector, DynamicMetricsManager.getInstance(), s -> s.startsWith("test"));

    connector.stop();
  }

  @Test
  public void testOnAssignmentChangeReassignment() {
    Properties props = new Properties();
    // Reduce time interval between calls to restartDeadTasks to force invocation of stopTasks
    props.setProperty("daemonThreadIntervalInSeconds", "2");
    // With failStopTaskOnce set to true the AbstractKafkaBasedConnectorTask.stop is configured
    // to fail the first time with InterruptedException and pass the second time.
    TestKafkaConnector connector = new TestKafkaConnector(false, props, true, false);
    DatastreamTaskImpl datastreamTask1 = new DatastreamTaskImpl();
    datastreamTask1.setTaskPrefix("testtask1");
    connector.onAssignmentChange(Collections.singletonList(datastreamTask1));
    connector.start(null);

    DatastreamTaskImpl datastreamTask2 = new DatastreamTaskImpl();
    datastreamTask2.setTaskPrefix("testtask2");
    // AbstractKafkaBasedConnectorTask stop should fail on this onAssignmentChange call
    connector.onAssignmentChange(Collections.singletonList(datastreamTask2));
    Assert.assertEquals(connector.getTasksToStopCount(), 1);
    ArrayList<DatastreamTask> taskList = new ArrayList<>();
    taskList.add(datastreamTask1);
    taskList.add(datastreamTask2);
    connector.onAssignmentChange(Collections.unmodifiableList(taskList));
    Assert.assertEquals(connector.getTasksToStopCount(), 0);
    // Wait for restartDeadTasks to be called to attempt another stopTasks call
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertEquals(connector.getRunningTasksCount(), 2);
    connector.stop();
  }

  @Test
  public void testOnAssignmentChangeStopTaskFailure() {
    Properties props = new Properties();
    // Reduce time interval between calls to restartDeadTasks to force invocation of stopTasks
    props.setProperty("daemonThreadIntervalInSeconds", "2");
    // With failStopTaskOnce set to true the AbstractKafkaBasedConnectorTask.stop is configured
    // to fail the first time with InterruptedException and pass the second time.
    TestKafkaConnector connector = new TestKafkaConnector(false, props, true, false);
    DatastreamTaskImpl datastreamTask = new DatastreamTaskImpl();
    datastreamTask.setTaskPrefix("testtask1");
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    connector.start(null);

    datastreamTask = new DatastreamTaskImpl();
    datastreamTask.setTaskPrefix("testtask2");
    // AbstractKafkaBasedConnectorTask stop should fail on this onAssignmentChange call
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    Assert.assertEquals(connector.getTasksToStopCount(), 1);
    // Wait for restartDeadTasks to be called to attempt another stopTasks call
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertEquals(connector.getTasksToStopCount(), 0);

    Counter counter = DynamicMetricsManager.getInstance()
        .getMetric("test" + "." + TestKafkaConnector.class.getSimpleName() + "." + "numTaskStopFailures");
    Assert.assertNotNull(counter);
    Assert.assertTrue(counter.getCount() >= 1);

    connector.stop();
  }

  @Test
  public void testOnAssignmentChangeMultipleReassignments() throws InterruptedException {
    Properties props = new Properties();
    // Reduce time interval between calls to restartDeadTasks to force invocation of stopTasks
    props.setProperty("daemonThreadIntervalInSeconds", "2");
    // With failStopTaskOnce set to true the AbstractKafkaBasedConnectorTask.stop is configured
    // to fail the first time with InterruptedException and pass the second time.
    TestKafkaConnector connector = new TestKafkaConnector(false, props, true);

    // first task assignment assigns task 1
    List<DatastreamTask> firstTaskAssignment = getTaskListInRange(1, 2);
    connector.onAssignmentChange(firstTaskAssignment);
    connector.start(null);
    Assert.assertEquals(connector.getRunningTasksCount(), 1);

    // second task assignment assigns task 2,3,4,5 and takes out task 1
    List<DatastreamTask> secondTaskAssignment = getTaskListInRange(2, 6);

    // during the assignment, the _taskToStop map count need to be less than 1, as only task 1 would be taken out.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(() -> connector.onAssignmentChange(secondTaskAssignment));
    executor.execute(() -> Assert.assertTrue(connector.getTasksToStopCount() <= 1));

    awaitForExecution(executor, 50L);
    Assert.assertTrue(connector.getTasksToStopCount() >= 1); // the count of the _taskToStopTracker
    Assert.assertEquals(connector.getRunningTasksCount(), 4);

    // second task assignment keeps task 5, assigns task 6,7,8 and takes out task 2,3,4
    List<DatastreamTask> thirdTaskAssignment = getTaskListInRange(5, 9);

    // during the assignment, the _taskToStop map count need to be less than 4, as task 2,3,4 would be taken out and task 1 if not already stopped.
    executor = Executors.newFixedThreadPool(2);
    executor.execute(() -> connector.onAssignmentChange(thirdTaskAssignment));
    executor.execute(() -> Assert.assertTrue(connector.getTasksToStopCount() <= 4));

    awaitForExecution(executor, 50L);
    Assert.assertTrue(connector.getTasksToStopCount() >= 3); // the count of the _taskToStopTracker

    // Wait for restartDeadTasks to be called to attempt another stopTasks call
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertEquals(connector.getRunningTasksCount(), 4);
    connector.stop();
  }

  @Test
  public void testCalculateThreadStartDelay() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "30");
    TestKafkaConnector connector = new TestKafkaConnector(false, props, false);

    // A delay of 5 minutes means that the delay should occur every 5 minutes after the hour (i.e. it should fire at
    // 6:00, 6:05, 6:10, 6:15, etc. but should not fire at 6:07)
    Duration threadDelay = Duration.ofMinutes(5);

    // The value of min initial delay means that we must wait at least this long in all circumstances before firing
    Duration minDelay = AbstractKafkaConnector.MIN_DAEMON_THREAD_STARTUP_DELAY;

    // Period to test
    Duration testPeriod = Duration.ofDays(1).plusHours(2); // This should be long enough to handle all edge cases
    OffsetDateTime start = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
    OffsetDateTime end = start.plus(testPeriod);

    // Calculate expected and actual run times over our test period for every second
    for (OffsetDateTime now = start; now.isBefore(end); now = now.plusSeconds(1)) {
      OffsetDateTime currentTime = now;
      OffsetDateTime expectedTime = Stream.iterate(now.truncatedTo(ChronoUnit.HOURS), time -> time.plus(threadDelay))
          .filter(runTime -> runTime.isAfter(currentTime) && !runTime.isBefore(currentTime.plus(minDelay)))
          .findFirst()
          .orElseThrow(() -> new RuntimeException(String.format("Failed to find next expected thread runtime with "
                  + "base time of %s, thread delay %s, min delay %s, and current time %s", start, threadDelay, minDelay,
              currentTime)))
          .truncatedTo(ChronoUnit.SECONDS);

      long actualDelaySeconds = connector.getThreadDelayTimeInSecond(now, (int) threadDelay.getSeconds());
      OffsetDateTime actualTime = now.plusSeconds(actualDelaySeconds);

      Assert.assertEquals(actualTime, expectedTime, String.format("Actual = %s, Expected = %s, with base time of %s, "
              + "thread delay %s, min delay %s, and current time %s", actualTime, expectedTime, start, threadDelay,
          minDelay, now));
    }
  }

  @Test
  public void testRestartThrowsException() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "1");
    TestKafkaConnector connector = new TestKafkaConnector(true, props, false);
    DatastreamTask datastreamTask = new DatastreamTaskImpl();
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    connector.start(null);
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertTrue(connector.getCreateTaskCalled() >= 3);

    connector.stop();
  }

  // helper method to generate the tasks in a range for assignment
  private List<DatastreamTask> getTaskListInRange(int start, int end) {
    List<DatastreamTask> taskAssignmentList = new ArrayList<>();
    IntStream.range(start, end).forEach(index -> {
      DatastreamTaskImpl dt = new DatastreamTaskImpl();
      dt.setTaskPrefix("testtask" + index);
      taskAssignmentList.add(dt);
    });
    return taskAssignmentList;
  }

  // helper method to await on the executor for the given timeout period
  private void awaitForExecution(ExecutorService executor, Long timeUnitMs) throws InterruptedException {
    try {
      executor.awaitTermination(timeUnitMs, TimeUnit.MILLISECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Dummy implementation of {@link AbstractKafkaConnector} for testing purposes
   */
  public class TestKafkaConnector extends AbstractKafkaConnector {
    private boolean _restartThrows;
    private boolean _failStopTaskOnce;
    private int _createTaskCalled = 0;
    private int _stopTaskCalled = 0;
    private boolean _taskThreadDead = true;

    /**
     * Constructor for TestKafkaConnector
     * @param restartThrows Indicates whether calling {@link #restartDeadTasks()}
     *                      for the first time should throw a {@link RuntimeException}
     * @param props Configuration properties to use
     * @param failStopTaskOnce Fails Stopping task once
     * @param taskThreadDead Mocks if the task thread is dead or alive
     */
    public TestKafkaConnector(boolean restartThrows, Properties props, boolean failStopTaskOnce, boolean taskThreadDead) {
      super("test", props, new KafkaGroupIdConstructor(
          Boolean.parseBoolean(props.getProperty(IS_GROUP_ID_HASHING_ENABLED, Boolean.FALSE.toString())),
          "TestkafkaConnectorCluster"), "TestkafkaConnectorCluster", LOG);
      _restartThrows = restartThrows;
      _failStopTaskOnce = failStopTaskOnce;
      _taskThreadDead = taskThreadDead;
    }

    /**
     * Constructor for TestKafkaConnector
     * @param restartThrows Indicates whether calling {@link #restartDeadTasks()}
     *                      for the first time should throw a {@link RuntimeException}
     * @param props Configuration properties to use
     * @param failStopTaskOnce Fails Stopping task once
     */
    public TestKafkaConnector(boolean restartThrows, Properties props, boolean failStopTaskOnce) {
      this(restartThrows, props, failStopTaskOnce, true);
    }

    @Override
    protected AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task) {
      _createTaskCalled++;
      AbstractKafkaBasedConnectorTask connectorTask = mock(AbstractKafkaBasedConnectorTask.class);
      try {
        when(connectorTask.awaitStop(anyLong(), anyObject())).thenReturn(true);
        if (_failStopTaskOnce) {
          doThrow(InterruptedException.class).doNothing().when(connectorTask).stop();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return connectorTask;
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
        throws DatastreamValidationException {
    }

    @Override
    protected boolean isConnectorTaskDead(ConnectorTaskEntry connectorTaskEntry) {
      return true;
    }

    @Override
    protected boolean isTaskThreadDead(ConnectorTaskEntry connectorTaskEntry) {
      return _taskThreadDead;
    }

    @Override
    protected void restartDeadTasks() {
      if (_restartThrows) {
        _restartThrows = false;
        throw new RuntimeException();
      }
      super.restartDeadTasks();
    }

    public int getCreateTaskCalled() {
      return _createTaskCalled;
    }

    public int getStopTaskCalled() {
      return _stopTaskCalled;
    }
  }
}
