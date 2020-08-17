/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import com.codahale.metrics.Gauge;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    TestKafkaConnector connector = new TestKafkaConnector(false, props);
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
    MetricsTestUtils.verifyMetrics(connector, DynamicMetricsManager.getInstance());

    connector.stop();
  }


  @Test
  public void testCalculateThreadStartDelay() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "30");
    TestKafkaConnector connector = new TestKafkaConnector(false, props);

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
    TestKafkaConnector connector = new TestKafkaConnector(true, props);
    DatastreamTask datastreamTask = new DatastreamTaskImpl();
    connector.onAssignmentChange(Collections.singletonList(datastreamTask));
    connector.start(null);
    PollUtils.poll(() -> connector.getCreateTaskCalled() >= 3, Duration.ofSeconds(1).toMillis(),
        Duration.ofSeconds(10).toMillis());
    Assert.assertTrue(connector.getCreateTaskCalled() >= 3);

    connector.stop();
  }

  /**
   * Dummy implementation of {@link AbstractKafkaConnector} for testing purposes
   */
  public class TestKafkaConnector extends AbstractKafkaConnector {
    private boolean _restartThrows;
    private int _createTaskCalled = 0;

    /**
     * Constructor for TestKafkaConnector
     * @param restartThrows Indicates whether calling {@link #restartDeadTasks()}
     *                      for the first time should throw a {@link RuntimeException}
     * @param props Configuration properties to use
     */
    public TestKafkaConnector(boolean restartThrows, Properties props) {
      super("test", props, new KafkaGroupIdConstructor(
          Boolean.parseBoolean(props.getProperty(IS_GROUP_ID_HASHING_ENABLED, Boolean.FALSE.toString())),
          "TestkafkaConnectorCluster"), "TestkafkaConnectorCluster", LOG);
      _restartThrows = restartThrows;
    }

    @Override
    protected AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task) {
      _createTaskCalled++;
      AbstractKafkaBasedConnectorTask connectorTask = mock(AbstractKafkaBasedConnectorTask.class);
      try {
        when(connectorTask.awaitStop(anyLong(), anyObject())).thenReturn(true);
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
    protected boolean isTaskDead(ConnectorTaskEntry connectorTaskEntry) {
      return true;
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
  }
}
