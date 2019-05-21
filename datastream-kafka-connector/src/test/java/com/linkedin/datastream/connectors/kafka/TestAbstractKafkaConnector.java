/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link AbstractKafkaConnector}
 */
public class TestAbstractKafkaConnector {

  private static final Logger LOG = LoggerFactory.getLogger(TestAbstractKafkaConnector.class);

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

    connector.stop();
  }


  @Test
  public void testCalculateThreadStartDelay() {
    Properties props = new Properties();
    props.setProperty("daemonThreadIntervalInSeconds", "30");
    TestKafkaConnector connector = new TestKafkaConnector(false, props);
    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
    long fireDelay = connector.getThreadDelayTimeInSecond(300);

    boolean found = false;
    for (int i = 0; i <= 3600 / 300; ++i) {
      OffsetDateTime expectedFireTime = now.truncatedTo(ChronoUnit.HOURS).plusMinutes(5 * i);
      if (Duration.between(expectedFireTime, now.plusSeconds(fireDelay)).abs().getSeconds() < 10) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
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
     * @param restartThrows Indicates whether calling {@link #restartIfNotRunning(DatastreamTask)}
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
    protected boolean isTaskRunning(DatastreamTask datastreamTask) {
      return false;
    }

    @Override
    protected void restartIfNotRunning(DatastreamTask task) {
      if (_restartThrows) {
        _restartThrows = false;
        throw new RuntimeException();
      }
      super.restartIfNotRunning(task);
    }

    public int getCreateTaskCalled() {
      return _createTaskCalled;
    }
  }
}
