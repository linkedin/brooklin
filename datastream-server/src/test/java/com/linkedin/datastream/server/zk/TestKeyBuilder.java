package com.linkedin.datastream.server.zk;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestKeyBuilder {
    @Test
    public void testDatastreamTaskStatePath() throws Exception {
        String cluster = "testDatastreamTaskStatePath";
        String connectorType = "connectorType";
        String datastreamName = "datastream1";
        String taskId = "0";
        String expected = String.format("/%s/%s/%s/%s/state", cluster, connectorType, datastreamName, taskId);

        //
        // for the case when taskId is not empty
        //
        String path = KeyBuilder.datastreamTaskState(cluster, connectorType, datastreamName, taskId);
        Assert.assertTrue(path.equals(expected));

        //
        // for the case when taskId is empty
        //
        taskId = "";
        expected= String.format("/%s/%s/%s/state", cluster, connectorType, datastreamName);
        path = KeyBuilder.datastreamTaskState(cluster, connectorType, datastreamName, taskId);
        Assert.assertTrue(path.equals(expected));
    }
}