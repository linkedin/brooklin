/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.PartitionThroughputInfo;


/**
 * An implementation of {@link PartitionThroughputProvider} which reads throughput data from a resource file.
 * Used for testing purposes.
 */
public class FileBasedPartitionThroughputProvider implements PartitionThroughputProvider {
  private static final String ROOT_NODE_NAME = "stats";
  private static final String MESSAGES_IN_RATE_TOKEN = "msgIn:";
  private static final String BYTES_IN_RATE_TOKEN = "bytesInKb:";

  private final String _fileName;

  /**
   * Creates an instance of {@link FileBasedPartitionThroughputProvider}
   * @param fileName Resource file name
   */
  public FileBasedPartitionThroughputProvider(String fileName) {
    _fileName = fileName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClusterThroughputInfo getThroughputInfo(String clusterName) {
    File partitionThroughputFile = getThroughputFileFromResources();
    return readThroughputInfoFromFile(partitionThroughputFile, clusterName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashMap<String, ClusterThroughputInfo> getThroughputInfo() {
    File partitionThroughputFile = getThroughputFileFromResources();
    return readThroughputInfoFromFile(partitionThroughputFile);
  }

  private File getThroughputFileFromResources() {
    URL resource = getClass().getClassLoader().getResource(_fileName);
    File partitionThroughputFile = null;
    if (resource == null) {
      throw new IllegalArgumentException("File not found.");
    }

    try {
      partitionThroughputFile = new File(resource.toURI());
    } catch (URISyntaxException ex) {
      throw new IllegalArgumentException("Failed to construct URI for the input file");
    }
    return partitionThroughputFile;
  }

  private HashMap<String, ClusterThroughputInfo> readThroughputInfoFromFile(File file) {
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, ClusterThroughputInfo> clusterInfoMap = new HashMap<>();

    try {
      JsonNode root = mapper.readTree(file);
      JsonNode allStats = root.get(ROOT_NODE_NAME);
      Iterator<String> clusterNames = allStats.getFieldNames();

      while (clusterNames.hasNext()) {
        String key = clusterNames.next();
        clusterInfoMap.put(key, getClusterThroughputInfoFromNode(mapper, key, allStats.get(key)));
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return clusterInfoMap;
  }

  private ClusterThroughputInfo readThroughputInfoFromFile(File file, String clusterName) {
    ObjectMapper mapper = new ObjectMapper();
    ClusterThroughputInfo clusterInfo = null;

    try {
      JsonNode root = mapper.readTree(file);
      JsonNode allStats = root.get(ROOT_NODE_NAME);
      JsonNode clusterStats = allStats.get(clusterName);

      if (clusterStats == null) {
        throw new IllegalArgumentException("Throughput info for cluster" + clusterName + "not found.");
      }

      clusterInfo = getClusterThroughputInfoFromNode(mapper, clusterName, clusterStats);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return clusterInfo;
  }

  private ClusterThroughputInfo getClusterThroughputInfoFromNode(ObjectMapper mapper, String clusterName,
      JsonNode clusterStats) {
    TypeReference<HashMap<String, String>> mapTypeRef = new TypeReference<HashMap<String, String>>() { };
    HashMap<String, PartitionThroughputInfo> partitionInfoMap = new HashMap<>();

    try {
      HashMap<String, String> partitionStats = mapper.readValue(clusterStats, mapTypeRef);
      for (String partition : partitionStats.keySet()) {
        String value = partitionStats.get(partition);
        String[] tokens = StringUtils.split(value, ",");
        int bytesInRate = Integer.parseInt(StringUtils.substring(tokens[0], BYTES_IN_RATE_TOKEN.length() + 1));
        int messagesInRate = Integer.parseInt(StringUtils.substring(tokens[1], MESSAGES_IN_RATE_TOKEN.length() + 1));
        partitionInfoMap.put(partition, new PartitionThroughputInfo(bytesInRate, messagesInRate, partition));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new ClusterThroughputInfo(clusterName, partitionInfoMap);
  }
}
