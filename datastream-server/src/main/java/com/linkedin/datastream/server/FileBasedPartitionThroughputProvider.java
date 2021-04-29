package com.linkedin.datastream.server;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class FileBasedPartitionThroughputProvider implements PartitionThroughputProvider {
  private static final String RESOURCE_FILE_NAME = "partitionThroughput.json";
  private static final String ROOT_NODE_NAME = "stats";

  @Override
  public ClusterThroughputInfo getThroughputInfo(String clusterName) {
    URL resource = getClass().getClassLoader().getResource(RESOURCE_FILE_NAME);
    File partitionThroughputFile = null;
    if (resource == null) {
      throw new IllegalArgumentException("File not found.");
    }

    try {
      partitionThroughputFile = new File(resource.toURI());
    } catch(URISyntaxException ex) {
      throw new IllegalArgumentException("Failed to construct URI for the input file");
    }

    return reatThroughputInfoFromFile(partitionThroughputFile, clusterName);
  }

  private ClusterThroughputInfo reatThroughputInfoFromFile(File file, String clusterName) {
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, PartitionThroughputInfo> partitionInfoMap = new HashMap<>();

    try {
      JsonNode root = mapper.readTree(file);
      JsonNode allStats = root.get(ROOT_NODE_NAME);
      JsonNode clusterStats = allStats.get(clusterName);

      if (clusterStats == null) {
        throw new IllegalArgumentException("Throughput info for cluster" + clusterName + "not found.");
      }

      TypeReference<HashMap<String, String>> mapTypeRef = new TypeReference<HashMap<String, String>>() {};
      HashMap<String, String> partitionStats = mapper.readValue(clusterStats, mapTypeRef);
      for(String partition : partitionStats.keySet()) {
        String value = partitionStats.get(partition);
        String[] tokens = StringUtils.split(value,",");
        Long bytesInRate = Long.parseLong(StringUtils.substring(tokens[0], 11));
        Long messagesInRate = Long.parseLong(StringUtils.substring(tokens[1], 7));
        partitionInfoMap.put(partition, new PartitionThroughputInfo(bytesInRate, messagesInRate));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return new ClusterThroughputInfo("metrics", partitionInfoMap);
  }
}
