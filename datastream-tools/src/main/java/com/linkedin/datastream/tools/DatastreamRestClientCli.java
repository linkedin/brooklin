package com.linkedin.datastream.tools;

/*
 * Copyright 2016 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;


public class DatastreamRestClientCli {

  private DatastreamRestClientCli() {
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("DatastreamRestClientCmd", "Console app to manage datastreams.", options, "", true);
  }

  private enum Operation {
    CREATE,
    READ,
    DELETE,
    READALL
  }

  private static void printDatastreams(List<Datastream> streams) {
    ObjectMapper mapper = new ObjectMapper();

    streams.stream().forEach(s -> {
      try {
        System.out.println(mapper.defaultPrettyPrintingWriter().writeValueAsString(s));
      } catch (IOException e) {
        throw new DatastreamRuntimeException(e);
      }
    });
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionUtils.createOption(OptionConstants.OPT_SHORT_OPERATION, OptionConstants.OPT_LONG_OPERATION,
        OptionConstants.OPT_ARG_OPERATION, true, OptionConstants.OPT_DESC_OPERATION));
    options.addOption(OptionUtils.createOption(OptionConstants.OPT_SHORT_MGMT_URI, OptionConstants.OPT_LONG_MGMT_URI,
        OptionConstants.OPT_ARG_MGMT_URI, true, OptionConstants.OPT_DESC_MGMT_URI));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_DATASTREAM_NAME, OptionConstants.OPT_LONG_DATASTREAM_NAME,
            OptionConstants.OPT_ARG_DATASTREAM_NAME, false, OptionConstants.OPT_DESC_DATASTREAM_NAME));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_NUM_PARTITION, OptionConstants.OPT_LONG_NUM_PARTITION,
            OptionConstants.OPT_ARG_NUM_PARTITION, false, OptionConstants.OPT_DESC_NUM_PARTITION));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_CONNECTOR_NAME, OptionConstants.OPT_LONG_CONNECTOR_NAME,
            OptionConstants.OPT_ARG_CONNECTOR_NAME, false, OptionConstants.OPT_DESC_CONNECTOR_NAME));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_SOURCE_URI, OptionConstants.OPT_LONG_SOURCE_URI,
            OptionConstants.OPT_ARG_SOURCE_URI, false, OptionConstants.OPT_DESC_SOURCE_URI));

    options.addOption(OptionUtils.createOption(OptionConstants.OPT_SHORT_METADATA, OptionConstants.OPT_LONG_METADATA,
        OptionConstants.OPT_ARG_METADATA, false, OptionConstants.OPT_DESC_METADATA));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_HELP, OptionConstants.OPT_LONG_HELP, null, false,
            OptionConstants.OPT_DESC_HELP));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_TRANSPORT_NAME, OptionConstants.OPT_LONG_TRANSPORT_NAME,
            OptionConstants.OPT_ARG_TRANSPORT_NAME, false, OptionConstants.OPT_DESC_TRANSPORT_NAME));

    CommandLineParser parser = new BasicParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.out.println("Failed to parse the arguments. " + e.getMessage());
      printHelp(options);
      return;
    }

    if (cmd.hasOption(OptionConstants.OPT_SHORT_HELP)) {
      printHelp(options);
      return;
    }

    Operation op = Operation.valueOf(cmd.getOptionValue(OptionConstants.OPT_SHORT_OPERATION).toUpperCase());
    String dmsUri = cmd.getOptionValue(OptionConstants.OPT_SHORT_MGMT_URI);
    DatastreamRestClient datastreamRestClient = null;
    try {
      datastreamRestClient = new DatastreamRestClient(dmsUri);
      switch (op) {
        case READ: {
          String datastreamName = getOptionValue(cmd, OptionConstants.OPT_SHORT_DATASTREAM_NAME, options);
          Datastream stream = datastreamRestClient.getDatastream(datastreamName);
          printDatastreams(Collections.singletonList(stream));
          return;
        }
        case READALL:
          printDatastreams(datastreamRestClient.getAllDatastreams());
          return;
        case DELETE: {
          String datastreamName = getOptionValue(cmd, OptionConstants.OPT_SHORT_DATASTREAM_NAME, options);
          datastreamRestClient.deleteDatastream(datastreamName);
          System.out.println("Success");
          return;
        }
        case CREATE: {
          String datastreamName = getOptionValue(cmd, OptionConstants.OPT_SHORT_DATASTREAM_NAME, options);
          String sourceUri = getOptionValue(cmd, OptionConstants.OPT_SHORT_SOURCE_URI, options);
          String connectorName = getOptionValue(cmd, OptionConstants.OPT_SHORT_CONNECTOR_NAME, options);
          int partitions = Integer.parseInt(getOptionValue(cmd, OptionConstants.OPT_SHORT_NUM_PARTITION, options));
          Map<String, String> metadata = new HashMap<>();
          String transport = "kafka"; // default value.
          if (cmd.hasOption(OptionConstants.OPT_SHORT_METADATA)) {
            metadata = JsonUtils.fromJson(getOptionValue(cmd, OptionConstants.OPT_SHORT_METADATA, options),
                new TypeReference<Map<String, String>>() {
                });
          }
          if (cmd.hasOption(OptionConstants.OPT_SHORT_TRANSPORT_NAME)) {
            transport = getOptionValue(cmd, OptionConstants.OPT_SHORT_TRANSPORT_NAME, options);
          }

          Datastream datastream = new Datastream();
          datastream.setName(datastreamName);
          datastream.setConnectorName(connectorName);
          DatastreamSource datastreamSource = new DatastreamSource();
          datastreamSource.setConnectionString(sourceUri);
          datastreamSource.setPartitions(partitions);
          datastream.setSource(datastreamSource);
          datastream.setMetadata(new StringMap(metadata));
          datastream.setTransportProviderName(transport);
          System.out.printf("Trying to create datastream %s", datastream);
          datastreamRestClient.createDatastream(datastream);
          System.out.printf("Created %s datastream. Now waiting for initialization (timeout = 10 seconds)\n",
              connectorName);
          Datastream completeDatastream = datastreamRestClient.waitTillDatastreamIsInitialized(datastreamName,
              (int) Duration.ofSeconds(10).toMillis());
          System.out.printf("Initialized %s datastream: %s\n", connectorName, completeDatastream);
          break;
        }
        default:
          // do nothing
      }
    } finally {
      if (datastreamRestClient != null) {
        datastreamRestClient.shutdown();
      }
    }
  }

  private static String getOptionValue(CommandLine cmd, String optionName, Options options) {
    if (!cmd.hasOption(optionName)) {
      printHelp(options);
      throw new DatastreamRuntimeException(String.format("Required option: %s is not passed ", optionName));
    }

    return cmd.getOptionValue(optionName);
  }
}
