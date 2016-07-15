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
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;


public class DatastreamRestClientCli {

  private DatastreamRestClientCli() {
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("DatastreamRestClientCmd",
        "Console app to manage datastreams. Available operations: read, readall, "
            + "create, delete Example usage: ./datastream-rest-client.sh read --dms localhost:32211 -n datastream -e",
        options, "", true);
  }

  private enum Operation {
    CREATE,
    READ,
    DELETE,
    READALL
  }

  private static void printDatastreams(List<Datastream> streams) {
    streams.stream().forEach(s -> System.out.println(JsonUtils.toJson(s)));
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionUtils.createOption(OptionConstants.OPT_SHORT_MGMT_URI, OptionConstants.OPT_LONG_MGMT_URI,
        OptionConstants.OPT_ARG_MGMT_URI, true, OptionConstants.OPT_DESC_MGMT_URI));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_DATASTREAM_NAME, OptionConstants.OPT_LONG_DATASTREAM_NAME,
            OptionConstants.OPT_ARG_DATASTREAM_NAME, true, OptionConstants.OPT_DESC_DATASTREAM_NAME));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_NUM_PARTITION, OptionConstants.OPT_LONG_NUM_PARTITION,
            OptionConstants.OPT_ARG_NUM_PARTITION, true, OptionConstants.OPT_DESC_NUM_PARTITION));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_CONNECTOR_NAME, OptionConstants.OPT_LONG_CONNECTOR_NAME,
            OptionConstants.OPT_ARG_CONNECTOR_NAME, true, OptionConstants.OPT_DESC_CONNECTOR_NAME));
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_SOURCE_URI, OptionConstants.OPT_LONG_SOURCE_URI,
            OptionConstants.OPT_ARG_SOURCE_URI, true, OptionConstants.OPT_DESC_SOURCE_URI));

    options.addOption(OptionUtils.createOption(OptionConstants.OPT_SHORT_METADATA, OptionConstants.OPT_LONG_METADATA,
        OptionConstants.OPT_ARG_METADATA, false, OptionConstants.OPT_DESC_METADATA));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_HELP, OptionConstants.OPT_LONG_HELP, null, false,
            OptionConstants.OPT_DESC_HELP));

    if (args.length == 0) {
      printHelp(options);
      return;
    }

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

    if (cmd.getArgs().length == 0) {
      System.out.println("Missing operation: choose from create, read, readall, delete");
      return;
    }

    Operation op = Operation.valueOf(cmd.getArgs()[0].toUpperCase());

    String dmsUri = cmd.getOptionValue(OptionConstants.OPT_SHORT_MGMT_URI);
    String datastreamName = cmd.getOptionValue(OptionConstants.OPT_SHORT_DATASTREAM_NAME);
    DatastreamRestClient datastreamRestClient = null;
    try {
      datastreamRestClient = new DatastreamRestClient(dmsUri);
      switch (op) {
        case READ:
          Datastream stream = datastreamRestClient.getDatastream(datastreamName);
          printDatastreams(Collections.singletonList(stream));
          return;
        case READALL:
          printDatastreams(datastreamRestClient.getAllDatastreams());
          return;
        case DELETE:
          datastreamRestClient.deleteDatastream(datastreamName);
          System.out.println("Success");
          return;
        case CREATE:
          String sourceUri = cmd.getOptionValue(OptionConstants.OPT_SHORT_SOURCE_URI);
          String connectorName = cmd.getOptionValue(OptionConstants.OPT_SHORT_CONNECTOR_NAME);
          int partitions = Integer.parseInt(cmd.getOptionValue(OptionConstants.OPT_SHORT_NUM_PARTITION));
          Map<String, String> metadata = new HashMap<>();
          if (cmd.hasOption(OptionConstants.OPT_SHORT_METADATA)) {
            metadata = JsonUtils.fromJson(cmd.getOptionValue(OptionConstants.OPT_SHORT_METADATA),
                new TypeReference<Map<String, String>>() {
                });
          }

          Datastream datastream = new Datastream();
          datastream.setName(datastreamName);
          datastream.setConnectorName(connectorName);
          DatastreamSource datastreamSource = new DatastreamSource();
          datastreamSource.setConnectionString(sourceUri);
          datastreamSource.setPartitions(partitions);
          datastream.setSource(datastreamSource);
          datastream.setMetadata(new StringMap(metadata));
          System.out.printf("Trying to create datastream %s", datastream);
          datastreamRestClient.createDatastream(datastream);
          System.out.printf("Created %s datastream. Now waiting for initialization (timeout = 10 seconds)\n",
              connectorName);
          Datastream completeDatastream = datastreamRestClient.waitTillDatastreamIsInitialized(datastreamName,
              (int) Duration.ofSeconds(10).toMillis());
          System.out.printf("Initialized %s datastream: %s\n", connectorName, completeDatastream);
          break;
        default:
          // do nothing
      }
    } finally {
      if (datastreamRestClient != null) {
        datastreamRestClient.shutdown();
      }
    }
  }
}
