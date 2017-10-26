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
import java.util.Optional;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
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

  private static void printDatastreams(boolean noformat, List<Datastream> streams) {
    ObjectMapper mapper = new ObjectMapper();

    streams.stream().forEach(s -> {
      try {
        String jsonValue = DatastreamUtils.toJSON(s);
        if (!noformat) {
          Object json = mapper.readValue(jsonValue, Object.class);
          jsonValue = mapper.defaultPrettyPrintingWriter().writeValueAsString(json);
        }

        System.out.println(jsonValue);
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
    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_TRANSPORT_NAME, OptionConstants.OPT_LONG_TRANSPORT_NAME,
            OptionConstants.OPT_ARG_TRANSPORT_NAME, false, OptionConstants.OPT_DESC_TRANSPORT_NAME));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_KEY_SERDE_NAME, OptionConstants.OPT_LONG_KEY_SERDE_NAME,
            OptionConstants.OPT_ARG_KEY_SERDE_NAME, false, OptionConstants.OPT_DESC_KEY_SERDE_NAME));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_PAYLOAD_SERDE_NAME, OptionConstants.OPT_LONG_PAYLOAD_SERDE_NAME,
            OptionConstants.OPT_ARG_PAYLOAD_SERDE_NAME, false, OptionConstants.OPT_DESC_PAYLOAD_SERDE_NAME));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_ENVELOPE_SERDE_NAME, OptionConstants.OPT_LONG_ENVELOPE_SERDE_NAME,
            OptionConstants.OPT_ARG_ENVELOPE_SERDE_NAME, false, OptionConstants.OPT_DESC_ENVELOPE_SERDE_NAME));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_UNFORMATTED, OptionConstants.OPT_LONG_UNFORMATTED, null,
            false, OptionConstants.OPT_DESC_UNFORMATTED));

    options.addOption(
        OptionUtils.createOption(OptionConstants.OPT_SHORT_DESTINATION_URI, OptionConstants.OPT_LONG_DESTINATION_URI,
            OptionConstants.OPT_ARG_DESTINATION_URI, false, OptionConstants.OPT_DESC_DESTINATION_URI));

    options.addOption(OptionUtils.createOption(OptionConstants.OPT_SHORT_DESTINATION_PARTITIONS,
        OptionConstants.OPT_LONG_DESTINATION_PARTITIONS, OptionConstants.OPT_ARG_DESTINATION_PARTITIONS, false,
        OptionConstants.OPT_DESC_DESTINATION_PARTITIONS));

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

    boolean noformat = cmd.hasOption(OptionConstants.OPT_SHORT_UNFORMATTED);

    Operation op = Operation.valueOf(cmd.getOptionValue(OptionConstants.OPT_SHORT_OPERATION).toUpperCase());
    String dmsUri = cmd.getOptionValue(OptionConstants.OPT_SHORT_MGMT_URI);
    DatastreamRestClient datastreamRestClient = null;
    try {
      datastreamRestClient = DatastreamRestClientFactory.getClient(dmsUri);
      switch (op) {
        case READ: {
          String datastreamName = getOptionValue(cmd, OptionConstants.OPT_SHORT_DATASTREAM_NAME, options);
          Datastream stream = datastreamRestClient.getDatastream(datastreamName);
          printDatastreams(noformat, Collections.singletonList(stream));
          return;
        }
        case READALL:
          printDatastreams(noformat, datastreamRestClient.getAllDatastreams());
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

          String destinationUri = "";
          int numDestinationPartitions = -1;
          if (cmd.hasOption(OptionConstants.OPT_SHORT_DESTINATION_URI)) {
            destinationUri = cmd.getOptionValue(OptionConstants.OPT_SHORT_DESTINATION_URI);
            numDestinationPartitions =
                Integer.valueOf(cmd.getOptionValue(OptionConstants.OPT_SHORT_DESTINATION_PARTITIONS));
          }

          int partitions = Integer.parseInt(getOptionValue(cmd, OptionConstants.OPT_SHORT_NUM_PARTITION, options));
          Map<String, String> metadata = new HashMap<>();
          if (cmd.hasOption(OptionConstants.OPT_SHORT_METADATA)) {
            metadata = JsonUtils.fromJson(getOptionValue(cmd, OptionConstants.OPT_SHORT_METADATA, options),
                new TypeReference<Map<String, String>>() {
                });
          }

          Optional<String> keySerdeName =
              getOptionalOptionValue(cmd, OptionConstants.OPT_SHORT_KEY_SERDE_NAME, options);

          Optional<String> payloadSerdeName =
              getOptionalOptionValue(cmd, OptionConstants.OPT_SHORT_PAYLOAD_SERDE_NAME, options);
          Optional<String> envelopeSerdeName =
              getOptionalOptionValue(cmd, OptionConstants.OPT_SHORT_ENVELOPE_SERDE_NAME, options);
          String transportProviderName = getOptionValue(cmd, OptionConstants.OPT_SHORT_TRANSPORT_NAME, options);

          Duration timeout = Duration.ofMinutes(2);

          Datastream datastream = new Datastream();
          datastream.setName(datastreamName);
          datastream.setConnectorName(connectorName);
          DatastreamSource datastreamSource = new DatastreamSource();
          datastreamSource.setConnectionString(sourceUri);
          datastreamSource.setPartitions(partitions);
          datastream.setTransportProviderName(transportProviderName);
          DatastreamDestination destination = new DatastreamDestination();
          datastream.setDestination(destination);
          if (StringUtils.isNotEmpty(destinationUri)) {
            destination.setConnectionString(destinationUri);
            destination.setPartitions(numDestinationPartitions);
          }

          keySerdeName.ifPresent(x -> datastream.getDestination().setKeySerDe(x));
          payloadSerdeName.ifPresent(x -> datastream.getDestination().setPayloadSerDe(x));
          envelopeSerdeName.ifPresent(x -> datastream.getDestination().setEnvelopeSerDe(x));

          datastream.setSource(datastreamSource);
          datastream.setMetadata(new StringMap(metadata));
          System.out.printf("Trying to create datastream %s", datastream);
          datastreamRestClient.createDatastream(datastream);
          System.out.printf("Created %s datastream. Now waiting for initialization (timeout = %d minutes)\n",
              connectorName, timeout.toMinutes());
          Datastream completeDatastream =
              datastreamRestClient.waitTillDatastreamIsInitialized(datastreamName, (int) timeout.toMillis());
          System.out.printf("Initialized %s datastream: %s\n", connectorName, completeDatastream);
          break;
        }
        default:
          // do nothing
      }
    } catch (Exception e) {
      System.out.println(e.toString());
    } finally {
      System.exit(0);
    }
  }

  private static Optional<String> getOptionalOptionValue(CommandLine cmd, String optShortKeySerdeName, Options options) {
    Optional<String> value = Optional.empty();
    if (cmd.hasOption(optShortKeySerdeName)) {
      value = Optional.of(getOptionValue(cmd, optShortKeySerdeName, options));
    }

    return value;
  }

  private static String getOptionValue(CommandLine cmd, String optionName, Options options) {
    if (!cmd.hasOption(optionName)) {
      printHelp(options);
      throw new DatastreamRuntimeException(String.format("Required option: %s is not passed ", optionName));
    }

    return cmd.getOptionValue(optionName);
  }
}
