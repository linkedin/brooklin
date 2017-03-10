package com.linkedin.datastream.tools;

public class OptionConstants {

  public static final String OPT_SHORT_MGMT_URI = "u";
  public static final String OPT_LONG_MGMT_URI = "uri";
  public static final String OPT_ARG_MGMT_URI = "MANAGEMENT_URI";
  public static final String OPT_DESC_MGMT_URI = "Management service rest endpoint uri";

  public static final String OPT_SHORT_OPERATION = "o";
  public static final String OPT_LONG_OPERATION = "operation";
  public static final String OPT_ARG_OPERATION = "DATASTREAM_OPERATION";
  public static final String OPT_DESC_OPERATION = "Operation to perform accepted values [CREATE, READ, DELETE, READALL]";

  public static final String OPT_SHORT_DATASTREAM_NAME = "n";
  public static final String OPT_LONG_DATASTREAM_NAME = "name";
  public static final String OPT_ARG_DATASTREAM_NAME = "DATASTREAM_NAME";
  public static final String OPT_DESC_DATASTREAM_NAME = "Name of the datastream";

  public static final String OPT_SHORT_TRANSPORT_NAME = "t";
  public static final String OPT_LONG_TRANSPORT_NAME = "transport";
  public static final String OPT_ARG_TRANSPORT_NAME = "TRANSPORT_NAME";
  public static final String OPT_DESC_TRANSPORT_NAME = "Name of the Datastream Transport to use, default kafka.";

  public static final String OPT_SHORT_DESTINATION_URI = "d";
  public static final String OPT_LONG_DESTINATION_URI = "destination";
  public static final String OPT_ARG_DESTINATION_URI = "DESTINATION_URI";
  public static final String OPT_DESC_DESTINATION_URI = "Datastream destination uri";

  public static final String OPT_SHORT_DESTINATION_PARTITIONS = "dp";
  public static final String OPT_LONG_DESTINATION_PARTITIONS = "destinationpartitions";
  public static final String OPT_ARG_DESTINATION_PARTITIONS = "DESTINATION_PARTITIONS";
  public static final String OPT_DESC_DESTINATION_PARTITIONS = "Number of partitions in the destination";

  public static final String OPT_SHORT_CONNECTOR_NAME = "c";
  public static final String OPT_LONG_CONNECTOR_NAME = "connector";
  public static final String OPT_ARG_CONNECTOR_NAME = "CONNECTOR_NAME";
  public static final String OPT_DESC_CONNECTOR_NAME = "Name of the connector";

  public static final String OPT_SHORT_SOURCE_URI = "s";
  public static final String OPT_LONG_SOURCE_URI = "source";
  public static final String OPT_ARG_SOURCE_URI = "SOURCE_URI";
  public static final String OPT_DESC_SOURCE_URI = "Datastream source uri";

  public static final String OPT_SHORT_METADATA = "m";
  public static final String OPT_LONG_METADATA = "metadata";
  public static final String OPT_ARG_METADATA = "DATASTREAM_METADATA";
  public static final String OPT_DESC_METADATA = "Datastream metadata key value pairs represented as json "
      + "{\"key1\":\"value1\",\"key2\":\"value2\"}";

  public static final String OPT_SHORT_NUM_PARTITION = "p";
  public static final String OPT_LONG_NUM_PARTITION = "partitions";
  public static final String OPT_ARG_NUM_PARTITION = "NUM_PARTITIONS";
  public static final String OPT_DESC_NUM_PARTITION = "Number of partitions in the source";

  public static final String OPT_SHORT_HELP = "h";
  public static final String OPT_LONG_HELP = "help";
  public static final String OPT_DESC_HELP = "Display this message";
}
