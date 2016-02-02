package com.linkedin.datastream.testutil.event.generator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GlobalSettings {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalSettings.class.getName());

  public String _datastreamName;
  public String _dbName;
  public String _tableName;
  public String _kafkaTopicName;

  public boolean isProducerDone;

  public String _schemataPath;
  public String _schemaFileName;

  public int _numPartitions;
  public int _maxTransactionSize;

  public int _numEvents;
  public String _percentData;

  public int _startResourceKey;

  public String _dataFileName;
  public boolean _generateDataFile;
  public boolean _generateDataFileOnly;

  public long _seed;

  public ProducerType _producerType = ProducerType.Datastream;

  public Mode _generationMode = Mode.Default;

  private int _numberOfErrors = 0;

  public enum Mode {
    Default,
    SizeConstrained
  }

  public enum ProducerType {
    Generic,
    Datastream,
    mySQL,
    Espresso
  }

  public synchronized void incrementErrorCount() {
    _numberOfErrors++;
  }

  public synchronized void incrementErrorCount(int errorCount) {
    _numberOfErrors += errorCount;
  }

  public int getErrorCount() {
    return _numberOfErrors;
  }

  public boolean parseCommandLineParameters(String[] args) {
    // Create command-line options
    Options options = new Options();
    Option datastreamName = OptionBuilder.withArgName("datastreamName").withLongOpt("datastreamName").hasArg()
        .withDescription("Name of datastream to generate events for.").isRequired(false).create("ds");
    Option dbName = OptionBuilder.withArgName("dbName").withLongOpt("dbName").hasArg()
        .withDescription("Name of database to generate events for.").isRequired(false).create("db");
    Option tableName = OptionBuilder.withArgName("tableName").withLongOpt("tableName").hasArg()
        .withDescription("Name of the table to generate events for.").isRequired(false).create("tb");
    Option kafkaTopicName = OptionBuilder.withArgName("kafkaTopicName").withLongOpt("kafkaTopicName").hasArg()
        .withDescription("Name of the topic to produce the events for.").isRequired(false).create("kt");
    Option schemataPath = OptionBuilder.withArgName("schemataPath").withLongOpt("schemataPath").hasArg()
        .withDescription("Directory where schema files are stored. Default: current directory").isRequired(false)
        .create("sp");
    Option schemaFileName = OptionBuilder.withArgName("schemaFileName").withLongOpt("schemaFileName").hasArg()
        .withDescription("Schema file name to read schema from. Default: none").isRequired().create("sf");
    Option numPartitions = OptionBuilder.withArgName("numPartitions").withLongOpt("numPartitions").hasArg()
        .withDescription("Number of partitions to generate. (optional - default: 1)").isRequired(false).create("np");
    Option maxTransactionSize =
        OptionBuilder.withArgName("maxTransactionSize").withLongOpt("maxTransactionSize").hasArg()
            .withDescription("Maximum number of events in a transaction. (optional - default: 1)").isRequired(false)
            .create("xs");
    Option numEvents = OptionBuilder.withArgName("numEvents").withLongOpt("numEvents").hasArg()
        .withDescription("Number of events to generate. (optional - default: 100)").isRequired(false).create("ne");
    Option percentData = OptionBuilder.withArgName("percentData").withLongOpt("percentData").hasArg().withDescription(
        "percentage of data going to be of type inserts, updates, deletes, controls in the list adding up to 100,"
            + "eg 50,30,20 if omitted only inserts are generated 100%")
        .isRequired(false).create("pd");

    Option startResourceKey = OptionBuilder.withArgName("startResourceKey").withLongOpt("startResourceKey").hasArg()
        .withDescription("Resource Key to start generating/validating. (optional - default: 1)").isRequired(false)
        .create("sr");
    Option dataFileName = OptionBuilder.withArgName("dataFileName").withLongOpt("dataFileName").hasArg()
        .withDescription("Data file name for tracing. Default: EventGeneratorTrace").isRequired(false).create("fn");
    Option generateDataFile =
        OptionBuilder.withArgName("generateDataFile").withLongOpt("generateDataFile").hasArg(false)
            .withDescription("Whether data file is generated in the working directory.").isRequired(false).create("df");
    Option generateDataFileOnly =
        OptionBuilder.withArgName("generateDataFileOnly").withLongOpt("generateDataFileOnly").hasArg(false)
            .withDescription("Only generate data. Do not execute. Default = False").isRequired(false).create("fo");
    Option seed = OptionBuilder.withArgName("seed").withLongOpt("seed").hasArg(false)
        .withDescription("Seed to use for generating data. Default = current time").isRequired(false).create("sd");

    // Add possible command-line options
    options.addOption(datastreamName);
    options.addOption(dbName);
    options.addOption(tableName);
    options.addOption(kafkaTopicName);
    options.addOption(schemataPath);
    options.addOption(schemaFileName);
    options.addOption(numPartitions);
    options.addOption(maxTransactionSize);
    options.addOption(numEvents);
    options.addOption(percentData);
    options.addOption(startResourceKey);
    options.addOption(dataFileName);
    options.addOption(generateDataFile);
    options.addOption(generateDataFileOnly);
    options.addOption(seed);

    // Parse the command line options
    CommandLineParser parser = new PosixParser();

    try {
      CommandLine commandLine = parser.parse(options, args);

      // read mandatory fields
      _datastreamName = commandLine.getOptionValue("ds", "test_ds");
      _dbName = commandLine.getOptionValue("db", "test_db");
      _tableName = commandLine.getOptionValue("tb", "test_table");
      _kafkaTopicName = commandLine.getOptionValue("kt", "es_test_db");

      _schemataPath = commandLine.getOptionValue("sp", ".");
      _schemaFileName = commandLine.getOptionValue("sf");

      _numPartitions = Integer.valueOf(commandLine.getOptionValue("np", "1"));
      _maxTransactionSize = Integer.valueOf(commandLine.getOptionValue("xs", "1"));
      _numEvents = Integer.valueOf(commandLine.getOptionValue("ne", "100"));
      _percentData = commandLine.getOptionValue("pd", "100,0,0,0"); // todo - a check that these percentages add up to 100

      _startResourceKey = Integer.valueOf(commandLine.getOptionValue("sr", "1"));
      _seed = Integer.valueOf(commandLine.getOptionValue("sd", "-1")); // add system.current time

      _dataFileName = commandLine.getOptionValue("fn", "eventGeneratorTrace.txt");
      _generateDataFile = commandLine.hasOption("df");
      _generateDataFileOnly = commandLine.hasOption("fo");
      if (_generateDataFileOnly) {
        _generateDataFile = true;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      LOG.info("Usage: StandaloneDatastreamEventGenerator -sf <schema_file_name>\n");
      LOG.info("Optional arguments:  -ds <string>    <---   datastream name.      default: \"test_ds\" \n");
      LOG.info("                     -db <string>    <---   database name.        default: \"test_db\" \n");
      LOG.info("                     -tb <string>    <---   table name.           default: \"test_table\" \n");
      LOG.info("                     -kt <string>    <---   kafka topic name.     default: \"es_test_db\" \n");
      LOG.info("                     -sp <string>    <---   schema file path.     default: \".\" \n");

      LOG.info("                     -np <integer>   <---   number of partitions. default: 1\n");
      LOG.info("                     -xs <integer>   <---   max transaction size. default: 1\n");
      LOG.info("                     -ne <integer>   <---   number of events.     default: 100\n");
      LOG.info("                     -pd <string>    <---   percent of inserts, updates, deletes and control events .     default: \"100,0,0,0\" \n");

      LOG.info("                     -sr <integer>   <---   start resource key.   default: 1\n");
      LOG.info("                     -sd <long>      <---   seed value.           default: -1\n");

      LOG.info("                     -fn <string>    <---   data file name.       default: \"eventGeneratorTrace.txt\" \n");
      LOG.info("                     -df             <---   generate data file.\n");
      LOG.info("                     -fo             <---   generate data file only.\n");
      return false;
    }
    return true;
  }
}
