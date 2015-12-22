package com.linkedin.datastream.testutil.event.generator;

public class StandaloneDatastreamEventGenerator {
  /**
   * @param args
   */
  public static void main(String[] args) {
    DatastreamEventGeneratorCmdline dsCmdline = new DatastreamEventGeneratorCmdline();
    boolean b = dsCmdline.runWithShutdownHook(args);
    System.exit((b) ? 0 : 1);
  }
}
