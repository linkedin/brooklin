/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
