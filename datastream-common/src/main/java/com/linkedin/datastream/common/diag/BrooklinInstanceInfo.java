/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import org.jetbrains.annotations.NotNull;


/**
 * BrooklinInstanceInfo provides a convenience method to fetch the current Brooklin server's cluster instance name with
 * a fallback to {@value #UNKNOWN_INSTANCE_NAME} if it cannot be determined.
 */
public class BrooklinInstanceInfo {

  /**
   * Default (fallback) instance name if it is not possible to determine our instance name.
   */
  private static final String UNKNOWN_INSTANCE_NAME = "UNKNOWN";

  /**
   * The singleton instance of this class.
   */
  private static final BrooklinInstanceInfo INSTANCE = new BrooklinInstanceInfo();

  /**
   * This instance's name.
   */
  @NotNull
  private String _instanceName;

  /**
   * Private construction for BrooklinInstanceInfo.
   */
  private BrooklinInstanceInfo() {
    _instanceName = UNKNOWN_INSTANCE_NAME;
  }

  /**
   * Gets this Brooklin server's cluster instance name if it was previously set, or {@value #UNKNOWN_INSTANCE_NAME} if
   * not.
   *
   * @return this server's instance name or {@value #UNKNOWN_INSTANCE_NAME}
   */
  @NotNull
  public static String getInstanceName() {
    return INSTANCE._instanceName;
  }

  /**
   * Sets this Brooklin server's cluster instance name.
   *
   * @param instanceName this server's instance name
   */
  public static void setInstanceName(@NotNull final String instanceName) {
    INSTANCE._instanceName = instanceName;
  }
}
