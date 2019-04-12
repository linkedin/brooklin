/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.tools;

import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for creating command line {@link Option}s
 */
public class OptionUtils {

  /**
   * Create a command line option
   * @param shortOpt short representation of the option
   * @param longOpt long representation of the option
   * @param argName display name for argument value
   * @param required indicates whether option is required
   * @param description describes the function of the option
   */
  public static Option createOption(String shortOpt, String longOpt, String argName, boolean required,
      String description) {
    boolean hasArg = false;

    if (!StringUtils.isBlank(argName)) {
      hasArg = true;
    }

    Option option = new Option(shortOpt, longOpt, hasArg, description);
    option.setRequired(required);
    if (hasArg) {
      option.setArgName(argName);
    }

    return option;
  }
}
