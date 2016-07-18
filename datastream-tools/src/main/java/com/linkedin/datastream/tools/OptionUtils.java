package com.linkedin.datastream.tools;

import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;


public class OptionUtils {

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
