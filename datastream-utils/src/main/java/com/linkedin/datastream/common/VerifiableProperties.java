package com.linkedin.datastream.common;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Enumeration;


/**
 * Verifiable properties for configs
 */
public class VerifiableProperties {

  private final HashSet<String> _referenceSet = new HashSet<String>();
  private final Properties _props;
  protected Logger LOG = LoggerFactory.getLogger(getClass());

  public VerifiableProperties(Properties props) {
    this._props = props;
  }

  /**
   * Get all properties under a specific domain (start with a certain prefix)
   * @param prefix The prefix being used to filter the properties. If it's blank (null or empty or whitespace only),
   *               the method will return a copy of all properties.
   * @param preserveFullKey Whether to preserve the full key (including the prefix) or strip the prefix in the result
   * @return A Properties that contains all the entries that are under the domain
   */
  public Properties getDomainProperties(String prefix, boolean preserveFullKey) {
    String fullPrefix;
    if (StringUtils.isBlank(prefix)) {
      fullPrefix = ""; // this will effectively retrieve all properties
    } else {
      fullPrefix = prefix.endsWith(".") ? prefix : prefix + ".";
    }
    Properties ret = new Properties();
    _props.keySet().forEach(key -> {
      String keyStr = key.toString();
      if (keyStr.startsWith(fullPrefix) && !keyStr.equals(fullPrefix)) {
        if (preserveFullKey) {
          ret.put(keyStr, getProperty(keyStr));
        } else {
          ret.put(keyStr.substring(fullPrefix.length()), getProperty(keyStr));
        }
      }});
    return ret;
  }

  public Properties getDomainProperties(String prefix) {
    return getDomainProperties(prefix, false);
  }

  public boolean containsKey(String name) {
    return _props.containsKey(name);
  }

  public String getProperty(String name) {
    String value = _props.getProperty(name);
    _referenceSet.add(name);
    return value;
  }

  /**
   * Read a required integer property value or throw an exception if no such property is found
   */
  public int getInt(String name) {
    return Integer.parseInt(getString(name));
  }

  public int getIntInRange(String name, int start, int end) {
    if (!containsKey(name)) {
      throw new IllegalArgumentException("Missing required property '" + name + "'");
    }
    return getIntInRange(name, -1, start, end);
  }

  /**
   * Read an integer from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the integer value
   */
  public int getInt(String name, int defaultVal) {
    return getIntInRange(name, defaultVal, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public Short getShort(String name, Short defaultVal) {
    return getShortInRange(name, defaultVal, Short.MIN_VALUE, Short.MAX_VALUE);
  }

  /**
   * Read an integer from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @param start The start of the range in which the value must fall (inclusive)
   * @param end The end of the range in which the value must fall
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the integer value
   */
  public int getIntInRange(String name, int defaultVal, int start, int end) {
    int v = 0;
    if (containsKey(name)) {
      v = Integer.parseInt(getProperty(name));
    } else {
      v = defaultVal;
    }
    if (v >= start && v <= end) {
      return v;
    } else {
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start + "-" + end
          + ".");
    }
  }

  public Short getShortInRange(String name, Short defaultVal, Short start, Short end) {
    Short v = 0;
    if (containsKey(name)) {
      v = Short.parseShort(getProperty(name));
    } else {
      v = defaultVal;
    }
    if (v >= start && v <= end) {
      return v;
    } else {
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start + "-" + end
          + ".");
    }
  }

  public Double getDoubleInRange(String name, Double defaultVal, Double start, Double end) {
    Double v = 0.0;
    if (containsKey(name)) {
      v = Double.parseDouble(getProperty(name));
    } else {
      v = defaultVal;
    }
    // use big decimal for double comparison
    BigDecimal startDecimal = new BigDecimal(start);
    BigDecimal endDecimal = new BigDecimal(end);
    BigDecimal value = new BigDecimal(v);
    if (value.compareTo(startDecimal) >= 0 && value.compareTo(endDecimal) <= 0) {
      return v;
    } else {
      throw new IllegalArgumentException(name + " has value " + v + " which is not in range " + start + "-" + end + ".");
    }
  }

  /**
   * Read a required long property value or throw an exception if no such property is found
   */
  public long getLong(String name) {
    return Long.parseLong(getString(name));
  }

  /**
   * Read an long from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the long value
   */
  public long getLong(String name, long defaultVal) {
    return getLongInRange(name, defaultVal, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /**
   * Read an long from the properties instance. Throw an exception
   * if the value is not in the given range (inclusive)
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @param start The start of the range in which the value must fall (inclusive)
   * @param end The end of the range in which the value must fall
   * @throws IllegalArgumentException If the value is not in the given range
   * @return the long value
   */
  public long getLongInRange(String name, long defaultVal, long start, long end) {
    long v = 0;
    if (containsKey(name)) {
      v = Long.parseLong(getProperty(name));
    } else {
      return defaultVal;
    }
    if (v >= start && v <= end) {
      return v;
    } else {
      throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " + start + "-" + end
          + ".");
    }
  }

  /**
   * Get a required argument as a double
   * @param name The property name
   * @return the value
   * @throw IllegalArgumentException If the given property is not present
   */
  public double getDouble(String name) {
    return Double.parseDouble(getString(name));
  }

  /**
   * Get an optional argument as a double
   * @param name The property name
   * @default The default value for the property if not present
   */
  public double getDouble(String name, double defaultVal) {
    if (containsKey(name)) {
      return getDouble(name);
    } else {
      return defaultVal;
    }
  }

  /**
   * Read a boolean value from the properties instance
   * @param name The property name
   * @param defaultVal The default value to use if the property is not found
   * @return the boolean value
   */
  public boolean getBoolean(String name, boolean defaultVal) {
    String v = "";
    if (!containsKey(name)) {
      return defaultVal;
    } else {
      v = getProperty(name);
      if (v.compareTo("true") == 0 || v.compareTo("false") == 0) {
        return Boolean.parseBoolean(v);
      } else {
        throw new IllegalArgumentException(name + " has value " + v + " which is not true or false.");
      }
    }
  }

  public boolean getBoolean(String name) {
    return Boolean.parseBoolean(getString(name));
  }

  /**
   * Get a string property, or, if no such property is defined, return the given default value
   */
  public String getString(String name, String defaultVal) {
    if (containsKey(name)) {
      return getProperty(name);
    } else {
      return defaultVal;
    }
  }

  /**
   * Get a string property or throw and exception if no such property is defined.
   */
  public String getString(String name) {
    if (!containsKey(name)) {
      throw new IllegalArgumentException("Missing required property '" + name + "'");
    } else {
      return getProperty(name);
    }
  }

  public void verify() {
    LOG.info("Verifying properties");
    Enumeration keys = _props.propertyNames();
    while (keys.hasMoreElements()) {
      Object key = keys.nextElement();
      if (!_referenceSet.contains(key)) {
        LOG.warn("Property {} is not valid", key);
      } else {
        LOG.info("Property {} is overridden to {}", key, _props.getProperty(key.toString()));
      }
    }
  }

  public String toString() {
    return _props.toString();
  }
}
