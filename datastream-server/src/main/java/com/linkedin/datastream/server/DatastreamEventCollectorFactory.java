package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.VerifiableProperties;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Constructor;
import java.util.Objects;


/**
 * A simple utility class for Connector to instantiate the specified
 * Datastream event collectors based on the configuration.
 */
public class DatastreamEventCollectorFactory {
  public static final String PREFIX = "datastream.server.";
  public static final String CONFIG_COLLECTOR_NAME = PREFIX + "eventCollectorClassName";

  private final VerifiableProperties _config;
  private final Class _collectorClass;

  public DatastreamEventCollectorFactory(VerifiableProperties config) throws DatastreamException {
    Objects.requireNonNull(config, "invalid config.");
    _config = config;
    String className = _config.getProperty(CONFIG_COLLECTOR_NAME);
    if (StringUtils.isEmpty(className)) {
      throw new IllegalArgumentException("invalid event collector class in config.");
    }
    try {
      _collectorClass = Class.forName(className);
    } catch (ClassNotFoundException ex) {
      throw new DatastreamException("Failed to load event collector: " + className, ex);
    }
  }

  public DatastreamEventCollector create(Datastream datastream) throws DatastreamException {
    try {
      Constructor<?> ctor = _collectorClass.getDeclaredConstructor(Datastream.class, VerifiableProperties.class);
      return (DatastreamEventCollector) ctor.newInstance(datastream, _config);
    } catch (Exception ex) {
      throw new DatastreamException("Failed to instantiate event collector: " + _collectorClass.getName(), ex);
    }
  }
}
