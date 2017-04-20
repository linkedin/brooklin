package com.linkedin.datastream.connectors.oracle.triggerbased;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.common.SchemaRegistryClientFactory;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SchemaRegistryFactoryImpl implements SchemaRegistryClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryFactoryImpl.class);
  public static final String CFG_MODE = "mode";
  public static final String CFG_REGISTRY_URI = "uri";
  public static final String CACHED_MODE = "cached";
  public static final String INMEMORY_MODE = "inmemory";

  public SchemaRegistryFactoryImpl() {
  }

  public SchemaRegistryClient createSchemaRegistryClient(Properties properties) {
    String mode = properties.getProperty(CFG_MODE, "mode is missing");
    if (mode.equalsIgnoreCase(CACHED_MODE)) {
      if (!properties.containsKey(CFG_REGISTRY_URI)) {
        throw new DatastreamRuntimeException("missing URI for cache-based schemaRegistryClient");
      }
      String uri = properties.getProperty(CFG_REGISTRY_URI);
      LOG.info("Creating schema registry Client with schema registry service " + uri);
      throw new DatastreamRuntimeException("not ready yet!!");
    } else if (mode.equalsIgnoreCase(INMEMORY_MODE)) {
      LOG.info("Creating MockSchemaRegistry, this should be used only for tests.");
      return new MockSchemaRegistry();
    } else {
      throw new DatastreamRuntimeException("invalid mode for schemaRegistry: " + mode);
    }
  }
}
