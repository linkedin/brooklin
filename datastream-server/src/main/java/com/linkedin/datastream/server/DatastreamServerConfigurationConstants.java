/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

/**
 * Holds constants for DatastreamServer configuration.
 */
public final class DatastreamServerConfigurationConstants {

  public static final String CONFIG_PREFIX = "brooklin.server.";
  public static final String CONFIG_CONNECTOR_NAMES = CONFIG_PREFIX + "connectorNames";
  public static final String CONFIG_HTTP_PORT = CONFIG_PREFIX + "httpPort";
  public static final String CONFIG_CSV_METRICS_DIR = CONFIG_PREFIX + "csvMetricsDir";
  public static final String CONFIG_ZK_ADDRESS = CoordinatorConfig.CONFIG_ZK_ADDRESS;
  public static final String CONFIG_CLUSTER_NAME = CoordinatorConfig.CONFIG_CLUSTER;
  public static final String CONFIG_ENABLE_EMBEDDED_JETTY = "enableEmbeddedJetty";
  public static final String CONFIG_FACTORY_CLASS_NAME = "factoryClassName";
  public static final String CONFIG_CONNECTOR_BOOTSTRAP_TYPE = "bootstrapConnector";
  public static final String CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY = "assignmentStrategyFactory";
  public static final String CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING = "customCheckpointing";
  public static final String CONFIG_CONNECTOR_PREFIX = CONFIG_PREFIX + "connector.";
  public static final String STRATEGY_DOMAIN = "strategy";
  public static final String CONFIG_TRANSPORT_PROVIDER_NAMES = CONFIG_PREFIX + "transportProviderNames";
  public static final String CONFIG_TRANSPORT_PROVIDER_PREFIX = CONFIG_PREFIX + "transportProvider.";
  public static final String CONFIG_SERDE_NAMES = CONFIG_PREFIX + "serdeNames";
  public static final String CONFIG_SERDE_PREFIX = CONFIG_PREFIX + "serde.";
  public static final String CONFIG_CONNECTOR_DEDUPER_FACTORY = "deduperFactory";
  public static final String DEFAULT_DEDUPER_FACTORY = SourceBasedDeduperFactory.class.getName();
  public static final String DOMAIN_DEDUPER = "deduper";
  public static final String CONFIG_CONNECTOR_AUTHORIZER_NAME = "authorizerName";
  // Restli port and path might be different in the staging or prod fabrics, so make it configurable.
  public static final String DOMAIN_DIAG = CONFIG_PREFIX + "diag";
  public static final String CONFIG_DIAG_PORT = "port";
  public static final String CONFIG_DIAG_PATH = "path";

}
