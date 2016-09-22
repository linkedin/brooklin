package com.linkedin.datastream.common;

/**
 * Various well known config keys used in datastream metadata.
 */
public class DatastreamMetadataConstants {

  /**
   * Represents whether the datastream has an User managed destination (a.k.a BYOT - Bring your own topic)
   */
  public static final String IS_USER_MANAGED_DESTINATION_KEY = "IsUserManagedDestination";

  /**
   * Whether the datastream should reuse existing datastream's destination if it is available.
   */
  public static final String REUSE_EXISTING_DESTINATION_KEY = "reuseExistingDestination";

  /**
   * Represents datastream owner
   */
  public static final String OWNER_KEY = "owner";

  /**
   * Timestamp in Epoch-millis when destination was created
   */
  public static final String DESTINATION_CREATION_MS = "destination.creation.ms";

  /**
   * Duration in Epoch-millis before destination starts to delete messages
   */
  public static final String DESTINATION_RETENION_MS = "destination.retention.ms";

  /**
   * Schema registry URI for Kafka
   */
  public static final String KAFKA_SCHEMA_REGISTRY_URI = "kafkaSchemaRegistry.uri";

  /**
   * Mode (cached, inmemory) for Kafka schema registry
   */
  public static final String KAFKA_SCHEMA_REGISTRY_MODE = "kafkaSchemaRegistry.mode";

}
