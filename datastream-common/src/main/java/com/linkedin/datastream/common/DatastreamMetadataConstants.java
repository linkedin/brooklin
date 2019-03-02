/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Various well known config keys used in datastream metadata.
 */
public class DatastreamMetadataConstants {

  /**
   * Represents whether the datastream has an User managed destination (a.k.a BYOT - Bring your own topic)
   */
  public static final String IS_USER_MANAGED_DESTINATION_KEY = "system.IsUserManagedDestination";

  /**
   * Represents whether the datastream has a connector managed destination, so destination should not be created on
   * datastream creation
   */
  public static final String IS_CONNECTOR_MANAGED_DESTINATION_KEY = "system.IsConnectorManagedDestination";

  /**
   * Whether the datastream should reuse existing datastream's destination if it is available.
   */
  public static final String REUSE_EXISTING_DESTINATION_KEY = "system.reuseExistingDestination";

  /**
   * Prefix any event metadata with this if transport supports sending metadata with events.
   */
  public static final String EVENT_METADATA_PREFIX = "system.event.metadata";

  /**
   * Represents datastream owner which is also the security principal for authorization.
   * NOTE that owner can be a list of entities.
   */
  public static final String OWNER_KEY = "owner";

  /**
   * Task prefix used for identifying all the tasks corresponding to the datastream.
   */
  public static final String TASK_PREFIX = "system.taskPrefix";

  /**
   * Common prefix for all destination related metadata
   */
  public static final String SYSTEM_DESTINATION_PREFIX = "system.destination.";

  /**
   * Timestamp in Epoch-millis when destination was created
   */
  public static final String DESTINATION_CREATION_MS = SYSTEM_DESTINATION_PREFIX + "creation.ms";

  /**
   * Duration in Epoch-millis before destination starts to delete messages
   */
  public static final String DESTINATION_RETENION_MS = SYSTEM_DESTINATION_PREFIX + "retention.ms";

  /**
   * Indicates if the data store in destination requires to be encrypted or not
   */
  public static final String DESTINATION_ENCRYPTION_REQUIRED = "system.destination.encryptionRequired";

  /**
   * The name of the schema used to serialize the messages in the destination
   */
  public static final String DESTINATION_PAYLOAD_SCHEMA_NAME = SYSTEM_DESTINATION_PREFIX + "payloadSchemaName";

  /**
   * Timestamp of datastream creation in epoch-milis
   */
  public static final String CREATION_MS = "system.creation.ms";

  /**
   * Position at which the ingestion should start for the datastream.
   */
  public static final String START_POSITION = "system.start.position";

  /**
   * UID is added and reserved for the future usage. If set, it could be used
   * to identity the datastream/topic name to prevent duplications.
   */
  public static final String UID = "system.uid";

  /**
   * Connector can use this for datastreams with finite set of events such that
   * can be deleted after TTL expires. The TTL is expressed as miliseconds.
   */
  public static final String TTL_MS = "system.ttl.ms";

  /**
   * Key to get the set of paused partitions from datastream metadata.
   */
  public static final String PAUSED_SOURCE_PARTITIONS_KEY = "system.pausedSourcePartitions";

  /**
   * Regex indicating pausing all partitions in a topic
   */
  public static final String REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC = "*";

  /**
   * Key to set consumer group ID of the datastream.
   */
  public static final String GROUP_ID = "group.id";
}
