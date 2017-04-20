package com.linkedin.datastream.connectors.oracle.triggerbased;


import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.common.DatastreamException;

/**
 * A simple utility class which helps abstracting away the SchemaRegistryRestfullClient
 */
class SchemaUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtil.class);

  static final String SCHEMA_ID_KEY = "schema_id";

  // A mapping between the DatastreamTask and the SchemaId
  private static final Map<DatastreamTask, String> TASK_SCHEMA_ID_MAP = new ConcurrentHashMap<>();

  /**
   * Grab the SchemaId from the datastream within the datastreamTask.
   * A datastream can technically have more than one datastream, however, all the datastreams
   * should point to the same destination and  source. Therefore, we need to make sure that
   * the schemaId in each stream is the same
   *
   * @param task - The datastreamTask this OracleTaskHandler is handling
   * @return The schema_id
   * @throws DatastreamRuntimeException - If one DatastreamTask has more than one stream which have two
   *                                      different schema_id's
   */
  static String getSchemaId(DatastreamTask task) {
    if (TASK_SCHEMA_ID_MAP.containsKey(task)) {
      return TASK_SCHEMA_ID_MAP.get(task);
    }

    List<Datastream> streams = task.getDatastreams();
    String schemaId = streams.get(0).getMetadata().get(SCHEMA_ID_KEY);

    for (Datastream stream : streams) {
      String otherSchemaId = stream.getMetadata().get(SCHEMA_ID_KEY);
      if (!otherSchemaId.equals(schemaId)) {
        throw new DatastreamRuntimeException(String.format("Two different SchemaIds %s and %s",
            schemaId,
            otherSchemaId));
      }
    }

    TASK_SCHEMA_ID_MAP.put(task, schemaId);

    return schemaId;
  }

  /**
   * Given a Datastream find the SchemaId with in the Metadata
   *
   * @param stream
   * @return schemaId
   */
  static String getSchemaId(Datastream stream) {
    return stream.getMetadata().get(SCHEMA_ID_KEY);
  }

  /**
   * Grab the schema using the passed in SchemaRegistryClient.
   * This function throws a DatastreamRunTimeException if the schema could not be found.
   *
   * @param id - the Id of the schema as registered in the Schema Registry (obtained from the DatastreamTask)
   * @param schemaRegistry
   * @return
   */
  static Schema getSchemaById(String id, SchemaRegistryClient schemaRegistry) throws DatastreamException {
    Schema schema = null;

    try {
      schema = schemaRegistry.getSchemaByID(id);
    } catch (Exception e) {
      throw new DatastreamException(String.format("Could not find Schema with Id: %s", id), e);
    }

    return schema;
  }

  /**
   * A simpler API that grabs the schema from the datastream Task. Called from the
   * OracleTaskHandler class.
   *
   * @param task
   * @return
   */
  static Schema getSchemaByTask(DatastreamTask task, SchemaRegistryClient registry) {
    String schemaId = getSchemaId(task);
    try {
      return getSchemaById(schemaId, registry);
    } catch (DatastreamException e) {
      // This should not happen since it has already been retrieved during the
      // {@code #initializeDatastream} method and stored in cache
      LOG.error("Failed to get Schema from Schema Registry from task " + task.getId());
      throw new DatastreamRuntimeException(e);
    }
  }

}