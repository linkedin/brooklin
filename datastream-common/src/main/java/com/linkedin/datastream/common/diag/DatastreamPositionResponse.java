/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.datastream.common.JsonUtils;


/**
 * This class holds a mapping between datastreams (by name) to a mapping of physical sources (which then maps to
 * position data). It is intended to be used in response to a position request from a
 * {@link com.linkedin.datastream.common.DiagnosticsAware} connector.
 *
 * @see PhysicalSources for information on physical source to position mapping
 * @see PhysicalSourcePosition for information on positions
 */
public class DatastreamPositionResponse {

  /**
   * A mapping from datastream name to physical sources.
   */
  private final Map<String, PhysicalSources> _datastreamToPhysicalSources = new ConcurrentHashMap<>();

  /**
   * Default constructor.
   */
  public DatastreamPositionResponse() {
  }

  /**
   * Constructor which accepts an existing Map of datastreams to PhysicalSources
   * @param datastreamToPhysicalSources the existing Map of datastreams to PhysicalSources
   */
  public DatastreamPositionResponse(final Map<String, PhysicalSources> datastreamToPhysicalSources) {
    _datastreamToPhysicalSources.putAll(datastreamToPhysicalSources);
  }

  /**
   * Retains only the datastreams in the specified collection (removes all others). In other words, removes from
   * the mappings of datastreams -> physical sources, all entries where the datastream is not contained in the specified
   * collection.
   * @param datastreams the list of datastreams to keep
   */
  public void retainAll(final Collection<String> datastreams) {
    _datastreamToPhysicalSources.keySet().retainAll(datastreams);
  }

  /**
   * Merges two DatastreamPositionResponse objects together using the freshest data available between the position data
   * in the PhysicalSources for each datastream.
   * @param first the first DatastreamPositionResponse object
   * @param second the second DatastreamPositionResponse object
   * @return a merged DatastreamPositionResponse object
   * @see PhysicalSources#merge(PhysicalSources, PhysicalSources) for information on how the
   *      position data is merged
   */
  public static DatastreamPositionResponse merge(final DatastreamPositionResponse first,
      final DatastreamPositionResponse second) {
    final DatastreamPositionResponse result = new DatastreamPositionResponse();
    result.setDatastreamToPhysicalSources(first._datastreamToPhysicalSources);
    if (second != null) {
      second._datastreamToPhysicalSources.forEach((datastream, secondSources) -> {
        final PhysicalSources firstSources = first.getDatastreamToPhysicalSources()
            .getOrDefault(datastream, new PhysicalSources());
        final PhysicalSources mergedSources = PhysicalSources.merge(firstSources, secondSources);
        result._datastreamToPhysicalSources.put(datastream, mergedSources);
      });
    }
    return result;
  }

  /**
   * Getter returns the current datastream to physical sources map.
   *
   * This method is used for JSON serialization.
   *
   * @return an immutable copy of the current datastream to physical sources map
   */
  public Map<String, PhysicalSources> getDatastreamToPhysicalSources() {
    return Collections.unmodifiableMap(_datastreamToPhysicalSources);
  }

  /**
   * Setter causes the entry set of the current datastream to physical sources map to match the provided one. If a null
   * map is provided, the current map will be cleared.
   *
   * This method is used for JSON serialization.
   *
   * @param datastreamToPhysicalSources a map to set the current datastream to physical sources map to
   */
  public void setDatastreamToPhysicalSources(final Map<String, PhysicalSources> datastreamToPhysicalSources) {
    _datastreamToPhysicalSources.clear();
    if (datastreamToPhysicalSources != null) {
      _datastreamToPhysicalSources.putAll(datastreamToPhysicalSources);
    }
  }

  /**
   * A simple String representation of this object suitable for use in debugging or logging.
   * @return a simple String representation of this object
   */
  @Override
  public String toString() {
    return "DatastreamPositionResponse{" + "_datastreamToPhysicalSources=" + _datastreamToPhysicalSources + '}';
  }

  /**
   * Serializes the given DatastreamPositionResponse object into an appropriate JSON representation.
   * @param response the DatastreamPositionResponse object
   * @return a serialized DatastreamPositionResponse object in JSON format
   */
  public static String toJson(final DatastreamPositionResponse response) {
    return JsonUtils.toJson(response);
  }

  /**
   * Deserializes the given JSON string into a DatastreamPositionResponse object.
   * @param json the serialized DatastreamPositionResponse object in JSON format
   * @return a DatastreamPositionResponse object
   */
  public static DatastreamPositionResponse fromJson(final String json) {
    return JsonUtils.fromJson(json, DatastreamPositionResponse.class);
  }
}