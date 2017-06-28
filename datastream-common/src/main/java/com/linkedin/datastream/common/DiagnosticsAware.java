package com.linkedin.datastream.common;

import java.util.Map;


/**
 * Classes that implement DiagnosticsAware should return the status or any information of a host/instance,
 * and be able to reduce all the responses across different hosts/instances.
 * The Restli request will call process on each host/instances, then aggregate all
 * the responses by calling reduce to return a merged response.
 */

public interface DiagnosticsAware {
  /**
   * @return process the query of a single host/instance, return response such as the status of the host
   */
  String process(String query);

  /**
   * @return reduce/merge the responses of a collection of host/instance into one response
   */
  String reduce(String query, Map<String, String> responses);
}
