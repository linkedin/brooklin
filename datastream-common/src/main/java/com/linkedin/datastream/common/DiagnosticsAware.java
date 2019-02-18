/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.net.URI;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;


/**
 * Classes that implement DiagnosticsAware should return the status or any information of a host/instance,
 * and be able to reduce all the responses across different hosts/instances.
 * The Restli request will call process on each host/instances, then aggregate all
 * the responses by calling reduce to return a merged response.
 */

public interface DiagnosticsAware {

  // Commonly used key in diagnostics queries
  String DATASTREAM_KEY = "datastream";

  /**
   * @return process the query of a single host/instance, return response such as the status of the host
   */
  String process(String query);

  /**
   * @return reduce/merge the responses of a collection of host/instance into one response
   */
  String reduce(String query, Map<String, String> responses);

  default String getPath(String query, Logger logger) throws Exception {
    URI uri = new URI(query);
    String path = uri.getPath();
    if (StringUtils.isEmpty(path)) {
      logger.error("invalid query (empty path): " + query);
      return null;
    }
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }
}
