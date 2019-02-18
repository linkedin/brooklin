/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.security;

import java.security.Principal;

import com.linkedin.datastream.common.Datastream;


/**
 * Abstraction to allow pluggable authorization systems to be used by brooklin server to authorize
 * CRUD operations on datastreams.
 */
public interface Authorizer {
  /**
   * CREATE/UPDATE/DELETE controls those operations on the datastream objects.
   * READ is 2nd operation to CREATE for source-specific authorization of the
   * "owner" of the datastream, ie. is the "owner" authorized to consume the
   * "data source" represented by the datastream being created.
   */
  enum Operation { CREATE, READ, UPDATE, DELETE }

  /**
   * Validate if the {@param principal} is authorized to perform {@param operation}
   * on the specified {@param datastream}.
   *
   * @param datastream datastream to be operated on
   * @param operation type of the operation in {@link Operation}
   * @param principal security principal of the caller
   * @return true if owners are authorized, false otherwise
   */
  boolean authorize(Datastream datastream, Operation operation, Principal principal);
}
