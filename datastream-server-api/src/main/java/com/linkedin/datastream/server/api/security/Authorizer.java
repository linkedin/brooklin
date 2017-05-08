package com.linkedin.datastream.server.api.security;

import java.time.Duration;
import java.util.Optional;

import com.linkedin.datastream.common.Datastream;


/**
 * Authorizer is an interface for an ACL-based authorization system used by BrooklinServer.
 * This interface has some basic assumptions on the actual ACL system:
 *
 *  - ACL key can be repesented by a string
 *  - Source/destination ACL key and namespace is sufficient for ACL CRUD.
 *  - Source ACL is the source-of-truth for destination ACL (if supported).
 *
 * Optionally,
 *  - ACL can be placed into namespaces as defined by {@link Authorizer#ACL_NAMESPACE}
 *  - {@link Authorizer#setServerAcl} can be implemented if BrooklinServer has limited access of the destination
 *  - {@link Authorizer#setDestinationAcl} can be implemented if destination system used the same ACL system
 */
public interface Authorizer {
  /**
   * ACL key of the data source
   */
  String SOURCE_ACL_KEY = "SourceAclKey";

  /**
   * ACL key of the destination (if supported by transport)
   */
  String DESTINATION_ACL_KEY = "DestinationAclKey";

  /**
   * ACL namespace (optional). Transport is expected to provide the namespace from
   * the destination information given source ACL key is already scoped for the data
   * source.
   */
  String ACL_NAMESPACE = "AclNamespace";

  /**
   * Check if the "owners" of the datastream are authorized to access the data source.
   * @param datastream datastream to be accessed (eg. create/delete)
   * @return true if owners are authorized, false otherwise
   */
  boolean hasSourceAccess(Datastream datastream);

  /**
   * Set ACLs to the destination based on the source ACL provisioned prior. This is
   * an optional feature of the actual transport system, which needs to implement
   * AclAware interface to indicate it has such support.
   * @param datastream
   */
  void setDestinationAcl(Datastream datastream);

  /**
   * If not implemented, no ACL sync needs to be performed.
   * @return an interval in-between each ACL sync operation
   */
  default Optional<Duration> getSyncInterval() {
    return Optional.empty();
  }

  /**
   * Enable BrooklinServer the permission to consume the data source and produce to
   * the destination of the specified datastream. This is optional if BrooklinServer
   * already has full access to both the source and destionation systems.
   * @param datastream
   */
  default void setServerAcl(Datastream datastream) {
  }
}
