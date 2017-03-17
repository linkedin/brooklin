package com.linkedin.datastream.server.api.security;

import com.linkedin.datastream.common.Datastream;


/**
 * AclAware can be implemented by both Connector and TransportProviderAdmin
 * to indicate ACLing is supported by a certain authorizer.
 */
public interface AclAware {
  /**
   * Returns an string-based key used for ACL CRUD by the authorizer.
   * @param datastream datastream to be ACL'ed
   * @return ACL key used by authorizer
   */
  String getAclKey(Datastream datastream);
}
