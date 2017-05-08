package com.linkedin.datastream.server.api.security;

import com.linkedin.datastream.common.Datastream;


/**
 * Provide namespace to hold an ACL of a datastream.
 */
public interface AclNamespaceProvider {
  String getNamespace(Datastream datastream);
}
