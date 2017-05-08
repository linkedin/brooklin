package com.linkedin.datastream.server.api.security;

import java.util.Properties;


/**
 * Factory interface for a Brooklin authorizer.
 */
public interface AuthorizerFactory {
  /**
   * Create an authorizer based on the specified config.
   * @param config
   * @return
   */
  Authorizer createAuthorizer(Properties config);
}
