/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.io.IOException;
import java.net.ServerSocket;


/**
 * Class that contains the helper utility methods for Network operations.
 */
public final class NetworkUtils {

  private NetworkUtils() {
  }

  /**
   * Get the next available port that can be used for opening a socket connection.
   */
  public synchronized static int getAvailablePort() {
    try {
      // This ServerSocket is bound to an ephemeral port (port 0) solely to discover a free port
      // number and is closed immediately via try-with-resources; no data is ever transmitted over
      // it, so transport encryption does not apply. The Semgrep unencrypted-socket rule is a false
      // positive for this port-discovery idiom.
      // nosemgrep: java.lang.security.audit.crypto.unencrypted-socket.unencrypted-socket
      try (ServerSocket socket = new ServerSocket(0)) {
        socket.setReuseAddress(true);
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot find available port: " + e.getMessage(), e);
    }
  }
}
