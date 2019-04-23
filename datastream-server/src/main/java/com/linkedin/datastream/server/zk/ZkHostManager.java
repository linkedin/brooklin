/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Zk host manager will record the join time for each zk node and blacklist the node which keep joining
 * and leaving the group
 */
public class ZkHostManager {
  private static final Logger LOG = LoggerFactory.getLogger(ZkHostManager.class);

  private final boolean _enableBlacklist;
  private final ZkHostConfig _config;

  private Map<String, ZkHost> _zkHosts = new HashMap<>();

  /**
   * Constructor
   * @param zkHostConfig
   */
  public ZkHostManager(ZkHostConfig zkHostConfig) {
    _config = zkHostConfig;
    _enableBlacklist = _config.getEnableBlacklist();
    if (_enableBlacklist) {
      LOG.info("Enable ZkHostManager, maxJoinNum: {}, blacklistedDuration: {}, stabilizedWindow: {}",
          _config.getMaxJoinNum(), _config.getBlacklistedDurationMs(), _config.getStabilizedWindowMs());
    }
  }

  /**
   * register the join action from zk live instance, if enableBlacklist is enabled, it will keep tracking the hostname
   * so that if a host will get blacklisted if it joins and leaves too frequently
   * @param liveInstance all live instance return from Zookeeper
   * @return return a list o stable zk instance that doesn't get blacklisted
   */
  public List<String> joinHosts(List<String> liveInstance) {
    if (!_enableBlacklist) {
      return liveInstance;
    }

    liveInstance.stream().forEach(instance -> {
      ZkHost zkHost = new ZkHost(instance, _config.getMaxJoinNum(), _config.getBlacklistedDurationMs(), _config.getStabilizedWindowMs());
      String hostname = zkHost.getHostname();
      if (_zkHosts.containsKey(hostname)) {
        _zkHosts.get(hostname).registerJoin(instance);
      } else {
        _zkHosts.put(hostname, zkHost);
      }
    });

    return _zkHosts.values().stream().filter(zkHost -> !zkHost.isBlacklisted())
        .map(ZkHost::getInstanceName).sorted().collect(Collectors.toList());
  }
}
