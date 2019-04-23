/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

import java.time.Duration;
import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Brooklin {@link ZkHost} configuration properties
 */
public class ZkHostConfig {
  public static final String ZK_HOST_DOMAIN_CONFIG = "zkHost";
  public static final String CONFIG_ENABLE_BLACKLIST = "enableBlacklist";
  public static final String CONFIG_MAX_JOIN_NUM = "maxJoinNum";
  public static final String CONFIG_BLACKLISTED_DURATION_MS = "blacklistedDurationMs";
  public static final String CONFIG_STABILIZED_WINDOW_MS = "stabilizedWindowMs";


  private boolean _enableBlacklist;
  private final int _maxJoinNum;
  private final long _blacklistedDurationMs;
  private final long _stabilizedWindowMs;

  private final VerifiableProperties _properties;

  /**
   * Construct an instance of ZkHostConfig
   * @param prefix String prefix of the ZkHostConfig group
   * @param properties
   */
  public ZkHostConfig(String prefix, Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    Properties props = verifiableProperties.getDomainProperties(prefix + ZK_HOST_DOMAIN_CONFIG);
    _properties = new VerifiableProperties(props);
    _maxJoinNum = _properties.getInt(CONFIG_MAX_JOIN_NUM, 5);
    _blacklistedDurationMs = _properties.getLong(CONFIG_BLACKLISTED_DURATION_MS, Duration.ofHours(1).toMillis());
    _stabilizedWindowMs = _properties.getLong(CONFIG_STABILIZED_WINDOW_MS, Duration.ofMinutes(5).toMillis());
    _enableBlacklist = _properties.getBoolean(CONFIG_ENABLE_BLACKLIST, false);
  }

  public boolean getEnableBlacklist() {
    return _enableBlacklist;
  }

  public int getMaxJoinNum() {
    return _maxJoinNum;
  }

  public long getBlacklistedDurationMs() {
    return _blacklistedDurationMs;
  }

  public long getStabilizedWindowMs() {
    return _stabilizedWindowMs;
  }
}
