/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;


/**
 * A factory for {@link JmxReporter} objects that generates metrics whose
 * names are decided according to {@link BrooklinObjectNameFactory}.
 */
public class JmxReporterFactory {
  private static final ObjectNameFactory OBJECT_NAME_FACTORY = new BrooklinObjectNameFactory();

  /**
   * Creates a {@link JmxReporter} that generates metrics whose names
   * are decided according to the {@link BrooklinObjectNameFactory}.
   */
  public static JmxReporter createJmxReporter(MetricRegistry metricRegistry) {
    Validate.notNull(metricRegistry);

    return JmxReporter.forRegistry(metricRegistry)
        .createsObjectNamesWith(OBJECT_NAME_FACTORY)
        .build();
  }

  /**
   * An implementation of {@link ObjectNameFactory} that uses the same method
   * for generating metric names as that employed by {@code io.dropwizard.metrics}'s
   * {@link com.codahale.metrics.jmx.DefaultObjectNameFactory} prior to {@code v4.1.0-rc2}.
   * In particular, this implementation does not encode metric types in the names
   * of the generated JMX MBeans/metrics.
   */
  private static class BrooklinObjectNameFactory implements ObjectNameFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxReporter.class);

    @Override
    public ObjectName createName(String type, String domain, String name) {
      try {
        ObjectName objectName = new ObjectName(domain, "name", name);
        if (objectName.isPattern()) {
          objectName = new ObjectName(domain, "name", ObjectName.quote(name));
        }
        return objectName;
      } catch (MalformedObjectNameException e) {
        try {
          return new ObjectName(domain, "name", ObjectName.quote(name));
        } catch (MalformedObjectNameException e1) {
          LOGGER.warn("Unable to register {} {}", type, name, e1);
          throw new RuntimeException(e1);
        }
      }
    }
  }
}
