/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

/**
 *  A utility factory of {@link DatastreamEventProducerRunnable} objects
 */
public class ProducerFactory {

  /**
   * Creates a {@link DatastreamEventProducerRunnable} based on the provided {@link GlobalSettings}
   * @throws IllegalArgumentException for all {@link GlobalSettings.ProducerType}s other than
   * {@link GlobalSettings.ProducerType#Datastream}.
   */
  public static Runnable getProducer(GlobalSettings globalSettings) {
    switch (globalSettings._producerType) {

      case Datastream: {
        return new DatastreamEventProducerRunnable(globalSettings);
      }

      case Generic: // let it slide
      case mySQL:
      case Espresso: {
        throw new IllegalArgumentException("The producer type requested is not yet supported");
      }

      default: {
        throw new IllegalArgumentException("The producer type requested is illegal");
      }
    }
  }
}
