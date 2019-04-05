/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

/**
 *  ProducerFactory: Factory for DatastreamEventProducerRunnable
 */
public class ProducerFactory {

  /**
   * returns the Producer runnable, currently supports DatastreamEventProducer 
   * only. Throw exception for all other producer types.
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
