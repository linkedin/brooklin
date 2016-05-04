package com.linkedin.datastream.testutil.event.generator;

/**
 *  ProducerFactory
 */
public class ProducerFactory {
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
