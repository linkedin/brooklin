package com.linkedin.datastream.server;

/**
 * Class to create DummyDatastreameventProducer that is used for testing
 */
public class DummyDatastreamEventProducerFactory implements EventProducerFactory{

    public DummyDatastreamEventProducerFactory()  {
        // Do Nothing.
    }


    public EventProducer create(DatastreamTask task){
        return new DummyDatastreamEventProducer();
    }
}
