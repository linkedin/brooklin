package com.linkedin.datastream.server;


/**
 * Factory to create event producers
 */
public interface EventProducerFactory {

    /**
     * Creates a event producer
     * @param task: Datastream task associated with the message producer
     * @return Event Producer
     */
    public EventProducer create(DatastreamTask task);
}
