package com.egi.datacollector.processor;

import com.egi.datacollector.util.exception.ProcessorException;

/**
 * Abstract class to be extended for creating processors. Processors are NOT inherently thread safe
 * since there will be only one processor instance per jvm, which will be invoked by multiple threads.<p>
 * 
 * The threads are handled by Akka actor system.
 * 
 * Any listeners (except cluster listener), when it receives a message, would add it to a distributed map. The cluster
 * instance which receives the entry will process the entry and then remove it from the map
 * 
 * @author esutdal
 *
 */
public abstract class Processor {
	
	public abstract boolean process(Data job) throws ProcessorException;

}
