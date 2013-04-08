package com.egi.datacollector.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.exception.ProcessorException;

public class ProcessorFactory {
	
	private static final Logger log = Logger.getLogger(ProcessorFactory.class);

	private ProcessorFactory (){}
	
	private static final Map<String, Processor> processorMap = new HashMap<String, Processor>();
	
	public static Processor getProcessor(Data job) throws ProcessorException{
		String processorClass = "datacollector.processor." + job.type();
		processorClass = Config.getProperty(processorClass);
		
		if(!processorMap.containsKey(processorClass)){
			synchronized (processorMap) {
				if(!processorMap.containsKey(processorClass)){
					
						try {
							Object processor = Class.forName(processorClass).newInstance();
							if (processor != null && processor instanceof Processor) {
								processorMap.put(processorClass, (Processor) processor);
							}
						} catch (InstantiationException e) {
							log.error("Cannot instantiate processor class!", e);
							throw new ProcessorException(e);
						} catch (IllegalAccessException e) {
							log.error("Cannot instantiate processor class!", e);
							throw new ProcessorException(e);
						} catch (ClassNotFoundException e) {
							log.error("Cannot instantiate processor class!", e);
							throw new ProcessorException(e);
						}
					 
				}
			}
		}
		
		return processorMap.get(processorClass);
		
	}
}
