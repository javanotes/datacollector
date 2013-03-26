package com.egi.datacollector.processor;

import com.egi.datacollector.util.exception.ProcessorException;

public abstract class Processor {
	
	public abstract boolean process(Data job) throws ProcessorException;

}
