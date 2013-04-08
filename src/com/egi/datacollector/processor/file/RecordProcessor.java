package com.egi.datacollector.processor.file;

import org.apache.log4j.Logger;

import com.egi.datacollector.processor.Data;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.util.exception.ProcessorException;


public class RecordProcessor extends Processor {

	private static Logger log = Logger.getLogger(RecordProcessor.class);
	
	@Override
	public boolean process(Data job) throws ProcessorException {
		// You have a file type and a single csv record in the RecordData
		// process the record (in batch maybe)
		if(job instanceof RecordData){
			RecordData record = ((RecordData)job);
			if(record.isEof()){
				log.info("End of file reached:" + record.isEof());
			}
			
			log.info("data: "+record.getRecord());
		}
		return false;
	}

}
