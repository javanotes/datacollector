package com.egi.datacollector.processor.file;

import com.egi.datacollector.processor.Data;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.processor.ProcessorFactory;
import com.egi.datacollector.util.exception.ProcessorException;

public class RecordsProcessor extends Processor {

	@Override
	public boolean process(Data job) throws ProcessorException {
		if(job instanceof RecordsData){
			RecordsData records = (RecordsData)job;
			Processor recordProcessor = ProcessorFactory.getProcessor(RecordProcessor.class);
			
			//get each record
			for(RecordData record : records.getRecords()){
				//do some transformation / parsing
				recordProcessor.process(record);
			}
			//persist in batch
		}
		return false;
	}

}
