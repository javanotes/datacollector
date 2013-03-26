package com.egi.datacollector.processor.file;

import com.egi.datacollector.processor.Data;

public class RecordData implements Data {
	
	private final String aRecord;
	
	public RecordData(String record){
		aRecord = record;
	}

	@Override
	public String type() {
		return "record";
	}

	public String getRecord() {
		return aRecord;
	}

}
