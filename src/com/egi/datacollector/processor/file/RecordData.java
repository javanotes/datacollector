package com.egi.datacollector.processor.file;

import com.egi.datacollector.processor.Data;

public class RecordData implements Data {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8983927812249542272L;
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
