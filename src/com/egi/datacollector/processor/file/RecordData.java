package com.egi.datacollector.processor.file;

import com.egi.datacollector.processor.Data;
import com.egi.datacollector.util.Constants;

/**
 * Holds each record in a file
 * @author esutdal
 *
 */
public class RecordData implements Data {
	
	public static RecordData endOfFile(){
		return new RecordData(Constants.EOF);
	}
	
	public boolean isEof(){
		return Constants.EOF.equals(aRecord);
	}
	
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
