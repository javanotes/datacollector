package com.egi.datacollector.processor.file.data;

import java.util.ArrayList;
import java.util.List;

import com.egi.datacollector.processor.data.Data;
import com.egi.datacollector.util.Utilities;

/**
 * Holds a set of records to process
 * @author esutdal
 *
 */
public class RecordsData implements Data{
	
	private List<RecordData> records;
	public List<RecordData> getRecords() {
		return records;
	}
	public void setRecords(List<RecordData> records) {
		this.records = records;
	}
	
	
	public RecordsData copy(){
		return (RecordsData) Utilities.deepCopy(this);
		
	}
	public RecordsData(){
		records = new ArrayList<RecordData>();
		
	}
	public void clear(){
		records.clear();
	}
	public void add(RecordData record){
		records.add(record);
	}
	public RecordsData(List<RecordData> records){
		this.records = records;
		
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -3859296786712025005L;

	@Override
	public String type() {
		return "records";
	}

}
