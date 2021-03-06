package com.egi.datacollector.processor.mapreduce.data;

import java.util.Collection;

import com.egi.datacollector.processor.data.Data;
import com.egi.datacollector.processor.file.data.RecordData;
import com.egi.datacollector.processor.mapreduce.KeyValue;

public class MapreduceData implements Data {
	
	private RecordData record;
	
	public MapreduceData(RecordData aRecordData){
		record = aRecordData;
	}

	public MapreduceData(Collection<KeyValue<String, String>> mapReduceResults) {
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -8475414760241690796L;

	@Override
	public String type() {
		return "mapreduce";
	}

	public RecordData getRecord() {
		return record;
	}

}
