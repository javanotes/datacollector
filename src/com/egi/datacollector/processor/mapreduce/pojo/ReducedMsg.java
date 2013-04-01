package com.egi.datacollector.processor.mapreduce.pojo;

import com.egi.datacollector.processor.mapreduce.KeyValue;

public class ReducedMsg<K, V> {

	private final KeyValue<K, V> reduced;
	
	public ReducedMsg(KeyValue<K, V> y){
		reduced = y;
	}

	public KeyValue<K, V> getReduced() {
		return reduced;
	}
}
