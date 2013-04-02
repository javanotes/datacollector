package com.egi.datacollector.processor.mapreduce.pojo;

import java.io.Serializable;

import com.egi.datacollector.processor.mapreduce.KeyValue;

public class ReducedMsg<K extends Serializable, V extends Serializable> {

	private final KeyValue<K, V> reduced;
	
	public ReducedMsg(KeyValue<K, V> y){
		reduced = y;
	}

	public KeyValue<K, V> getReduced() {
		return reduced;
	}
}
