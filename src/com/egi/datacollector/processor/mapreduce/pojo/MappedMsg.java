package com.egi.datacollector.processor.mapreduce.pojo;

import java.io.Serializable;
import java.util.Collection;

import com.egi.datacollector.processor.mapreduce.KeyValue;

public class MappedMsg<K extends Serializable, V extends Serializable> {
	
	private final Collection<KeyValue<K, V>> list;
	
	public MappedMsg(Collection<KeyValue<K, V>> list){
		this.list = list;
	}

	public Collection<KeyValue<K, V>> getList() {
		return list;
	}

}
