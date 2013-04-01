package com.egi.datacollector.processor.mapreduce.pojo;

import java.util.Collection;

public class ReduceMsg<K, V> {

	private final Collection<V> element;
	private final K key;
	
	public Collection<V>  getElement() {
		return element;
	}
	
	public ReduceMsg(Collection<V> element, K k){
		this.element = element;
		key = k;
	}
	public K getKey() {
		return key;
	}
	
	
}
