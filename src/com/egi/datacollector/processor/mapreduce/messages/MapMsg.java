package com.egi.datacollector.processor.mapreduce.messages;


public class MapMsg<X, K, V> {

	private final X element;
	
	public X getElement() {
		return element;
	}
	
	
	public MapMsg(X element){
		this.element = element;
		
	}
		
}
