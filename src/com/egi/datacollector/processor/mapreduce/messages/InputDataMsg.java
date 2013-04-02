package com.egi.datacollector.processor.mapreduce.messages;


public class InputDataMsg<X> {

	private final X data;
	
	public InputDataMsg(X data){
		this.data = data;
	}

	public X getData() {
		return data;
	}
}
