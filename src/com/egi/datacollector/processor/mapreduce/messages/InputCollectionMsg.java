package com.egi.datacollector.processor.mapreduce.messages;

import java.util.Collection;

public class InputCollectionMsg<X> {

	private final Collection<X> source;
	
	public InputCollectionMsg(Collection<X> source){
		this.source = source;
	}

	public Collection<X> getSource() {
		return source;
	}
}
