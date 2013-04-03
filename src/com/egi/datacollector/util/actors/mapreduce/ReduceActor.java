package com.egi.datacollector.util.actors.mapreduce;

import java.io.Serializable;

import akka.actor.UntypedActor;

import com.egi.datacollector.processor.mapreduce.IReducer;
import com.egi.datacollector.processor.mapreduce.KeyValue;
import com.egi.datacollector.processor.mapreduce.messages.ReduceMsg;
import com.egi.datacollector.processor.mapreduce.messages.ReducedMsg;

public class ReduceActor<K extends Serializable, V extends Serializable> extends UntypedActor {
	
	private final IReducer<K, V> reduceFunction;
	
	public ReduceActor(IReducer<K, V> r){
		reduceFunction = r;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onReceive(Object arg0) throws Exception {
		if(arg0 instanceof ReduceMsg){
			ReduceMsg reduce = (ReduceMsg)arg0;
			
			KeyValue<K, V> reduced = (KeyValue<K, V>) reduceFunction.reduceForKey((K) reduce.getKey(), reduce.getElement());
			
			getSender().tell(new ReducedMsg<K, V>(reduced), getSelf());
			
		}

	}

}
