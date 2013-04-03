package com.egi.datacollector.util.actors.mapreduce;

import java.io.Serializable;
import java.util.Collection;

import akka.actor.UntypedActor;

import com.egi.datacollector.processor.mapreduce.IMapper;
import com.egi.datacollector.processor.mapreduce.KeyValue;
import com.egi.datacollector.processor.mapreduce.messages.MapMsg;
import com.egi.datacollector.processor.mapreduce.messages.MappedMsg;

public class MapActor<X, K extends Serializable, V extends Serializable> extends UntypedActor {
	
	private final IMapper<X, K, V> mapFunction;
	
	public MapActor(IMapper<X, K, V> m){
		mapFunction = m;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onReceive(Object arg0) throws Exception {
		if(arg0 instanceof MapMsg){
			MapMsg add = (MapMsg)arg0;
			
			Collection<KeyValue<K, V>> list = mapFunction.mapToKeyValue((X) add.getElement());
			getSender().tell(new MappedMsg<K, V>(list), getSelf());
			
		}

	}

}
