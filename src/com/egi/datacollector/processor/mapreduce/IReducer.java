package com.egi.datacollector.processor.mapreduce;

import java.io.Serializable;
import java.util.Collection;

/**
 * Implement the class to provide the reduce logic. Will associate a collection of V for the key K.
 * Multiple threads will act on a single instance of this class. So synchronize instance variables if any accordingly.
 * @author esutdal
 *
 *
 * @param <K>	mapped key type
 * @param <V>	mapped value type
 */
public interface IReducer<K extends Serializable, V extends Serializable> {
	
	public KeyValue<K, V> reduceForKey(K key, Collection<V> values);
		

}
