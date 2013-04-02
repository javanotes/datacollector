package com.egi.datacollector.processor.mapreduce;

import java.io.Serializable;
import java.util.Collection;

/**
 * Implement the class to provide the reduce logic. 
 * Multiple threads will act on a single instance of this class. So synchronize instance variables if any accordingly.
 * @author esutdal
 *
 * @param <X>	source object type
 * @param <K>	mapped key type
 * @param <V>	mapped value type
 */
public interface IMapper<X,K extends Serializable, V extends Serializable> extends Serializable{
	
	public Collection<KeyValue<K, V>> mapToKeyValue(X each);
	
}
