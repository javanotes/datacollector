package com.egi.datacollector.processor.mapreduce;

import java.io.Serializable;

/**
 * A key-value dictionary entry for each mapped entity
 * @author esutdal
 *
 * @param <K> mapped key type
 * @param <V> mapped value type
 */
public class KeyValue<K extends Serializable, V extends Serializable> implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -954413895987517744L;
	
	public KeyValue(K key, V value){
		
		this.key = key;
		this.value = value;
	}

	public K key;
	public V value;
}
