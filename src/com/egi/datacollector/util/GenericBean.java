package com.egi.datacollector.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * DO NOT USE!
 * @author esutdal
 *
 */
@Deprecated
public abstract class GenericBean {
	
	/**
	 * Return the class field names
	 * @return
	 */
	protected abstract List<String> fields();
	
	private long _id;
	private final Map<String, Field> props = new HashMap<String, Field>();
		
	public void print(){
		System.out.println("--------- Begin ----------");
		for(Entry<String, Field> entry : props.entrySet()){
			try {
				System.out.println(entry.getKey() + " : " + entry.getValue().get(this));
			} catch (Exception e) {
				System.err.println(e.getMessage());
			} 
		}
		System.out.println("---------- End -----------");
	}
	
	private Field getField(String name){
		Field f = null;
		if (fields().contains(name)) {
			if (props.containsKey(name)) {
				f = props.get(name);
			} else {
				synchronized (props) {
					if (!props.containsKey(name)) {
						try {

							f = getClass().getDeclaredField(name .equals("default") ? "dflt" : name);
							props.put(name, f);
						} catch (SecurityException e) {

						} catch (NoSuchFieldException e) {
							System.err.println(e.getMessage());
						}
					}
				}
			}
		}
		return f;
	}
	
	/**
	 * Get the property value
	 * @param prop
	 * @return
	 */
	public String get(String prop){
		String value = null;
		Field f = getField(prop);
		if(f != null){
			try {
				value = (String) f.get(this);
			} catch (IllegalArgumentException e) {
				
			} catch (IllegalAccessException e) {
				
			}
		}
		return value;
		
	}
	
	/**
	 * Set the property value
	 * @param prop
	 * @param value
	 */
	public void set(String prop, String value){
		
		Field f = getField(prop);
		if(f != null){
			try {
				f.set(this, value);
			} catch (IllegalArgumentException e) {
				
			} catch (IllegalAccessException e) {
				
			} 
			 
		}
		 
	}

}
