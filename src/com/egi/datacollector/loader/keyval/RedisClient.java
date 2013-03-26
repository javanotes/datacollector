package com.egi.datacollector.loader.keyval;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.egi.datacollector.util.exception.PersistenceException;

/**
 * The interface to be used by Hazelcast to persist the distributed map
 * @author esutdal
 *
 */
public class RedisClient{
	
	private static final Logger log = Logger.getLogger(RedisClient.class);

	private RedisClient(){}
	
	private static Object toObject(String serialized){
		Object object = null;
		if(serialized != null){
			char [] chars = serialized.toCharArray();
			byte [] bytes = new byte [chars.length];
			for(int i=0; i<chars.length; i++){
				bytes[i] = (byte) chars[i];
			}
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(bais);
				object = ois.readObject();
			} catch (Exception e) {
				
			}
		}
		return object;
		
	}
	private static String toBytes(Serializable object){
		byte [] bytes = null;
		if(object != null){
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = null;
			try {
				oos = new ObjectOutputStream(baos);
				oos.writeObject(object);
				bytes = baos.toByteArray();
				
				char [] chars = new char [bytes.length];
				for(int i=0; i<bytes.length; i++){
					chars[i] = (char) bytes[i];
				}
				return new String(chars);
			} catch (IOException e) {
				
			}
			finally{
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
						
					}
				}
				try {
					baos.close();
				} catch (IOException e) {
					
				}
			}
		}
		return null;
		
	}
	
	public static void shutdown(){
		Redis.instance().stop();
	}
	
	public static void set(String key, Serializable value){
		RedisConnection c = null;
		try {
			c = Redis.instance().acquire();
			if(c!=null){
				c.setKeyValue(key, toBytes(value));
				log.debug("");
			}
		} catch (PersistenceException e) {
			log.error("Error", e);
		}
		finally{
			Redis.instance().release(c);
		}
	}
	
	public static Object get(String key){
		RedisConnection c = null;
		try {
			c = Redis.instance().acquire();
			if(c!=null){
				String serialized = c.getKeyValue(key);
				if("-1".equals(serialized)){
					return null;
				}
				else{
					return toObject(serialized);
				}
			}
		} catch (PersistenceException e) {
			log.error("Error", e);
		}
		finally{
			Redis.instance().release(c);
		}
		return key;
	}
		
	public static Map<String, Object> loadAll(final Collection<String> objectKeys) {
		RedisConnection c = null;
		Map<String, Object> values = new HashMap<String, Object>();
		if (objectKeys != null && !objectKeys.isEmpty()) {
			try {
				c = Redis.instance().acquire();
				if (c != null) {
					String [] keys = new String [objectKeys.size()];
					int i=0;
					for(String l : objectKeys){
						keys[i++] = (l);
					}
					List<String> serialized = c.getAllKeyValue(keys);
					if(serialized != null && serialized.size() == objectKeys.size()){
						i=0;
						for(String l : objectKeys){
							values.put(l, toObject(serialized.get(i++)));
						}
												
					}
					else{
						log.error("Not all keys are mapped to values!");
					}
					
				}
			} catch (PersistenceException e) {
				log.error("Error", e);
			} finally {
				Redis.instance().release(c);
			}
		}
		return values;
	}

	public static Set<String> loadAllKeys() {
		RedisConnection c = null;
		Set<String> allKeys = new HashSet<String>();
		try {
			c = Redis.instance().acquire();
			if(c!=null){
				Set<String> keys = c.getAllKeys();
				if(keys != null){
					for(String key : keys){
						try {
							allKeys.add(key);
						} catch (NumberFormatException e) {
							log.warn("Invalid Long key - " + key);
						}
					}
				}
			}
		} catch (PersistenceException e) {
			log.error("Error", e);
		}
		finally{
			Redis.instance().release(c);
		}
		return allKeys;
	}

	public static void delete(String arg0) {
		RedisConnection c = null;
		
		try {
			c = Redis.instance().acquire();
			if(c!=null){
				c.deleteKey(arg0);
			}
		} catch (PersistenceException e) {
			log.error("Error", e);
		}
		finally{
			Redis.instance().release(c);
		}
		
		
	}

	public static void deleteAll(Collection<String> keys) {
		RedisConnection c = null;
		
		if (keys != null) {
			String [] keyArr = new String [keys.size()];
			int i=0;
			for(String key : keys){
				keyArr[i++] = key;
			}
			try {
				c = Redis.instance().acquire();
				if (c != null) {
					c.deleteKey(keyArr);
				}
			} catch (PersistenceException e) {
				log.error("Error", e);
			} finally {
				Redis.instance().release(c);
			}
		}
		
	}

	public static void storeAll(Map<String, Object> keyVal) {
		RedisConnection c = null;
		
		if (keyVal != null && !keyVal.isEmpty()) {
			List<String> all = new ArrayList<String>();
			for(Entry<String, Object> entry : keyVal.entrySet()){
				all.add(entry.getKey());
				all.add(toBytes((Serializable) entry.getValue()));
			}
			String [] keyArr = new String [all.size()];
			int i=0;
			for(String each : all){
				keyArr[i++] = each;
			}
			try {
				c = Redis.instance().acquire();
				if (c != null) {
					c.setAllKeyValue(keyArr);
				}
			} catch (PersistenceException e) {
				log.error("Error", e);
			} finally {
				Redis.instance().release(c);
			}
		}
		
	}
}
