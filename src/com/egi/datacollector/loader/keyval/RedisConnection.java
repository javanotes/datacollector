package com.egi.datacollector.loader.keyval;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import com.egi.datacollector.util.exception.PersistenceException;


/**
 * A single Redis connection instance. This instance is poolable.
 * @author Sutanu Dalui
 *
 */
class RedisConnection {
	private static final Logger log = Logger.getLogger(RedisConnection.class);
	private Jedis clientConnection = null;
	private boolean _free = true;
	private final long initTime;
	private long modifiedTime;
	
	static final String NO_OF_ARGS = "*_\r\n";
	static final String NO_OF_BYTES = "$_\r\n";
	static final String CMD = "_\r\n";
	
	void setKeyValue(String key, String value){
		if(clientConnection != null){
			String reply = clientConnection.set(key, value);
			log.debug(reply);
		}
	}
	
	void setAllKeyValue(String...keyVals){
		if(clientConnection != null){
			clientConnection.mset(keyVals);
		}
	}
	
	String getKeyValue(String key){
		if(clientConnection != null){
			return clientConnection.get(key);
			
		}
		return "";
	}
	
	Set<String> getAllKeys(){
		if(clientConnection != null){
			return clientConnection.keys("*");
		}
		return null;
		
	}
	
	boolean deleteKey(String...key){
		if(clientConnection != null){
			return clientConnection.del(key) > 0;
		}
		return false;
		
	}
		
	List<String> getAllKeyValue(String [] keyArr){
		if(clientConnection != null){
			
			//if any key value is null then the logic wont work perfectly!
			return clientConnection.mget(keyArr);
			
			
		}
		return null;
		
	}
	
	void close(){
		
		if(clientConnection != null){
			clientConnection.quit();
			clientConnection.disconnect();
			clientConnection = null;
		}
		
		log.debug("Closed redis connection");
	}
	
	RedisConnection(String host, int port) throws PersistenceException{
		clientConnection = new Jedis(host, port);
					
		try {
			clientConnection.connect();
			initTime = System.currentTimeMillis();
		} catch (JedisException e) {
			log.error("Cannot connect to Redis server: ", e);
			throw new PersistenceException(e);
		}
				
		log.debug("New pooled instance created ..");
	}
	
	boolean isFree(){
		return _free;
	}
	
	boolean expired(long timeToLive, long timeToIdle){
		long now = System.currentTimeMillis();
		return _free && ((now - initTime) > timeToLive) && ((now - modifiedTime) > timeToIdle);
	}
	
	boolean lock(){
		if(_free){
			_free = false;
		}
		log.debug("Locked instance");
		return !_free;
	}
	
	void unlock(){
		modifiedTime = System.currentTimeMillis();
		_free = true;
		log.debug("UnLocked instance");
		
	}
	

}
