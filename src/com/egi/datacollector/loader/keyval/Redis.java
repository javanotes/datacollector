package com.egi.datacollector.loader.keyval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.exception.PersistenceException;

/**
 * Class to generate Redis poolable connection instances
 * @author Sutanu Dalui
 *
 */
class Redis {
	
	private static final Logger log = Logger.getLogger(Redis.class);
	private final Object pooledInstanceMonitor = new Object();
	private volatile boolean rejectRequest = false;
	private final List<RedisConnection> connectionPool = new ArrayList<RedisConnection>();
	
	protected String serverHost = "localhost";
	protected int serverPort = 6381;
	private Timer poolFreeTask = null;
	protected int poolExpiryCheckSecs = 60*30;
	protected int poolSize = 10;
	protected int poolWaitSecs = 5;
	protected long timeToIdle;
	protected long timeToLive;
	
	private static Redis _instance = null;
	
	private RedisConnection createNewPooledInstance() throws PersistenceException {
		
		RedisConnection poolit = new RedisConnection(serverHost, serverPort);
		return poolit;
		
	}
	
	protected Redis(){
		readConfig();
		startTimer();
	}
	
	private void readConfig() {
		/*
		Properties props = new Properties();
		try {*/
			
			serverHost = Config.getRedisServerHost();
			serverPort = Config.getRedisServerPort();
			poolExpiryCheckSecs = Config.getRedisPoolExpirySecs();
			poolSize = Config.getRedisPoolSize();
			poolWaitSecs = Config.getRedisPoolWaitSecs();
			timeToIdle = Config.getRedisTimeToIdle();
			timeToLive = Config.getRedisTimeToLive();
			
			log.debug("Config properties loaded");
		/*} catch (IOException e) {
			System.err.println("Could not read config! Going to use default values - " + e.getMessage());
			
		}*/
		
	}

	protected static Redis instance(){
		if(_instance == null){
			synchronized(Redis.class){
				if(_instance == null){
					_instance = new Redis();
										
				}
			}
		}
		return _instance;
	}
	
	
	
	protected void release(RedisConnection pooledInstance){
		if (pooledInstance != null) {
			synchronized (pooledInstanceMonitor) {
				//log.debug("pooledInstance.isFree(): " + pooledInstance.isFree());
				if (pooledInstance != null && !pooledInstance.isFree()) {
					//log.debug("Releasing pooled conn");
					pooledInstance.unlock();
					synchronized(pooledInstance){
						pooledInstance.notify();
					}
					pooledInstanceMonitor.notifyAll();
				}
				
			}
		}
	}
	
	/**
	 * clients can override this method to perform any cleanup
	 * do not throw any exceptions. swallow everything now!
	 */
	protected void onShutdown(){
		//do nothing by default
	}
	
	protected void stop(){
		if(!rejectRequest){
			rejectRequest = true;
			if(poolFreeTask != null)
				poolFreeTask.cancel();
			clean();
			onShutdown();
			log.info("Stopped Redis pool");
		}
		
	}
	
	private void startTimer(){
		if(poolFreeTask != null)
			poolFreeTask.cancel();
		
		poolFreeTask = new Timer("datacollector.redispool.expiry.task", true);
		poolFreeTask.schedule(new TimerTask() {
			
			@Override
			public void run() {
				log.debug("Expiring idle connections");
				if(!connectionPool.isEmpty()){
					synchronized (connectionPool) {
						
						for (Iterator<RedisConnection> iter = connectionPool.iterator();iter.hasNext();) {
							RedisConnection proxy = iter.next();
							if (proxy.expired(timeToLive, timeToIdle)) {
								synchronized (pooledInstanceMonitor) {

									if (proxy.expired(timeToLive, timeToIdle)) {

										proxy.close();
										iter.remove();
										log.debug("Removed expired pooled instance ..");

									}

								}
							}
						}
					}
				}
				log.debug("End run");
			}
		}, poolExpiryCheckSecs * 1000, poolExpiryCheckSecs * 1000); //configurable
				
		log.debug("Pool expiry task scheduled ..");
	}
	
	private void clean(){
		if(!connectionPool.isEmpty()){
			for(RedisConnection proxy : connectionPool){
				
				if(!proxy.isFree()){
					synchronized(proxy){
						try {
							if(!proxy.isFree()){
								proxy.wait();
							}
						} catch (InterruptedException e) {}
					}
					if(proxy.isFree()){
						
						proxy.close();
						
					}
				}
				else{
					
						proxy.close();
					
				}
			}
			//log.debug("Closed all pooled connections..");
		}
	}
	
	protected RedisConnection acquire() throws PersistenceException{
		RedisConnection instance = null;
		
		if(!rejectRequest){
			synchronized (connectionPool) {
				if (!connectionPool.isEmpty()) {
					
					//checking for a free instance
					for (RedisConnection freed : connectionPool) {
						if (freed.isFree()) {
							synchronized (pooledInstanceMonitor) {
								if (freed.isFree()) {
									freed.lock();
									instance = freed;
									break;
								}
							}
							
						}
					}
				}
				
				//all instances are busy or first instance to be created
				if (instance == null) {
					
					//pool size has not reached. create a new pooled instance
					if(connectionPool.size() < poolSize){
						RedisConnection freed = createNewPooledInstance();//retry to be implemented from client
						if (freed != null) {
							freed.lock();
							connectionPool.add(freed);
							instance = freed;
							
						}
					}
					//wait for a free connection
					else {
						try {
							pooledInstanceMonitor.wait(poolWaitSecs * 1000);
						} catch (InterruptedException e) {}
						
						for (RedisConnection freed : connectionPool) {
							if (freed.isFree()) {
								synchronized (pooledInstanceMonitor) {
									if (freed.isFree()) {
										freed.lock();
										instance = freed;
										break;
									}
								}
								
							}
						}
					}
			
				}
			}
		}
		else{
			throw new PersistenceException("Redis: Connection pool shutting down");
		}
		
		return instance;
		
	}

}
