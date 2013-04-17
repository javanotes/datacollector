package com.egi.datacollector.listener.cluster;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.egi.datacollector.processor.file.data.RecordData;
import com.egi.datacollector.processor.smpp.data.SmppData;
import com.egi.datacollector.server.Main;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.Constants;
import com.egi.datacollector.util.actors.ActorFramework;
import com.egi.datacollector.util.exception.ClusterException;
import com.egi.datacollector.util.exception.SystemException;
import com.egi.datacollector.util.ssh.SshExecution;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InstanceDestroyedException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionService;

/**
 * Class to act as a Hazelcast cluster member. <p><b>Note:</b>
 * If multicast communication doesn't work in your env, try to add this rule to iptables:<p>
 * <i>iptables -A INPUT -m pkttype --pkt-type multicast -j ACCEPT</i>
 * @author esutdal
 *
 */
class Cluster {
	
	/*
	 * The broadcast address for an IPv4 host can be obtained by performing a bitwise OR operation 
	 * between the bit complement of the subnet mask and the host's IP address.

	   Example: For broadcasting a packet to an entire IPv4 subnet using the private IP address space 172.16.0.0/12, 
	   which has the subnet mask 255.240.0.0, the broadcast address is 172.16.0.0 | 0.15.255.255 = 172.31.255.255.
	 */
	
	public static final String CLUSTER_QUEUE = "DataCollectorDistributedQueue";
	public static final String CLUSTER_NAME = "DataCollectorCluster";
	public static final String CLUSTER_MUTEX = "CLUSTER_MUTEX";
	
	public static final String INSTANCE_SHUTDOWN_Topic = "INSTANCE_SHUTDOWN_Topic";
	public static final String MAP_REDUCE_TOPIC = "mapReduceBroadcast";
	public static final String INSTANCE_STARTUP_Q = "INSTANCE_STARTUP_Q";
	
	public static final String PERSISTENT_JOB_MAP = "distributableJobs";
	public static final String MAPREDUCE_JOB_MAP = "mapReduceJobs";
	
	public static final String CLUSTER_UNIQUE_NUMBER_GEN = "uniqNumGen";
	
	static final String KEY_LOCK = "KEY_LOCK";
	static final String INSTANCE_LOCK = "INSTANCE_LOCK";
	static final String CLUSTER_UNIQUE_NUMBER = "uniqNum";
	static final String FTP_LOCK = "FTP_LOCK";
	static final String SMPP_LOCK = "SMPP_LOCK";
	static final String CLUSTER_LOCK = "CLUSTER_LOCK";
	static final String BARRIER_LATCH = "BARRIER_LATCH";
	
	private static final Logger log = Logger.getLogger(Cluster.class);
	
	private static HazelcastInstance hazelcast = null;
	
	boolean latch() {
		if(isRunning()){
			ICountDownLatch latch = hazelcast.getCountDownLatch(BARRIER_LATCH);
			if(latch != null){
				return latch.setCount(noOfMembers());
			}
		}
		return false;
	}
	boolean latch(int count){
		if(isRunning()){
			ICountDownLatch latch = hazelcast.getCountDownLatch(BARRIER_LATCH);
			if(latch != null){
				return latch.setCount(count);
			}
		}
		return false;
	}
	void await() throws ClusterException{
		if(isRunning()){
			ICountDownLatch latch = hazelcast.getCountDownLatch(BARRIER_LATCH);
			if(latch != null){
				try {
					latch.await();
				} catch (MemberLeftException e) {
					log.error("Exception while waiting on latch", e);
					throw new ClusterException(e);
				} catch (InstanceDestroyedException e) {
					log.error("Exception while waiting on latch", e);
					throw new ClusterException(e);
				} catch (InterruptedException e) {
					log.error("Exception while waiting on latch", e);
					throw new ClusterException(e);
				}
			}
		}
	}
	void await(long time, TimeUnit unit) throws ClusterException{
		if(isRunning()){
			ICountDownLatch latch = hazelcast.getCountDownLatch(BARRIER_LATCH);
			if(latch != null){
				try {
					latch.await(time, unit);
				} catch (MemberLeftException e) {
					log.error("Exception while waiting on latch", e);
					throw new ClusterException(e);
				} catch (InstanceDestroyedException e) {
					log.error("Exception while waiting on latch", e);
					throw new ClusterException(e);
				} catch (InterruptedException e) {
					log.error("Exception while waiting on latch", e);
					throw new ClusterException(e);
				}
			}
		}
	}
	void countdown(){
		if(isRunning()){
			ICountDownLatch latch = hazelcast.getCountDownLatch(BARRIER_LATCH);
			if(latch != null){
				latch.countDown();
			}
		}
	}
	
	/**
	 * TODO
	 * @param node
	 */
	void tryRestartMember(Member node){
		if (!node.isLiteMember()) {
			
			ILock lock = hazelcast.getLock(CLUSTER_LOCK);
			SshExecution ssh = null;
			boolean locked = lock.tryLock();
			if (locked) {
				String host = node.getInetSocketAddress().getHostString();
				
				try {
					ssh = new SshExecution(host, Config.sshUser(), Config.sshPassword());
					String result = ssh.runCommand(Config.sshCmdIsInstanceRunning());
					
					if (Integer.parseInt(result) == 0) {
						log.info("Restarting member node: " + host);
						result = ssh.runCommand(Config.sshCmdStartInstance());
						if (Integer.parseInt(result) == 0){
							log.info("Started member");
							try {
								//block this lock and allow for remote instance to startup completely
								Thread.sleep(2000);
							} catch (InterruptedException e) {
								
							}
						}
						else{
							log.warn("Seems like remote member did not start!");
						}
					}

				} catch (SystemException e) {
					log.warn("Exception while trying to restart member", e);
				} finally {
					if (ssh != null) {
						ssh.end();
					}
					lock.unlock();
				}
			}
		}
	}
	
	int noOfMembers(){
		if(isRunning()){
			return hazelcast.getCluster().getMembers().size();
		}
		return 0;
	}
	
	final ClusterLock ftp_lock(){
		return new ClusterLock(hazelcast.getLock(FTP_LOCK));
	}
	
	final ClusterLock smpp_lock(){
		return new ClusterLock(hazelcast.getLock(SMPP_LOCK));
	}
	
	/**
	 * If the particular instance is running an active hazelcast service
	 * @return
	 */
	private boolean isRunning(){
		return hazelcast != null && hazelcast.getLifecycleService() != null && hazelcast.getLifecycleService().isRunning();
	}
	
	boolean tryMakeInstancePrimary(){
		if(isRunning()){
			ILock lock = hazelcast.getLock(INSTANCE_LOCK);
			try {
				
				if(lock.tryLock()){
					return Main.makeInstancePrimary();
				}
			} 
			finally{
				if(lock != null){
					lock.unlock();
				}
			}
		}
		return false;
		
	}
	private void releaseKey(long key){
		ILock lock = hazelcast.getLock(KEY_LOCK);
		try {
			boolean locked = lock.tryLock(15, TimeUnit.SECONDS);
			if(locked){
				
				IList<Long> numGen = hazelcast.getList(CLUSTER_UNIQUE_NUMBER);
				numGen.add(key);
				
			}
			
			
		} catch (InterruptedException e) {
			
		}
		finally{
			if(lock != null){
				lock.unlock();
			}
		}
		
	}
	private long acquireKey(){
		ILock lock = hazelcast.getLock(KEY_LOCK);
		try {
			boolean locked = lock.tryLock(15, TimeUnit.SECONDS);
			if(locked){
				
				IList<Long> numGen = hazelcast.getList(CLUSTER_UNIQUE_NUMBER);
				
				if(!numGen.isEmpty()){
					return numGen.remove(0);
				}
				
			}
			
			return hazelcast.getIdGenerator(CLUSTER_UNIQUE_NUMBER_GEN).newId();
			
		} catch (InterruptedException e) {
			
		}
		finally{
			if(lock != null){
				lock.unlock();
			}
		}
		return 0;
		
	}
	
	Object get(String distributedMapName, Object key){
		if(isRunning()){
			
			return hazelcast.getMap(PERSISTENT_JOB_MAP) != null ? hazelcast.getMap(PERSISTENT_JOB_MAP).get(key) : null;
		}
		return null;
		
	}
	
	void remove(String distributedMapName, Object key){
		if(isRunning()){
			if(hazelcast.getMap(distributedMapName) != null){
				hazelcast.getMap(distributedMapName).remove(key);
				if(key instanceof Long)
					releaseKey((Long) key);
			}
		}
	}
		
	void put(String distributedMapName, Object key, Object val){
		if(isRunning()){
			if(hazelcast.getMap(distributedMapName) != null)
				hazelcast.getMap(distributedMapName).put(key, val);
		}
	}
	
	/**
	 * 
	 * @throws FileNotFoundException
	 */
	Cluster() throws FileNotFoundException{
				
		String configFilename = Config.getHazelcastConfig();
		com.hazelcast.config.Config hzConfig;
		try {
			hzConfig = new FileSystemXmlConfig(configFilename);
			
			//we don't use redis store then
			if(hzConfig.getMapConfig(PERSISTENT_JOB_MAP) != null && hzConfig.getMapConfig(PERSISTENT_JOB_MAP).getMapStoreConfig() != null){
				if (Config.isClusteredModeEnabled()) {
					hzConfig.getMapConfig(PERSISTENT_JOB_MAP).getMapStoreConfig().setEnabled(false);
					hzConfig.getMapConfig(PERSISTENT_JOB_MAP).setBackupCounts(1, 0);
				}
				else{
					hzConfig.getMapConfig(PERSISTENT_JOB_MAP).getMapStoreConfig().setEnabled(true);
					hzConfig.getMapConfig(PERSISTENT_JOB_MAP).setBackupCounts(0, 0);
				}
			}
			
			hazelcast = Hazelcast.newHazelcastInstance(hzConfig);
			
			hazelcast.getLifecycleService().addLifecycleListener(new LifecycleListener() {
				
				@Override
				public void stateChanged(LifecycleEvent lifecycleevent) {
					_memberState = lifecycleevent.getState();
					
				}
			});
						
			
		} catch (FileNotFoundException e) {
			throw e;
		}
		
		
	}
	
	void clearPendingEntries() {
		IMap<Object, Object> map = hazelcast.getMap(PERSISTENT_JOB_MAP);
		for(Object key : map.localKeySet()){
			//a dummy entry event
			EntryEvent<Object, Object> tempEntry = new EntryEvent<Object, Object>(hazelcast.getName(), new MemberImpl(), 1, key, map.get(key));
			ActorFramework.instance().processDataFromDistributedMap(tempEntry);
			
		}
		
	}
	
	private PartitionService partitionService = null;
	
	@SuppressWarnings("unused")
	private LifecycleState _memberState = LifecycleState.STARTING;
	
	void setupMapReduceCommChannel(MessageListener<Object> msgListener, EntryListener<Object, Object> entryListener){
		hazelcast.getTopic(MAP_REDUCE_TOPIC).addMessageListener(msgListener);
		hazelcast.getMap(MAPREDUCE_JOB_MAP).addLocalEntryListener(entryListener);
	}
			
	void init(final MembershipListener clusterListener, MessageListener<Object> msgListener, EntryListener<Object, Object> localMapListener){
			
		hazelcast.getCluster().addMembershipListener(clusterListener);
		hazelcast.getTopic(INSTANCE_SHUTDOWN_Topic).addMessageListener(msgListener);
		hazelcast.getMap(PERSISTENT_JOB_MAP).addLocalEntryListener(localMapListener);
		
		//this is for sort of broadcast messages
		hazelcast.getMap(PERSISTENT_JOB_MAP).addEntryListener(new EntryListener<Object, Object>() {

			@Override
			public void entryAdded(EntryEvent<Object, Object> entry) {
				//this is a end-of-file entry. all instances should know it
				ActorFramework.instance().processDataFromDistributedMap(entry);
			}

			@Override
			public void entryEvicted(EntryEvent<Object, Object> entryevent) {
				
				
			}

			@Override
			public void entryRemoved(EntryEvent<Object, Object> entryevent) {
				
			}

			@Override
			public void entryUpdated(EntryEvent<Object, Object> entry) {
				//this is a end-of-file entry. all instances should know it
				ActorFramework.instance().processDataFromDistributedMap(entry);
			}
			
		}, Constants.EOF, true);
		
		if(Config.isClusteredModeEnabled()){
			/**
			 * Adding a partition service for handling entry migration
			 */
			partitionService = hazelcast.getPartitionService();
			partitionService.addMigrationListener(new MigrationListener() {
				
				@Override
				public void migrationStarted(MigrationEvent migrationevent) {
					//log.info("Partition migration initiated");
					
				}
				
				@Override
				public void migrationFailed(MigrationEvent migrationevent) {
					log.warn("Partition migration failed!");
					
				}
				
				@Override
				public void migrationCompleted(MigrationEvent migrationevent) {
					if(migrationevent.getNewOwner().localMember()){
						IMap<Object, Object> map = hazelcast.getMap(PERSISTENT_JOB_MAP);
						for(Object key : map.localKeySet()){
							if(partitionService.getPartition(key).getPartitionId() == migrationevent.getPartitionId()){
								EntryEvent<Object, Object> entry = new EntryEvent<Object, Object>(hazelcast.getName(), hazelcast.getCluster().getLocalMember(), 
										EntryEventType.ADDED.getType(), key, map.get(key));
								log.debug("Processing data received from partition migration");
								ActorFramework.instance().processDataFromDistributedMap(entry);
							}
						}
					}
										
				}
			});
		}
		
	}
	
	void broadcastPlannedShutdown(String id){
		if(isRunning()){
			
			hazelcast.getTopic(INSTANCE_SHUTDOWN_Topic).publish(id);
			
		}
	}
	
	void broadcast(String distributedTopicName, Object code){
		if(isRunning()){
			
			hazelcast.getTopic(distributedTopicName).publish(code);
			
		}
	}
			
	void stop(){
		if(hazelcast != null){
			broadcastPlannedShutdown(Main.getProcessId());
			hazelcast.getLifecycleService().shutdown();
		}
		
		
	}

	/**
	 * Adding a job to the distributed Map
	 * @param smppData
	 */
	void put(SmppData smppData) {
		if(isRunning()){
			Long key = acquireKey();
			hazelcast.getMap(PERSISTENT_JOB_MAP).put(key, smppData);
		}
		
	}

	void putMR(RecordData fileRecord){
		if(isRunning()){
			Object key = fileRecord.isEof() ? Constants.EOF : acquireKey();
			hazelcast.getMap(MAPREDUCE_JOB_MAP).put(key, fileRecord);
		}
	}
	void put(RecordData fileRecord) {
		if(isRunning()){
			Object key = fileRecord.isEof() ? Constants.EOF : acquireKey();
			hazelcast.getMap(PERSISTENT_JOB_MAP).put(key, fileRecord);
		}
		
	}
	

}
