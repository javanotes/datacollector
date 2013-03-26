package com.egi.datacollector.listener.cluster;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.egi.datacollector.Main;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.concurrent.ActorFramework;
import com.egi.datacollector.util.exception.GeneralException;
import com.egi.datacollector.util.ssh.SshExecution;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.impl.MemberImpl;

/**
 * Class to act as a Hazelcast cluster member. <p><b>Note:</b>
 * If multicast communication doesn't work in your env, try to add this rule to iptables:<p>
 * <i>iptables -A INPUT -m pkttype --pkt-type multicast -j ACCEPT</i>
 * @author esutdal
 *
 */
class Cluster {
	
	public static final String CLUSTER_QUEUE = "DataCollectorDistributedQueue";
	public static final String CLUSTER_NAME = "DataCollectorCluster";
	public static final String CLUSTER_MUTEX = "CLUSTER_MUTEX";
	
	public static final String INSTANCE_SHUTDOWN_Topic = "INSTANCE_SHUTDOWN_Topic";
	public static final String INSTANCE_STARTUP_Q = "INSTANCE_STARTUP_Q";
	
	public static final String PERSISTENT_JOB_MAP = "persistentMap";
	
	static final String KEY_LOCK = "KEY_LOCK";
	static final String INSTANCE_LOCK = "INSTANCE_LOCK";
	static final String SEQ_GENERATOR = "SEQ_GENERATOR";
	static final String FTP_LOCK = "FTP_LOCK";
	static final String SMPP_LOCK = "SMPP_LOCK";
	static final String CLUSTER_LOCK = "CLUSTER_LOCK";
	
	private static final Logger log = Logger.getLogger(Cluster.class);
	
	private static HazelcastInstance hazelcast = null;
	
	void tryRestartMember(Member node){
		ILock lock = hazelcast.getLock(CLUSTER_LOCK);
		SshExecution ssh = null;
		boolean locked = lock.tryLock();
		if(locked){
			String host = node.getInetSocketAddress().getHostString();
			log.info("Restarting member node: " + host);
			try {
				ssh = new SshExecution(host, Config.sshUser(), Config.sshPassword());
				String result = ssh.runCommand(Config.sshCmdIsInstanceRunning());
				if(Integer.parseInt(result) == 0){
					log.debug("SSH login to node successful");
					//TODO run datacollector on node
					log.info("Started member");
				}
				
			} catch (GeneralException e) {
				log.warn("Exception while trying to restart member", e);
			}
			finally{
				if(ssh != null){
					ssh.end();
				}
				lock.unlock();
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
	private long getNextKeyForCluster(){
		ILock lock = hazelcast.getLock(KEY_LOCK);
		try {
			boolean locked = lock.tryLock(5, TimeUnit.SECONDS);
			if(locked){
				IList<Long> numGen = hazelcast.getList(SEQ_GENERATOR);
				//when using a persistent map, remember to get the value from persistent storage
				if(numGen.size() == 0){
					numGen.add(0L);
				}
				Long next = numGen.get(0) + 1;
				numGen.set(0, next);
				return next;
			}
		} catch (InterruptedException e) {
			
		}
		finally{
			if(lock != null){
				lock.unlock();
			}
		}
		return 0;
		
	}
	
	Object get(Object key){
		if(isRunning()){
			
			return hazelcast.getMap(PERSISTENT_JOB_MAP).get(key);
		}
		return null;
		
	}
	
	void remove(Object key){
		if(isRunning()){
			
			hazelcast.getMap(PERSISTENT_JOB_MAP).remove(key);
		}
	}
	void put(Object val){
		if(isRunning()){
			Long key = getNextKeyForCluster();
			hazelcast.getMap(PERSISTENT_JOB_MAP).put(key, val);
		}
	}
	
	void put(Object key, Object val){
		if(isRunning()){
			hazelcast.getMap(PERSISTENT_JOB_MAP).put(key, val);
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
	
	@SuppressWarnings("unused")
	private LifecycleState _memberState = LifecycleState.STARTING;
	
	void init(final MembershipListener clusterListener, MessageListener<Object> msgListener, EntryListener<Object, Object> localMapListener){
			
		hazelcast.getCluster().addMembershipListener(clusterListener);
		hazelcast.getTopic(INSTANCE_SHUTDOWN_Topic).addMessageListener(msgListener);
		
		hazelcast.getMap(PERSISTENT_JOB_MAP).addLocalEntryListener(localMapListener);
		
		
	}
	
	void broadcastPlannedShutdown(String id){
		if(isRunning()){
			
			hazelcast.getTopic(INSTANCE_SHUTDOWN_Topic).publish(id);
			
		}
	}
			
	public void stop(){
		if(hazelcast != null){
			broadcastPlannedShutdown(Main.getProcessId());
			hazelcast.getLifecycleService().shutdown();
		}
		
		
	}

}
