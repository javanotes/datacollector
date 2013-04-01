package com.egi.datacollector.listener.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.egi.datacollector.listener.Listener;
import com.egi.datacollector.listener.ListenerState.State;
import com.egi.datacollector.startup.Main;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.concurrent.ActorFramework;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.logica.smscsim.ShortMessageValue;

/**
 * This is the class responsible for maintaining the peer to peer clustering using Hazelcast. 
 * It is different from other listeners since this will be active throughout the VM lifetime
 * @author esutdal
 *
 */
public class ClusterListener extends Listener implements Runnable {
	
	private static final Logger log = Logger.getLogger(ClusterListener.class);
		
	private Cluster cluster = null;
	
	//we will be needing these for short time tasks.  Since a member addition / removal operation should not occur very frequently
	private final ExecutorService worker = Executors.newCachedThreadPool(new ThreadFactory() {
		private int n = 0;
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "datacollector.cluster.listener.executor" + "-" +(n++));
			t.setDaemon(true);
			return t;
		}
	});
	
	
	private static ClusterListener _singleton = null;
	
	private ClusterListener(){
		
		state.set(State.Init);
	}
	
	public static ClusterListener instance(){
		
		if(_singleton == null){
			synchronized (ClusterListener.class) {
				if(_singleton == null)
					_singleton = new ClusterListener();
			}
			
		}
		return _singleton;
	}
	
	@Override
	public void startListening(boolean retry) {
		new Thread(this, "datacollector.cluster.listener").start();
			
	}
	
	@Override
	public void stopListening() {
		stopSignalReceived.set(true);
		
		if(cluster != null){
			cluster.stop();
		}
		worker.shutdown();
		try {
			worker.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
		}
		if(stopSignalReceived.compareAndSet(true, false)){
			Main._instanceLatchNotify();
			
		}
		
		//do not care about the worker thread group
		log.info("Stopped cluster listener");
	}
	
	@Override
	public void run() {
		
		checkState();
		if(state.get() == State.Runnable){
			state.set(State.Running);
			Main._instanceLatchNotify();
			log.info("Running Cluster listener");
			if (!Config.isClusteredModeEnabled()) {
				cluster.clearPendingEntries();
				log.info("Cleared pending entries");
			}
			
		}
				
	}
	
	public ClusterLock getFtpLock(){
		return cluster.ftp_lock();
	}
	
	public ClusterLock getSmppLock(){
		return cluster.smpp_lock();
	}

	/**
	 * NOT USED
	 * @return
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private static boolean contestToBringUpASingleNewMember(){
		/*
		 * This has to be done using a hazelcast cluster lock
		 */
		boolean valid = false;
		File f = new File(Config.getClusterLockFile());
		RandomAccessFile raf = null;
		FileLock lock = null;
		try {
			if(!(f.exists() && f.isFile())){
				if(f.createNewFile()){
					
				}
			}
			raf = new RandomAccessFile(f, "rw");
			
			FileChannel lockChannel = raf.getChannel();
			
			lock = lockChannel.tryLock();
			if(lock != null){
				//try to make this instance as the primary
				boolean primary = Main.makeInstancePrimary();
				if(primary){
					log.info("Taking over as primary");
					log.info("Now trying to bring up another instance");
					Main.startAnotherInstance(f);
					valid = true;
				
				}
			}
			
		}  catch (Throwable e) {
			log.warn("Could not invoke jvm process - " + e.getMessage());
		}
		finally{
			try {
				if(lock != null)
					lock.close();
			} catch (IOException e) {
				
			}
			try {
				raf.close();
			} catch (IOException e) {
				
			}
		}
		return valid;
		
	}
	
	public void removeFromClusterJobMap(Object entryKey){
		if(entryKey != null){
			cluster.remove((Long) entryKey);
		}
	}
	
	public void addToClusterJobMap(ShortMessageValue sms){
		log.debug("Got a new sms to set to distributed map: " + sms.getMessageId());
		//cluster.put(sms.getMessageId(), sms);
		cluster.put(sms);
	}
	@Deprecated
	public void addToClusterJobMap(byte [] dataBytes){
		
		cluster.put(dataBytes);
	}
	
	/**
	 * 
	 * @author esutdal
	 *
	 */
	private class TopicListener implements MessageListener<Object>{

		@Override
		public void onMessage(Message<Object> message) {
			synchronized(plannedShutDownRecvd){
				plannedShutDownRecvd.add(0);
			}
			
		}
		
	}
	
		
	/**
	 * 
	 * @author esutdal
	 *
	 */
	private class InstanceListener implements MembershipListener{

		@Override
		public void memberRemoved(final MembershipEvent event) {
			
								
			worker.execute(new Runnable() {
				
				@Override
				public void run() {
					
					//This logic has significance only if we have multiple instances on same node
					if (!Main.isAPrimaryInstance()) {
						boolean primary = cluster.tryMakeInstancePrimary();
						if (primary) {
							log.info("Took over as primary");

						}
						else{
							log.info("Did not take over as primary");
						}
					}
					else{
						log.info("Instance already running as a primary");
					}
					
					if(plannedShutDownRecvd.isEmpty()){
						log.info("Detected forced shutdown of member: " + event.getMember().toString());
						
						//This is a force kill of instance.
						//so do need to bring up another instance
						//this code can be used only if we are using a single node with multiple jvms
						//ideally the instances will reside on different nodes. there we would need
						//manual intervention or monitor scripts to bring up a new instance
												
						cluster.tryRestartMember(event.getMember());
						
					}
					else{
						log.info("Detected planned shutdown of member: " + event.getMember().toString());
						synchronized(plannedShutDownRecvd){
							plannedShutDownRecvd.remove();
						}
						//this else block should be reached by only one component, the one which successfully deques
						//this is a planned shutdown. do not bring up instance
						//but we don't know whether the primary was the victim!
						//so try to run fail-over
						
						//do nothing
					}
					
				}
			});
			
		}
		
		@Override
		public void memberAdded(final MembershipEvent event) {
			
								
			worker.execute(new Runnable() {
				
				@Override
				public void run() {
					log.info("Detected startup of member: " + event.getMember().toString());
					
				}
			});
		}
		
	}
	
	/**
	 * A local distributed map listener
	 * @author esutdal
	 *
	 */
	private class LocalMapListener implements EntryListener<Object, Object>{

		@Override
		public void entryAdded(EntryEvent<Object, Object> entry) {
			log.debug("Added entry");
			ActorFramework.instance().processDataFromDistributedMap(entry);
			
		}

		@Override
		public void entryEvicted(EntryEvent<Object, Object> arg0) {
			// do nothing
			
		}

		@Override
		public void entryRemoved(EntryEvent<Object, Object> arg0) {
			//do nothing
			
		}

		@Override
		public void entryUpdated(EntryEvent<Object, Object> arg0) {
			//do nothing			
		}
		
	}
	
	private final LinkedList<Integer> plannedShutDownRecvd = new LinkedList<Integer>();
	
	@Override
	public void checkState(){
		if(cluster == null){
			try {
				cluster = new Cluster();
				cluster.init(new InstanceListener(), new TopicListener(), new LocalMapListener());
				
			} catch (FileNotFoundException e) {
				log.fatal("Unable to read hazelcast config. Cluster listener not running!", e);
				state.set(State.Error);
				return;
			}
			
		}
		state.set(State.Runnable);
	}

	@Override
	public boolean isListening() {
		return state.get() == State.Running;
	}

}
