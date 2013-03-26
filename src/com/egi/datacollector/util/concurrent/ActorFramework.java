package com.egi.datacollector.util.concurrent;

import org.apache.log4j.Logger;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;

import com.egi.datacollector.processor.file.RecordData;
import com.hazelcast.core.EntryEvent;
import com.logica.smscsim.ShortMessageValue;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
/**
 * Would generate one distributor actor (with guaranteed distribution)
 * and one processor actor (with guaranteed processing and finite retries on error)
 * @author esutdal
 *
 */
public class ActorFramework {
	private static final Logger log = Logger.getLogger(ActorFramework.class);
	private ActorSystem akka = null;
	
	public void stop(){
		if(smppLoader != null){
			akka.stop(smppLoader);
		}
		if(smppDistributor != null){
			akka.stop(smppDistributor);
		}
		if(fileRecordLoader != null){
			akka.stop(fileRecordLoader);
		}
		akka.shutdown();
		log.info("Stopped actor system");
	}
	
	private ActorFramework(){
		akka = ActorSystem.create("actors", akkaConfig);
		
		smppLoader = akka.actorOf(new Props(new UntypedActorFactory() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -6926979874949770649L;

			@Override
			public Actor create() throws Exception {
				return new SmppProcessorActor();
			}
		}).withDispatcher("datacollector"));
		
		fileRecordLoader = akka.actorOf(new Props(new UntypedActorFactory() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -7969266935319752530L;

			@Override
			public Actor create() throws Exception {
				return new FileProcessorActor();
			}
		}).withDispatcher("datacollector"));
		
		smppDistributor = akka.actorOf(new Props(new UntypedActorFactory() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -6629560629348597346L;

			@Override
			public Actor create() throws Exception {
				return new DistributorActor();
			}
		}).withDispatcher("datacollector-durable"));
	}
	
	/**
	 * 
	 * @param pduBytes
	 */
	
	public void submitDataToDistributedMap(ShortMessageValue sms){
		smppDistributor.tell(sms, akka.guardian());
	}
	
	public void submitDataToDistributedMap(byte [] pduBytes){
		smppDistributor.tell(pduBytes, akka.guardian());
	}
	
	public void submitFileRecordToProcess(RecordData record){
		fileRecordLoader.tell(record, akka.guardian());
	}
	
	/**
	 * 
	 * @param theKey
	 */
	
	public void processDataFromDistributedMap(EntryEvent<Object, Object> mapEntry){
		smppLoader.tell(mapEntry, akka.guardian());
	}
	
	
	private ActorRef smppDistributor = null;
	private ActorRef smppLoader = null;
	private ActorRef fileRecordLoader = null;
	
	//TODO: from configuration file
	private final static Config akkaConfig = ConfigFactory.parseString(
			
			"datacollector.type = Dispatcher \n" +
			"datacollector.executor = fork-join-executor \n" +
			"datacollector.fork-join-executor.parallelism-min = 8 \n" +
			"datacollector.fork-join-executor.parallelism-factor = 3.0 \n" +
			"datacollector.fork-join-executor.parallelism-max = 64 \n" + 
			
			"datacollector-durable.type = Dispatcher \n" +
			"datacollector-durable.executor = fork-join-executor \n" +
			"datacollector-durable.fork-join-executor.parallelism-min = 8 \n" +
			"datacollector-durable.fork-join-executor.parallelism-factor = 3.0 \n" +
			"datacollector-durable.fork-join-executor.parallelism-max = 64 \n" + 
			"datacollector-durable.mailbox-type = akka.actor.mailbox.filebased.FileBasedMailboxType "
	);
	
	private static ActorFramework _singleton = null;
	
	public static ActorFramework instance(){
		if(_singleton == null){
			synchronized(ActorFramework.class){
				if(_singleton == null){
					_singleton = new ActorFramework();
				}
			}
		}
		return _singleton;
	}

}
