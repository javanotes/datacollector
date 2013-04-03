package com.egi.datacollector.util.actors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import scala.concurrent.duration.Duration;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;

import com.egi.datacollector.processor.file.RecordData;
import com.egi.datacollector.processor.mapreduce.IMapper;
import com.egi.datacollector.processor.mapreduce.IReducer;
import com.egi.datacollector.processor.mapreduce.KeyValue;
import com.egi.datacollector.processor.mapreduce.messages.EndProduceMsg;
import com.egi.datacollector.processor.mapreduce.messages.InputDataMsg;
import com.egi.datacollector.processor.smpp.SmppData;
import com.egi.datacollector.util.actors.mapreduce.MasterActor;
import com.hazelcast.core.EntryEvent;
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
		akka.awaitTermination(Duration.apply(1, TimeUnit.MINUTES));
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
		
		mapReducedLoader = akka.actorOf(new Props(new UntypedActorFactory() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -7969266935319752530L;

			@Override
			public Actor create() throws Exception {
				return new MapreduceProcessorActor();
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
	
		
		
	public void submitFileRecordToProcess(RecordData record){
		fileRecordLoader.tell(record, akka.guardian());
	}
	private AtomicBoolean mapReduceInit = new AtomicBoolean();
	
	private CountDownLatch mapReduceLatch = null;
	private Collection<KeyValue<String, String>> mapReduceResults = null;
	
	//TODO Can be done only after the map reduce functions are written
	private void initMapReduce(){
		if(mapReduceInit.compareAndSet(false, true)){
			
			mapReduceLatch = new CountDownLatch(1);
			mapReduceResults = new ArrayList<KeyValue<String, String>>();
			
			mapreduceMaster = akka.actorOf(new Props(new UntypedActorFactory() {
				
				
				/**
				 * 
				 */
				private static final long serialVersionUID = -2797565506163000340L;

				@Override
				public Actor create() {
					
					return new MasterActor<String, String, String>(new IMapper<String, String, String>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Collection<KeyValue<String, String>> mapToKeyValue(String string) {
							// TODO Auto-generated method stub
							return null;
						}
					}, new IReducer<String, String>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public KeyValue<String, String> reduceForKey(
								String key,
								Collection<String> values) {
							// TODO Auto-generated method stub
							return null;
						}
					}, mapReduceResults, mapReduceLatch, 0, 0, 0);
				}
			}));
			
			log.info("Starting mapreduce job");
		}
	}
	/**
	 * 
	 * @param mrData
	 */
	public void submitForMapReduce(RecordData mrData){
		initMapReduce();
		mapreduceMaster.tell(new InputDataMsg<RecordData>(mrData), akka.guardian());
	}
	
	public void processMappedAndReduced(){
		//map reduce to be stopped
		mapreduceMaster.tell(new EndProduceMsg(), akka.guardian());
		try {
			
			boolean finished = mapReduceLatch.await(30, TimeUnit.MINUTES);
			if(!finished){
				log.error("Mapreduce job timed out! Check logs");
			}
						
		} catch (InterruptedException e) {
			
		}
		mapReduceInit.compareAndSet(true, false);
		mapReducedLoader.tell(mapReduceResults, akka.guardian());
		
	}
	/**
	 * 
	 * @param theKey
	 */
	
	public void processDataFromDistributedMap(EntryEvent<Object, Object> mapEntry){
		smppLoader.tell(mapEntry, akka.guardian());
	}
	
	public void processDataFromDistributedMap(Object mapValue){
		smppLoader.tell(mapValue, akka.guardian());
	}
	
	private ActorRef smppDistributor = null;
	
	private ActorRef smppLoader = null;
	private ActorRef fileRecordLoader = null;
	private ActorRef mapreduceMaster = null;
	private ActorRef mapReducedLoader = null;
	
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

	public void submitDataToDistributedMap(SmppData sms) {
		smppDistributor.tell(sms, akka.guardian());
		
	}
		

}
