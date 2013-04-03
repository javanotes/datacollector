package com.egi.datacollector.processor.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;

import com.egi.datacollector.processor.mapreduce.messages.EndProduceMsg;
import com.egi.datacollector.processor.mapreduce.messages.InputCollectionMsg;
import com.egi.datacollector.processor.mapreduce.messages.InputDataMsg;
import com.egi.datacollector.util.actors.mapreduce.MasterActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * 
 * 
 * @author esutdal
 *
 * @param <X> source object type (to be mapped)
 * @param <K> mapped key type
 * @param <V> mapped value type
 */
public class MapReduce<X extends Serializable, K extends Serializable, V extends Serializable>{
		
	private static final Logger log = Logger.getLogger(MapReduce.class);
	
	private IMapper<X, K, V> map = null;
	private IReducer<K, V> reduce = null;
	
	private transient ActorRef masterActor = null;
	private int partSize = 9;
	
	private int mappers = 10;
	private int reducers = 20;
	
	/**
	 * Set the number of mapper actors. Default 10
	 * @param mappers
	 */
	public void setMappers(int mappers) {
		this.mappers = mappers;
	}

	/**
	 * Set the number of reducer actors. Default 20
	 * @param reducers
	 */
	public void setReducers(int reducers) {
		this.reducers = reducers;
	}
	
	
	/**
	 * We are employing a parallel divide and conquer algorithm for the reduction phase.
	 * Since the total input size is unknown, it is upto the user to set a proper partition size for each division.
	 * 
	 * Do not use too high a value since that will decrease the level of parallelism in the reduction phase.
	 * Defaults to 999. Should ideally be < Integer.MAX_VALUE
	 * @param pSize
	 */
	public void setPartitionSize(int pSize){
		partSize = pSize;
	}
	/*
	 * The facade and the akka system need to coordinate amongst themselves.
	 * Facade will wait till the master actor completes processing. This signalling will
	 * be done through the latch
	 */
	private final transient CountDownLatch latch = new CountDownLatch(1);
		
	private final static Config akkaConfig = ConfigFactory.parseString(
			
				"datacollector.mapreduce-dispatcher.type = Dispatcher \n" +
				"datacollector.mapreduce-dispatcher.executor = fork-join-executor \n" +
				"datacollector.mapreduce-dispatcher.fork-join-executor.parallelism-min = 8 \n" +
				"datacollector.mapreduce-dispatcher.fork-join-executor.parallelism-factor = 3.0 \n" +
				"datacollector.mapreduce-dispatcher.fork-join-executor.parallelism-max = 64 \n" 
	);
			
	private final List<KeyValue<K, V>> results = new ArrayList<KeyValue<K, V>>();
	
	/**
	 * Instantiate the facade with a map and reduce function
	 * @param map	
	 * @param reduce
	 */
	public MapReduce(IMapper<X, K, V> map, IReducer<K, V> reduce){
		this.map = map;
		this.reduce = reduce;
		init();
	}
	
	private volatile boolean isAkkaInit = false;
	
	
	private void init(){
		if(!isAkkaInit){
			
			akka = ActorSystem.create("actors", akkaConfig);
			
			masterActor = akka.actorOf(new Props(new UntypedActorFactory() {
				
				
				/**
				 * 
				 */
				private static final long serialVersionUID = -2797565506163000340L;

				@Override
				public Actor create() {
					
					return new MasterActor<X, K, V>(map, reduce, results, latch, partSize, mappers, reducers);
				}
			}));
			isAkkaInit = true;
			log.info("Starting mapreduce job");
		}
		
	}
	
	private ActorSystem akka = null;
		
	/**
	 * For passing collection of data
	 * @param source
	 */
	public void execute(Collection<X> source){
		if(source != null){
			
			//start the system by passing message to the master Actor
			masterActor.tell(new InputCollectionMsg<X>(source), akka.guardian());
						
		}
		
	}
	
	/**
	 * For passing each data
	 * @param source
	 */
	public void execute(X eachData){
		if(eachData != null){
			
			//start the system by passing message to the master Actor
			masterActor.tell(new InputDataMsg<X>(eachData), akka.guardian());
						
		}
		
	}
				
	public Collection<KeyValue<K, V>> getResult(){
		masterActor.tell(new EndProduceMsg(), akka.guardian());
		try {
			//wait while the processing is complete
			
			boolean finished = latch.await(30, TimeUnit.MINUTES);
			if(!finished){
				log.error("Mapreduce job timed out! Check logs");
			}
						
		} catch (InterruptedException e) {
			
		}
		
		return results;
		
	}
		
			
}
