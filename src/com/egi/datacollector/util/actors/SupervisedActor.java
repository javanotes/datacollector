package com.egi.datacollector.util.actors;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import scala.Option;
import akka.actor.UntypedActor;

import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.processor.ProcessorFactory;
import com.egi.datacollector.processor.file.data.RecordData;
import com.egi.datacollector.processor.file.data.RecordsData;
import com.egi.datacollector.processor.smpp.data.SmppData;
import com.egi.datacollector.util.actors.messages.Clear;
import com.egi.datacollector.util.actors.messages.Key;
import com.hazelcast.core.EntryEvent;
import com.logica.smpp.pdu.PDUException;

/**
 * The supervised actor. Would do the actual job of processing and is restarted
 * by the supervisor on a failure.
 * @author esutdal
 *
 */
class SupervisedActor extends UntypedActor {
	
	private static final Logger log = Logger.getLogger(SupervisedActor.class);

	@SuppressWarnings("rawtypes")
	@Override
	public void onReceive(Object processorJob) throws Exception {
		if(processorJob instanceof EntryEvent){
			Object data = ((EntryEvent) processorJob).getValue();
			if(data instanceof SmppData){
				SmppData job = ((SmppData) data);
				Processor processor = ProcessorFactory.getProcessor(job);
				processor.process(job);
				ClusterListener.instance().removeFromDistributableJobsMap(((EntryEvent) processorJob).getKey());
			}
			else if(data instanceof RecordData){
				getSender().tell(new Key(((EntryEvent) processorJob).getKey()), getSelf());
				getSender().tell(data, getSelf());
			}
		}
		else if(processorJob instanceof RecordsData){
			RecordsData job = ((RecordsData) processorJob);
			Processor processor = ProcessorFactory.getProcessor(job);
			processor.process(job);
			getSender().tell(new Clear(), getSelf());				
			
		}
		else{
			unhandled(processorJob);
		}
								
	}
	
	/**
	 * Define the retry scheme here
	 */
	@Override
	public void preRestart(Throwable reason, Option<Object> message){
		//send it to the supervisor so that it can be enqueued once again to retry
		if(reason.getCause() instanceof PDUException || reason.getCause() instanceof SQLException){ 
    		  log.error("Not retrying since there seems to be a problem with the data itself!");
    		  //super.preRestart(reason, message);
    		  
    	}
		else{
			log.warn("Trying to redo a processing");
			getSender().tell(message.get(), getSelf());
			//super.preRestart(reason, message);
		}
	}

}
