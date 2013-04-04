package com.egi.datacollector.util.actors;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import scala.Option;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.UntypedActor;
import akka.japi.Function;
import akka.routing.SmallestMailboxRouter;

import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.processor.ProcessorFactory;
import com.egi.datacollector.processor.file.RecordData;
import com.egi.datacollector.processor.smpp.SmppData;
import com.egi.datacollector.util.Config;
import com.hazelcast.core.EntryEvent;
import com.logica.smpp.pdu.PDUException;

/**
 * A retry implemented actor for executing processing
 * @author esutdal
 *
 */
class ProcessorActor extends UntypedActor {
	
	private static final Logger log = Logger.getLogger(ProcessorActor.class);
	
	private ActorRef worker = null;
	
	public ProcessorActor(){
		super();
		worker = getContext().actorOf(new Props(Worker.class).withDispatcher("datacollector").withRouter(new SmallestMailboxRouter(Config.getNoOfProcessorActors())));
	}

	
	@Override
	public void onReceive(Object arg0) throws Exception {
		worker.tell(arg0, getSelf());

	}
	
	private static SupervisorStrategy strategy = new OneForOneStrategy(10, Duration.apply(1, TimeUnit.MINUTES),
		    new Function<Throwable, Directive>() {
		      @Override
		      public Directive apply(Throwable t) {
		    	  		    	  
		    	  /*
		    	   * From the docs:
		    	   * "..The new actor then resumes processing its mailbox, meaning that the restart is not visible outside of the actor itself 
		    	   * with the notable exception that the message during which the failure occurred is not re-processed..."
		    	   * 
		    	   * We handle this as as:
		    	   * The erring child has already given up  its message to the supervisor. So this message will again be sent to any of the 
		    	   * workers (and thus attempted to be re-processed)
		    	  
		    	   */
		    	 return SupervisorStrategy.restart();
		    	 		        
		      }
		    });
		 
	@Override
	public SupervisorStrategy supervisorStrategy() {
	  return strategy;
	}
		
	
	private static class Worker extends UntypedActor{

		@SuppressWarnings({ "rawtypes"})
		@Override
		public void onReceive(Object hazelcastEntry) throws Exception {
			if(hazelcastEntry instanceof EntryEvent){
				Object data = ((EntryEvent) hazelcastEntry).getValue();
				if(data instanceof SmppData){
					SmppData job = ((SmppData) data);
					Processor processor = ProcessorFactory.getProcessor(job);
					processor.process(job);
					ClusterListener.instance().removeFromDistributableJobsMap(((EntryEvent) hazelcastEntry).getKey());
				}
				else if(data instanceof RecordData){
					RecordData job = ((RecordData) data);
					Processor processor = ProcessorFactory.getProcessor(job);
					processor.process(job);
					ClusterListener.instance().removeFromDistributableJobsMap(((EntryEvent) hazelcastEntry).getKey());
				}
			}
									
		}
		
				
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

}
