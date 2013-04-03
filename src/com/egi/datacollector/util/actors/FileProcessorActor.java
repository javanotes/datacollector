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

import com.egi.datacollector.processor.ProcessorFactory;
import com.egi.datacollector.processor.file.RecordData;

class FileProcessorActor extends UntypedActor {
	
	private ActorRef worker = null;
	
	private static final Logger log = Logger.getLogger(FileProcessorActor.class);
	
	public FileProcessorActor(){
		super();
		worker = getContext().actorOf(new Props(Worker.class).withDispatcher("datacollector").withRouter(new SmallestMailboxRouter(2)));
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
		
		private int state = 0;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if(arg0 instanceof RecordData){
				RecordData record = (RecordData) arg0;
				
				ProcessorFactory.getProcessor(record).process(record);
			}
						
		}
		
				
		@Override
		public void preRestart(Throwable reason, Option<Object> message){
			//send it to the supervisor so that it can be enqueued once again to retry
			if(reason.getCause() instanceof SQLException){
	    		  log.error("Not retrying since there seems to be a problem with the SQL itself!");
	    		  super.preRestart(reason, message);
	    		  
	    	}
			else{
				log.warn(""+state+" Trying to redo a processing: "+((RecordData)message.get()).getRecord());
				
				getSelf().tell(message.get(), getSender());
				super.preRestart(reason, message);
			}
		}
		
	}

}
