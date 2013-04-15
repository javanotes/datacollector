package com.egi.datacollector.util.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

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
import com.egi.datacollector.processor.file.data.RecordData;
import com.egi.datacollector.processor.file.data.RecordsData;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.actors.messages.Clear;
import com.egi.datacollector.util.actors.messages.Key;

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
		worker = getContext().actorOf(new Props(WorkerActor.class).withDispatcher("datacollector").withRouter(new SmallestMailboxRouter(Config.getNoOfProcessorActors())));
	}

	private final List<Object> entryKeys = new ArrayList<Object>();
	private final RecordsData records = new RecordsData();
		
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof Key){
			entryKeys.add(((Key)msg).key);
		}
		else if(msg instanceof Clear){
			if(!entryKeys.isEmpty()){
				for(Object entryKey : entryKeys){
					ClusterListener.instance().removeFromDistributableJobsMap(entryKey);
				}
				entryKeys.clear();
			}
			ClusterListener.instance().countdownClusterLatch();
		}
		else if(msg instanceof RecordData){
			
			if(((RecordData) msg).isEof()){
				worker.tell(records.copy(), getSelf());
				records.clear();
			}
			else{
				records.add((RecordData) msg);
			}
		}
		else{
			worker.tell(msg, getSelf());
		}

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
	
	

}
