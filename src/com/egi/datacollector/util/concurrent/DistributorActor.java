package com.egi.datacollector.util.concurrent;

import org.apache.log4j.Logger;

import com.egi.datacollector.listener.cluster.ClusterListener;
import com.logica.smscsim.ShortMessageValue;

import akka.actor.UntypedActor;

/**
 * This will run in the primary only
 * @author esutdal
 *
 */
class DistributorActor extends UntypedActor {
	
	private static final Logger log = Logger.getLogger(DistributorActor.class);

	@SuppressWarnings("deprecation")
	@Override
	public void onReceive(Object socketBytes) throws Exception {
		if(socketBytes instanceof byte[]){
			//this is not used
			ClusterListener.instance().addToClusterJobMap((byte[]) socketBytes);
			log.debug("Got a new data to set to distributed map");
			
		}
		else if(socketBytes instanceof ShortMessageValue){
			ClusterListener.instance().addToClusterJobMap((ShortMessageValue) socketBytes);
			
		}

	}

}
