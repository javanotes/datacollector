package com.egi.datacollector.util.concurrent;

import akka.actor.UntypedActor;

import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.processor.smpp.SmppData;

/**
 * This will run in the primary only
 * @author esutdal
 *
 */
class DistributorActor extends UntypedActor {
	
		
	@Override
	public void onReceive(Object socketBytes) throws Exception {
		if(socketBytes instanceof SmppData){
			ClusterListener.instance().addToDistributableJobsMap((SmppData) socketBytes);
		}
		else
			unhandled(socketBytes);

	}

}
