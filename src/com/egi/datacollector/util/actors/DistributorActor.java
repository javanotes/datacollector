package com.egi.datacollector.util.actors;

import akka.actor.UntypedActor;

import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.processor.file.data.RecordData;
import com.egi.datacollector.processor.smpp.data.SmppData;

/**
 * This will run in the primary only
 * @author esutdal
 *
 */
class DistributorActor extends UntypedActor {
	
		
	@Override
	public void onReceive(Object distributableData) throws Exception {
		if(distributableData instanceof SmppData){
			ClusterListener.instance().addToDistributableJobsMap((SmppData) distributableData);
		}
		else if(distributableData instanceof RecordData){
			ClusterListener.instance().addToDistributableJobsMap((RecordData) distributableData);
		}
		else
			unhandled(distributableData);

	}

}
