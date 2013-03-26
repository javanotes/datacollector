package com.egi.datacollector.listener.cluster;

import com.hazelcast.core.ILock;

public class ClusterLock {
	
	private final ILock theLock;
	protected ClusterLock(ILock lock){
		theLock = lock;
	}
	public boolean isLocked(){
		return theLock.tryLock();
	}
	public void unLock(){
		theLock.unlock();
	}

}
