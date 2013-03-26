package com.egi.datacollector.listener;

import java.util.concurrent.atomic.AtomicBoolean;

import com.egi.datacollector.listener.ListenerState.State;

public abstract class Listener {
	
	protected ListenerState state = new ListenerState(State.Void);
	
	protected AtomicBoolean stopSignalReceived = new AtomicBoolean(false);
	
	public abstract boolean isListening();
	public abstract void startListening(boolean retry);
	public abstract void stopListening();
	public abstract void checkState();
}
