package com.egi.datacollector.listener;

public final class ListenerState {
	
	public static enum State {Init, Running, Error, Runnable, Void}
	
	private State _state = null;
	
	public ListenerState(State state){
		_state = state;
	}
	
	public void set(State state){
		_state = state;
	}
	
	public State get(){
		return _state;
		
	}

}
