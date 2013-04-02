package com.egi.datacollector.listener.smpp;

import java.io.IOException;
import java.net.BindException;

import org.apache.log4j.Logger;

import com.egi.datacollector.listener.Listener;
import com.egi.datacollector.listener.ListenerState.State;
import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.listener.cluster.ClusterLock;
import com.egi.datacollector.processor.smpp.SmppData;
import com.egi.datacollector.server.Main;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.concurrent.ActorFramework;
import com.logica.smpp.pdu.SubmitSM;
import com.logica.smscsim.DeliveryInfoSender;
import com.logica.smscsim.PDUProcessorGroup;
import com.logica.smscsim.SMSCListener;
import com.logica.smscsim.SMSCSession;
import com.logica.smscsim.ShortMessageStore;
import com.logica.smscsim.SimulatorPDUProcessor;
import com.logica.smscsim.SimulatorPDUProcessorFactory;

public class SmppListener extends Listener implements Runnable {
	
	//private ServerSocketChannel server = null;
	private static final Logger log = Logger.getLogger(SmppListener.class);
	
	private SMSCListener smscListener = null;
    private SimulatorPDUProcessorFactory factory = null;
    private PDUProcessorGroup processors = null;
    private ShortMessageStore messageStore = null;
    private DeliveryInfoSender deliveryInfoSender = null;
    
    /**
     * 
     * @author esutdal
     *
     */
    private class SmppMessageStore extends ShortMessageStore{
    	
    	SmppMessageStore(){
    		super();
    	}
    	@Override
    	public void submit(SubmitSM message, String messageId, String systemId)
		{
			
			SmppData sms = new SmppData(message.getSourceAddr().getAddress(), message.getDestAddr().getAddress(), message.getShortMessage());
			
			ActorFramework.instance().submitDataToDistributedMap(sms);
		}
    }
	
    private void socketListen() throws IOException{
    	if (retry) {
			ClusterLock lock = ClusterListener.instance().getSmppLock();
			if (lock.isLocked()) {
				try {
					try {
						smscListener.start();
						state.set(State.Runnable);
					} catch (BindException e) {
						state.set(State.Init);
						
						int i = 0;
						while (i++ < Config.getSmppRetryCount()) {
							try {
								Thread.sleep(Config.getSmppRetryWait());
							} catch (InterruptedException e1) {
							}
							try {
								smscListener.start();
								state.set(State.Runnable);
								return;
							} catch (BindException e1) {
								log.warn(e1.getMessage() + " - Retry: " + i);
							}
						}
						log.error(e.getMessage());
						throw e;
					}
				} catch (IOException e) {
					log.error("Unable to start SMPP listener", e);
					state.set(State.Error);
					throw e;
				}  finally {
					lock.unLock();
				}
			}
		}
    	else{
    		
			try {
				smscListener.start();
				state.set(State.Runnable);
			} catch (BindException e) {
				state.set(State.Init);
				log.warn(e.getMessage());
				return;
			}
			catch (IOException e) {
				log.error("Unable to start SMPP listener", e);
				state.set(State.Error);
				throw e;
			} 
    	}
    }
    
    
	private void init(){
		smscListener = new SMSCListener(Config.getSMPPListenPort(), true);
		
		processors = new PDUProcessorGroup();
        messageStore = new SmppMessageStore();
        deliveryInfoSender = new DeliveryInfoSender();
        deliveryInfoSender.start();
        
        //authentication is disabled        
        factory = new SimulatorPDUProcessorFactory(processors, messageStore, deliveryInfoSender, null);
        smscListener.setPDUProcessorFactory(factory);
        
        try {
			socketListen();
		} catch (IOException e) {
			log.info("Listener states will be cleared since it is not listening");
		}
        if(state.get() != State.Runnable){
			disconnect();
		}
	}
	
	private SmppListener(){
		//configuration etc
	}
	
	private static SmppListener _singleton = null;
	public static SmppListener instance(){
		
		if(_singleton == null){
			synchronized (SmppListener.class) {
				if(_singleton == null)
					_singleton = new SmppListener();
			}
			
		}
		return _singleton;
	}
	
	public void checkState(){
		init();
		switch(state.get()){
		case Runnable:
			state.set(State.Running);
			log.info("Running Smpp connector");
			break;
			
		case Running:
			log.info("This instance is already running a listener");
			break;
			
		case Init:
			log.info("Some other instance is running a listener");
			break;
			
		default:
			log.fatal("There seems to be some error in starting listener. Check logs");
			break;	
			
		}
	}
	private boolean retry = false;

	private boolean onInit = true;
	@Override
	public void startListening(boolean retry) {
		this.retry = retry;
		Thread t = new Thread(this, "datacollector.smpp.bootstrap");
		t.start();
		
	}
	
	private void disconnect(){
		if (smscListener != null) {
            
            synchronized (processors) {
                int procCount = processors.count();
                SimulatorPDUProcessor proc;
                SMSCSession session;
                for(int i=0; i<procCount; i++) {
                    proc = (SimulatorPDUProcessor)processors.get(i);
                    session = proc.getSession();
                    
                    session.stop();
                    
                }
            }
            try {
				smscListener.stop();
			} catch (Throwable e) {
				//catching all since a null pointer will be thrown
				//in a clustered environment. this is because the ServerSocket
				//is wrapped over a library class and it remains null on bind exception
				//can't help, this is how Logica has implemented their smpp library :)
			}
            smscListener = null;
            if (deliveryInfoSender!=null) {
                deliveryInfoSender.stop();
                deliveryInfoSender = null;
            }
		}
		processors = null;
		messageStore = null;
	}

	@Override
	public void stopListening() {
		log.debug("Stopping smpp server");
		stopSignalReceived.set(true);
		disconnect();
		if(stopSignalReceived.compareAndSet(true, false)){
			Main._instanceLatchNotify();
			
		}
		log.info("Stopped smpp listener");
	}

	

	@Override
	public void run() {
		checkState();
		
		if(onInit ){
			Main._instanceLatchNotify();
			onInit = false;
		}
	}
	@Override
	public boolean isListening() {
		
		return state.get() == State.Running;
	}
	

}
