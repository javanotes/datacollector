/*
 * Copyright (c) 1996-2001
 * Logica Mobile Networks Limited
 * All rights reserved.
 *
 * This software is distributed under Logica Open Source License Version 1.0
 * ("Licence Agreement"). You shall use it and distribute only in accordance
 * with the terms of the License Agreement.
 *
 */
package com.logica.smscsim;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.egi.datacollector.util.Config;
import com.logica.smpp.Connection;
import com.logica.smpp.SmppObject;
import com.logica.smpp.TCPIPConnection;

/**
 * <u><i>
 * Note: This class has been adapted
 * </i></u><p>
 * This class accepts client connection on given port. When the connection
 * is accepted, the listener creates an instance of <code>SMSCSession</code>,
 * generates new <code>PDUProcessor</code> using object derived from
 * <code>PDUProcessorFactory</code>, passes the processor to the smsc session
 * and starts the session as a standalone thread.
 *
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.1, 26 Sep 2001
 * @see SMSCSession
 * @see PDUProcessor
 * @see PDUProcessorFactory
 */

/*
  23-08-01 ticp@logica.com added yield() to run() to give chance to other
                           threads
  26-09-01 ticp@logica.com debug now in a group
*/

public class SMSCAdaptor extends SmppObject implements Runnable
{
    private Connection serverConn = null;
    private int port;
    
    private long acceptTimeout = com.logica.smpp.Data.ACCEPT_TIMEOUT;
    private PDUProcessorFactory processorFactory = null;
    private boolean keepReceiving = true;
    private boolean isReceiving = false;
    private boolean asynchronous = false;
    
    /**
     * Construct synchronous listener listening on the given port.
     * @param port the port to listen on
     * @see #SMSCListener(int,boolean)
     */
    public SMSCAdaptor(int port)
    {
        this.port = port;
    }
    
        
    /**
     * Constructor with control if the listener starts as a separate thread.
     * If <code>asynchronous</code> is true, then the listener is started
     * as a separate thread, i.e. the creating thread can continue after
     * calling of method <code>start</code>. If it's false, then the
     * caller blocks while the listener does it's work, i.e. listening.
     * @param port the port to listen on
     * @param asynchronous if the listening will be performed as separate thread
     * @see #start()
     */
    public SMSCAdaptor(int port, boolean asynchronous)
    {
        this.port = port;
        this.asynchronous = asynchronous;
    }
    
    /**
     * Starts the listening. If the listener is asynchronous (reccomended),
     * then new thread is created which listens on the port and the
     * <code>start</code> method returns to the caller. Otherwise
     * the caller is blocked in the start method.
     * @see #stop()
     */
    public synchronized void start()
    throws IOException
    {
        debug.write("going to start SMSCListener on port "+port);
        if (!isReceiving) {
            serverConn = new TCPIPConnection(port);
            serverConn.setReceiveTimeout(getAcceptTimeout());
            serverConn.open();
            keepReceiving = true;
            if (asynchronous) {
                debug.write("starting listener in separate thread.");
                Thread serverThread = new Thread(this, "datacollector.smpp.listener");
                serverThread.start();
                debug.write("listener started in separate thread.");
            } else {
                debug.write("going to listen in the context of current thread.");
                run();
            }
        } else {
            debug.write("already receiving, not starting the listener.");
        }
    }
    
    /**
     * Signals the listener that it should stop listening and wait
     * until the listener stops. Note that based on the timeout settings
     * it can take some time befor this method is finished -- the listener
     * can be blocked on i/o operation and only after exiting i/o
     * it can detect that it should stop.
     * @see #start()
     */
    public synchronized void stop()
    throws IOException
    {
        debug.write("going to stop SMSCListener on port "+port);
        keepReceiving = false;
        if (serverConn != null) {
			serverConn.close();
			synchronized(this){
            	try {
					this.wait(60000L);
				} catch (InterruptedException e) {
					
				}
            }
		}
		threadPool.shutdown();
        try {
			threadPool.awaitTermination(1L, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			
		}
        debug.write("SMSCListener stopped on port "+port);
    }
    
    /**
     * The actual listening code which is run either from the thread
     * (for async listener) or called from <code>start</code> method
     * (for sync listener). The method can be exited by calling of method
     * <code>stop</code>.
     * @see #start()
     * @see #stop()
     */
    public void run()
    {
        debug.enter(this, "run of SMSCListener on port "+port);
        isReceiving = true;
        try {
            while (keepReceiving) {
                listen();
                //Thread.yield();
            }
        } finally {
            isReceiving = false;
            synchronized(this){
            	this.notify();
            }
        }
        debug.exit(this);
    }
    
    private final ExecutorService threadPool = /*new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), 
    																	Config.getSmppMaxConnSize(), 
    																	60, 
    																	TimeUnit.SECONDS, 
    																	new ArrayBlockingQueue<Runnable>(Config.getSmppMaxConnSize()), 
    																	new ThreadFactory() {
    																		
    																    	private AtomicInteger n = new AtomicInteger();
    																		@Override
    																		public Thread newThread(Runnable r) {
    																			
    																			return new Thread(r, "datacollector.smpp.request-"+n.getAndIncrement());
    																		}
    																	}, 
    																	new RejectedExecutionHandler(){

																			@Override
																			public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
																				
																				System.err.println("SMSCAdaptor::Request rejected");
																			}
    																		
    																	});*/
    
											    Config.getSmppMaxConnSize() == 0 ? Executors.newCachedThreadPool(new ThreadFactory() {
													
											    	private AtomicInteger n = new AtomicInteger();
													@Override
													public Thread newThread(Runnable r) {
														
														return new Thread(r, "datacollector.smpp.request-"+n.getAndIncrement());
													}
												})
												
												:
													Executors.newFixedThreadPool(Config.getSmppMaxConnSize(), new ThreadFactory() {
														
												    	private AtomicInteger n = new AtomicInteger();
														@Override
														public Thread newThread(Runnable r) {
															
															return new Thread(r, "datacollector.smpp.request-"+n.getAndIncrement());
														}
													});
    
    /**
     * The "one" listen attempt called from <code>run</code> method.
     * The listening is atomicised to allow contoled stopping of the listening.
     * The length of the single listen attempt
     * is defined by <code>acceptTimeout</code>.
     * If a connection is accepted, then new session is created on this
     * connection, new PDU processor is generated using PDU processor factory
     * and the new session is started in separate thread.
     * @see #run()
     * @see com.logica.smpp.Connection
     * @see SMSCSession
     * @see PDUProcessor
     * @see PDUProcessorFactory
     */
    private void listen()
    {
        debug.enter(Simulator.DSIMD2, this, "SMSCListener listening on port "+port);
        try {
            Connection connection = null;
            serverConn.setReceiveTimeout(getAcceptTimeout());
            connection = serverConn.accept();

            if (connection != null) {
                debug.write("SMSCListener accepted a connection on port "+port);
                SMSCSession session = new SMSCSession(connection);
                PDUProcessor pduProcessor = null;
                if (processorFactory != null) {
                    pduProcessor = processorFactory.createPDUProcessor(session);
                }
                session.setPDUProcessor(pduProcessor);
                //Thread thread = new Thread(session);
                //thread.start();
                threadPool.submit(session);
                debug.write("SMSCListener launched a session on the accepted connection.");
            } else {
                debug.write(Simulator.DSIMD2, "no connection accepted this time.");
            }
        } catch (InterruptedIOException e) {
            // thrown when the timeout expires => it's ok, we just didn't
            // receive anything
            debug.write("InterruptedIOException accepting, timeout? -> " + e);
        } catch (IOException e) {
            // accept can throw this from various reasons
            // and we don't want to continue then (?)
            event.write(e,"IOException accepting connection");
            keepReceiving = false;
        }
        debug.exit(Simulator.DSIMD2, this);
    }
    
    /**
     * Sets a PDU processor factory to use for generating PDU processors.
     * @param processorFactory the new PDU processor factory
     */
    public void setPDUProcessorFactory(PDUProcessorFactory processorFactory)
    {
        this.processorFactory = processorFactory;
    }

    /**
     * Sets new timeout for accepting new connection.
     * The listening blocks the for maximum this time, then it
     * exits regardless the connection was acctepted or not.
     * @param value the new value for accept timeout
     */
    public void setAcceptTimeout(int value) { acceptTimeout = value; }
    
    /**
     * Returns the current setting of accept timeout.
     * @return the current accept timeout
     * @see #setAcceptTimeout(int)
     */
    public long getAcceptTimeout() { return acceptTimeout; }
	    
}
