package com.egi.datacollector.util.ssh;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionMonitor;
import ch.ethz.ssh2.Session;

import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.exception.SystemException;

/**
 * A wrapper to execute remote scripts via ssh. Try to re-use a single instance as much as possible in a single thread of execution.
 * <i>Note: This class is not thread-safe.</i>
 * @author esutdal
 *
 */
public class SshExecution {
	
	private static final Logger log = Logger.getLogger(SshExecution.class);
	
	public static final String SSH_NOT_EXECUTED = "SSH_NOT_EXECUTED";
	public static final String SSH_NOT_CONNECTED = "SSH_NOT_CONNECTED";
	public static final String SSH_NOT_AUTHENTICATED = "SSH_NOT_AUTHENTICATED";
	public static final String SSH_NO_SESSION = "SSH_NO_SESSION";
	
	//private final ReentrantLock mutex = new ReentrantLock();
	//private final Condition produced = mutex.newCondition();
	//private final Condition consumed = mutex.newCondition();
	
	static{
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				threads.shutdown();
				try {
					threads.awaitTermination(1, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					//let them die down on a server shutdown
				}
			}
		});
	}
		
	private static final ExecutorService threads = Executors.newCachedThreadPool(new ThreadFactory() {
		private int n = 0;
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "datacollector.ssh.worker-"+n++);
			t.setDaemon(true);
			return t;
		}
	});
	
	public boolean isConnected(){
		return connected;
		
	}
	
	private Connection sshConnection = null;
	private Session sshSession = null;
	private BufferedReader responseReader = null;
			
	private final Object sent = new Object();
	private final Object received = new Object();
	
	private StringBuilder out = null;
	
	private Runnable reader = new Runnable(){
		@Override
		public void run(){
			while(connected){
				/*mutex.lock();
				try {
					try {
						produced.await();
					} catch (InterruptedException e1) {
						
					}*/
					try {
						synchronized (sent) {
							sent.wait();
						}
					} catch (InterruptedException e) {}
					
					if (connected) {
						String response = "";
						out = new StringBuilder();
						do {
							try {
								if (responseReader != null) {
									response = responseReader.readLine();
								} else {
									response = null;
								}
							} catch (IOException e) {
							}
							if(response != null)
								out.append(response);

						} while (response != null);
						//consumed.signal();
						synchronized (received) {
							received.notify();
						}
					}
				/*} 
				finally{
					mutex.unlock();
				}*/
			}
		}
	};
	
	private boolean connected = false;
	
	/**
	 * 
	 * @param unixCmd
	 * @param timedWait
	 * @param waitTime
	 * @return
	 */
	private String exec(String unixCmd, boolean timedWait, long waitTime){
		if(connected && sshSession != null){
			
			//mutex.lock();
			try {
				sshSession.execCommand(unixCmd);
				/*produced.signal();
				try {
					if(timedWait)
						consumed.await(waitTime, TimeUnit.MILLISECONDS);
					else
						consumed.await();
				} catch (InterruptedException e) {
					
				}*/
				synchronized (sent) {
					sent.notify();
				}
				synchronized (received) {
					try {
						if(timedWait)
							received.wait(waitTime);
						else
							received.wait();
					} catch (InterruptedException e) {
						
					}
				}
				
			} catch (IOException e) {
				log.warn("SSH: Could not execute command ["+unixCmd+"] - " + e.getMessage());
				
			}
			/*finally{
				mutex.unlock();
			}*/
			return (out != null ? out.toString() : SSH_NOT_EXECUTED);
		}
		return SSH_NOT_CONNECTED;
	}
	
	/**
	 * Blocking call. Waits 5 minutes for a response
	 * @param unixCmd
	 * @return
	 */
	public String runCommand(String unixCmd){
		return exec(unixCmd, true, 300000L);
		
	}
	
	/**
	 * Non blocking call. Sends response asynchronously
	 * @param unixCmd
	 * @param asyncResponse
	 */
	public void runCommandAsync(final String unixCmd, final AsyncResponse asyncResponse){
		threads.execute(new Runnable() {
			
			@Override
			public void run() {
				asyncResponse.responseReceived(exec(unixCmd, false, 0));
				
			}
		});
					
	}
	
	public void end(){
		if(responseReader != null){
			try {
				responseReader.close();
			} catch (IOException e) {
				
			}
			responseReader = null;
		}
		if(sshSession != null){
			sshSession.close();
			sshSession = null;
		}
		if(sshConnection != null){
			sshConnection.close();
			sshConnection = null;
		}
		/*mutex.lock();
		try{
			
			produced.signal();
		}
		finally{
			mutex.unlock();
		}*/
		synchronized (sent) {
			sent.notify();
		}
	}
	
	private void init(String host, String user, String password) throws SystemException{
		if (sshConnection == null || !connected || !sshConnection.isAuthenticationComplete()) {
			end();
			sshConnection = new Connection(host);
			
			try {
				
				sshConnection.connect(null, 15000, 0);
				synchronized (this) {
					
					connected = sshConnection.authenticateWithPassword(user, password);
				}
				if (connected) {
					
					sshConnection.addConnectionMonitor(new ConnectionMonitor() {
						
						@Override
						public void connectionLost(Throwable throwable) {
							
							synchronized (this) {
								connected = false;
							}
							end();
						}
					});
					
					sshSession = sshConnection.openSession();
					if(sshSession != null){
						responseReader = new BufferedReader(new InputStreamReader(sshSession.getStdout()));
					}
					else{
						synchronized (this) {
							connected = false;
						}
						throw new SystemException(SSH_NO_SESSION);
					}
				}
				else{
					throw new SystemException(SSH_NOT_AUTHENTICATED);
				}
			} catch (IOException e) {
				synchronized (this) {
					connected = false;
				}
				log.error(e, e);
				throw new SystemException(SSH_NOT_CONNECTED);
			}
			finally{
				if(!connected){
					end();
				}
			}
		}
		
	}
	
	/**
	 * 
	 * @param hostServer
	 * @param sshUser
	 * @param password
	 * @throws SystemException
	 */
	public SshExecution(String hostServer, String sshUser, String password) throws SystemException{
		init(hostServer, sshUser, password);
		threads.execute(reader);
	}
		
	/**
	 * -h [host] 
	 * -u [username] 
	 * -p [password]
	 * @param strings
	 * @throws SystemException 
	 */
	public SshExecution(String...options) throws SystemException{
		String host = Config.getOption("h", options);
		String user = Config.getOption("u", options);
		String password = Config.getOption("p", options);
		init(host, user, password);
		threads.execute(reader);
	}
	
	public static void main(String...s){
		SshExecution ssh = null;
		try {
			//
			//169.144.107.94
			ssh = new SshExecution("-h","169.144.107.94","-u","root","-p","red32hat");
			System.out.println("response: "+ssh.runCommand("ifconfig"));
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(ssh != null){
				ssh.end();
			}
		}
	}
	

}
