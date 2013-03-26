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
import com.egi.datacollector.util.exception.GeneralException;

/**
 * A wrapper to execute remote scripts via ssh. Try to re-use a single instance as much as possible
 * @author esutdal
 *
 */
public class SshExecution {
	
	private static final Logger log = Logger.getLogger(SshExecution.class);
	
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
	BufferedReader responseReader = null;
			
	private final Object sent = new Object();
	private final Object received = new Object();
	private StringBuilder out = null;
	
	private Runnable reader = new Runnable(){
		@Override
		public void run(){
			while(connected){
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
					
					synchronized (received) {
						received.notify();
					}
				}
				
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
			try {
				sshSession.execCommand(unixCmd);
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
				return (out != null ? out.toString() : "NO_RESP_RECVD");
			} catch (IOException e) {
				log.warn("SSH: Could not execute command ["+unixCmd+"] - " + e.getMessage());
				
			}
		}
		return "NO_RESP_RECVD";
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
		synchronized (sent) {
			sent.notify();
		}
	}
	
	private void init(String host, String user, String password) throws GeneralException{
		if (sshConnection == null || !connected || !sshConnection.isAuthenticationComplete()) {
			end();
			sshConnection = new Connection(host);
			
			try {
				sshConnection.connect();
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
						throw new GeneralException("SSH: Could not establish session");
					}
				}
				else{
					throw new GeneralException("SSH: Could not authenticate");
				}
			} catch (IOException e) {
				synchronized (this) {
					connected = false;
				}
				throw new GeneralException(e);
			}
			finally{
				if(!connected){
					end();
				}
			}
		}
		
	}
	
	Runnable runReader(){
		return reader;
		
	}
	
	/**
	 * 
	 * @param hostServer
	 * @param sshUser
	 * @param password
	 * @throws GeneralException
	 */
	public SshExecution(String hostServer, String sshUser, String password) throws GeneralException{
		init(hostServer, sshUser, password);
		threads.execute(reader);
	}
		
	/**
	 * -h [host] 
	 * -u [username] 
	 * -p [password]
	 * @param strings
	 * @throws GeneralException 
	 */
	public SshExecution(String...options) throws GeneralException{
		String host = Config.getOption("h", options);
		String user = Config.getOption("u", options);
		String password = Config.getOption("p", options);
		init(host, user, password);
		threads.execute(reader);
	}
	
	public static void main(String...s){
		SshExecution ssh = null;
		try {
			ssh = new SshExecution("-h","169.144.107.94","-u","root","-p","red32hat");
			System.out.println("response: "+ssh.runCommand("ps -eaf|grep -i 'java'|wc -l"));
		} catch (GeneralException e) {
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
