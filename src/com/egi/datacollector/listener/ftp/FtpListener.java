package com.egi.datacollector.listener.ftp;


import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.egi.datacollector.listener.Listener;
import com.egi.datacollector.listener.ListenerState.State;
import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.listener.cluster.ClusterLock;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.processor.ProcessorFactory;
import com.egi.datacollector.processor.file.data.FileData;
import com.egi.datacollector.server.Main;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.exception.ProcessorException;
import com.enterprisedt.net.ftp.FTPConnectMode;
import com.enterprisedt.net.ftp.FTPException;
import com.enterprisedt.net.ftp.FTPFile;
import com.enterprisedt.net.ftp.FTPTransferType;
import com.enterprisedt.net.ftp.FileTransferClient;
import com.enterprisedt.net.ftp.WriteMode;

/**
 * 
 * @author esutdal
 *
 */
public class FtpListener extends Listener implements Runnable  {

	private static final Logger log = Logger.getLogger(FtpListener.class);
	private static FtpListener _instance = null;
	private FileTransferClient connection = null;
	
	private boolean onInit = true;
	
	private FtpListener(){
		state.set(State.Init);
	}
	public static FtpListener instance(){
		if(_instance == null){
			synchronized(FtpListener.class){
				if(_instance == null){
					_instance = new FtpListener();
				}
			}
		}
		return _instance;
		
	}
	private void connect(){
		
		log.info("Trying to connect to Ftp server");
		disconnect();
		if (connection == null) {
			
			connection = new FileTransferClient();
			try {
				connection.setRemoteHost(Config.getFtpServerHost());
				if (Config.getFtpServerPort() != -1)
					connection.setRemotePort(Config.getFtpServerPort());
				connection.setUserName(Config.getFtpServerUser());
				connection.setPassword(Config.getFtpServerPassword());

			} catch (FTPException e) {
				log.error("Unable to set FTP parameters", e);
				state.set(State.Error);
				
				return;
			}
			try {
				connection.connect();
			} catch (FTPException | IOException e) {
				log.warn("FTP did not allow to login with the given credentials! Trying to login anonymously - " + e.getMessage());
				try {
					if(connection.isConnected())
						connection.disconnect();
					connection.setUserName("anonymous");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {

					}
					connection.connect();
				} catch (FTPException | IOException e2) {
					log.error("Cannot connect to FTP server", e2);
					state.set(State.Error);
					
					return;
				}
			}
			try {
				connection.getAdvancedFTPSettings().setConnectMode(FTPConnectMode.PASV);

			} catch (FTPException e) {
				log.warn("Could not set connection property - PASV");
			}
			try {
				connection.setContentType(FTPTransferType.ASCII);
			} catch (IOException | FTPException e) {
				log.warn("Could not set connection property - ASCII");
			}
			log.info("Connected to Ftp server: " + Config.getFtpServerHost());
			
			state.set(State.Runnable);
		}
			
	}
	
	private ScheduledExecutorService timer = null; 
	
	@Override
	public void startListening(boolean retry) {
		if(timer != null){
			timer.shutdown();
			try {
				timer.awaitTermination(5, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				
			}
		}
		timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "datacollector.ftp.listener");
			}
		});
		timer.scheduleWithFixedDelay(this, 0, Config.getFtpPollInterval(), TimeUnit.HOURS);
		
	}
	
	private void disconnect(){
		if(connection != null){
			try {
				connection.disconnect();
			} catch (FTPException | IOException e) {
				
			}
			connection = null;
		}
		
	}

	@Override
	public void stopListening() {
		stopSignalReceived.set(true);
		if(timer != null){
			timer.shutdown();
			try {
				timer.awaitTermination(5, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				
			}
		}
		disconnect();
		if(stopSignalReceived.compareAndSet(true, false)){
			Main._instanceLatchNotify();
			
		}
		log.info("Stopped Ftp listener");
	}

	@Override
	public void checkState() {
		log.info("checking state");
		switch(state.get()){
		
		case Void:
			break;
	
		case Runnable:
			state.set(State.Running);
			log.info("Running FTP listener");
			break;
			
		case Init:
			break;
			
		case Running:
			log.info("File transfer running");
			break;
			
		default:
			log.error("Unexpected listener State: " + state.get());
			break;
		}

	}
	private FileData job = null;
	private final ExecutorService threadPool = Executors.newFixedThreadPool(Config.getFtpDownloadThreads());
	
	@Override
	public void run() {
		
		boolean download = true;
		if(onInit){
			state.set(State.Runnable);
			Main._instanceLatchNotify();
			onInit = false;
			download = Config.isFtpDownloadOnInit();
		}
		if(download){
			connect();
			checkState();
			log.info("Trying to pull from server");
			ClusterLock lock = ClusterListener.instance().getFtpLock();
			if(lock.isLocked()){
				
				try {
					log.info("Changing to dir: " + Config.getFtpRemoteDir());
					connection.changeDirectory(Config.getFtpRemoteDir());
					log.info("Changed to dir: " + Config.getFtpRemoteDir());
					FTPFile [] files = connection.directoryList();
					if(files != null){
						List<Future<?>> results = new ArrayList<Future<?>>();
						job = new FileData();
						job.setMemMapIO(Config.useMemoryMappedIO());
						for(FTPFile file : files){
							log.info("Got a file system instance: " + file.getName());
							if(file.isFile()){
								log.info("This is a file");
								final String remoteFileName = file.getName();
								final String localFileName = Config.getFtpLocalDir() + File.separator + remoteFileName;
								 Future<?> result = threadPool.submit(new Callable<Void>() {

									@Override
									public Void call() throws Exception {
										
										log.info("Downloading remote file: " + remoteFileName + " to: "+localFileName);
										try {
											connection.downloadFile(localFileName, remoteFileName, WriteMode.OVERWRITE);
											log.info("Downloaded remote file");
											job.addFile(localFileName);
											if(Config.isFtpRemoveRemoteOnDownload()){
												connection.deleteFile(remoteFileName);
												log.info("Deleted remote file" );
											}
											
										} catch (FTPException | IOException e) {
											log.error("Unable to download file: " + remoteFileName);
											throw new Exception(e);
										}
										return null;
									}
								});
								results.add(result);
								
							}
							else{
								log.info("This is not a file!");
							}
						}
						threadPool.shutdown();
						if(!results.isEmpty()){
							for(Future<?> result : results){
								try {
									result.get();
									
								} catch (InterruptedException | ExecutionException e) {
									log.error(e);
								}
							}
						}
					}
					else{
						log.warn("No files found to download");
					}
				} catch (FTPException | IOException | ParseException e) {
					log.error("Cannot download file from Ftp server", e);
					log.info(" +++ If this exception occured while switching to primary, this can be ignored +++ ");
				}
				
			}
			lock.unLock();
			if(connection != null && connection.isConnected()){
				try {
					connection.disconnect();
				} catch (FTPException | IOException e) {
					
				}
				connection = null;
			}
			if(job != null){
				try {
					Processor processor = ProcessorFactory.getProcessor(job);
					if(processor != null){
						processor.process(job);
					}
				} catch (ProcessorException e) {
					log.error("Unable to process downloaded files", e);
				}
			}
			
		}
		else{
			log.info("Not attempting to download from Ftp server");
		}
	}
	@Override
	public boolean isListening() {
		return state.get() == State.Runnable || state.get() == State.Running;
	}

}
