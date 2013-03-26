package com.egi.datacollector.listener.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

import org.apache.log4j.Logger;

import com.egi.datacollector.Main;
import com.egi.datacollector.listener.Listener;
import com.egi.datacollector.listener.ListenerState.State;
import com.egi.datacollector.util.Config;

@Deprecated
public class FileListener extends Listener implements Runnable{
	
	private static final Logger log = Logger.getLogger(FileListener.class);
	
	private WatchService watchDog = null; 
	private File watchDirFile = null;;
	private Path watchDirPath = null;
	
	private WatchKey registration = null;
	//private boolean stop = false;
		
	private FileListener(){
				
	}
	
	private static FileListener _singleton = null;
	public static FileListener instance(){
		
		if(_singleton == null){
			
			synchronized (FileListener.class) {
				if(_singleton == null)
					_singleton = new FileListener();
			}
			
		}
		return _singleton;
	}
	
	@Override
	public void run(){
		WatchKey event = null;
		
		checkState();
		
		if(state.get() == State.Runnable){
			state.set(State.Running);
			log.info("Running file watcher");
			Main._instanceLatchNotify();
		}
		
		while(state.get() == State.Running){
			try {
				log.info("Listening for new files ..");
				event = watchDog.take();
			} catch (InterruptedException e) {
				
			}catch (ClosedWatchServiceException e) {
				
				state.set(State.Init);
			}
			if(event != null && event.isValid()){
				List<WatchEvent<?>> events = event.pollEvents();
				//clusteredInstance.getQueue("events-bus").offer(events);
				for(WatchEvent<?> what : events){
					System.out.println(
							   "An event was found after file creation of kind " + what.kind() 
							   + ". The event occurred on file " + what.context() + ".");
				}
				event.reset();
			}
		}
		if(event != null && event.isValid())
			event.cancel();
		if(registration != null && registration.isValid())
			registration.cancel();
		
		if(stopSignalReceived.compareAndSet(true, false)){
			Main._instanceLatchNotify();
			
		}
		
		log.info("Stopped file watcher");
	}
	
	
	@Override
	public void stopListening(){
		stopSignalReceived.set(true);
		if(watchDog != null){
			try {
				watchDog.close();
				log.debug("watchDog closed");
			} catch (IOException e) {
				log.error(e);
			}
		}
		if(lock != null){
			try {
				lock.close();
				log.debug("lock closed");
			} catch (IOException e) {
				log.warn(e);
			}
		}
		if(lockChannel != null){
			try {
				lockChannel.close();
				log.debug("lock channel closed");
			} catch (IOException e) {
				log.warn(e);
			}
		}
				
	}
	
	private static void sleep(int secs){
		try {
			Thread.sleep(secs * 1000);
		} catch (InterruptedException e) {}
	}
	
	private FileLock lock = null;
	private FileChannel lockChannel = null;
	
	private boolean canWatch(){
		boolean valid = false;
		File f = new File(Config.getFileWatcherLockFile());
		RandomAccessFile raf = null;
		
		try {
			if(!(f.exists() && f.isFile())){
				if(f.createNewFile()){
					log.debug("New lock file created");
				}
			}
			raf = new RandomAccessFile(f, "rw");
			lockChannel = raf.getChannel();
			lock = lockChannel.tryLock();
			valid = lock != null;
						
		}  catch (Throwable e) {
			log.warn(e.getMessage());
		}
		finally{
			try {
				if(!valid){
					raf.close();
					log.debug("Closed lock channel since this is not primary");
				}
			} catch (IOException e) {
				log.error(e);
			}
		}
		return valid;
		
	}
	
	private void runWatchService() throws IOException{
		if(canWatch()){
			log.info("Starting file watch");
			registration = watchDirPath.register(watchDog, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			state.set(State.Runnable);
		}else{
			//log.info("Working as secondary");
		}
		
	}
	private void init() throws IOException{
		try {
			
			watchDog = FileSystems.getDefault().newWatchService();
			watchDirFile = new File(Config.getDirectoryToWatch());
			watchDirPath = watchDirFile.toPath();
			
			state.set(State.Init);
			
			runWatchService();						
			sleep(1);
			
		} catch (IOException e) {
			log.error("Could not init FileWatcher", e);
			state.set(State.Error);
			throw e;
		}
		catch (UnsupportedOperationException e){
			log.error("Could not init FileWatcher", e);
			state.set(State.Error);
			throw new IOException(e);
		}
	}
	
	public static void main(String...strings){
		
	}
	
	public void checkState(){
		switch(state.get()){
		
		case Void:
			try {
				init();
			} catch (IOException e) {
				log.error(e);
			}
			break;
	
		case Runnable:
			state.set(State.Running);
			break;
			
		case Init:
			try {
				runWatchService();
				
			} catch (IOException e) {
				log.error(e);
			}
			break;
			
		case Running:
			log.info("Already running a listener");
			break;
			
		default:
			log.fatal("Cannot run listener! State: " + state.get());
			break;
			
	}
	}

	@Override
	public void startListening(boolean retry) {
				
		Thread t = new Thread(this, "datacollector.file.listener");
		t.start();
		
	}

	@Override
	public boolean isListening() {
		return state.get() == State.Running;
	}

}
