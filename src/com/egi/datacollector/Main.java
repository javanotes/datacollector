package com.egi.datacollector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.egi.datacollector.listener.Listener;
import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.listener.ftp.FtpListener;
import com.egi.datacollector.listener.smpp.SmppListener;
import com.egi.datacollector.loader.keyval.RedisClient;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.concurrent.ActorFramework;
import com.egi.datacollector.util.exception.BootstrapException;

/**
 * The bootstrap class for running a single datacollector server instance in the farm<p>
 * Usage: <i>com.egi.datacollector.Main -c /path/to/config/file/property_file.properties</i>
 * @author esutdal
 *
 */
public class Main {
	
	private static final Logger log = Logger.getLogger(Main.class);
	
	private static final List<Listener> listeners = new ArrayList<Listener>();
	
	private static String processId = "";
	
	private static CountDownLatch _instanceLatch = null;
	
	public static boolean makeInstancePrimary(){
		_instanceLatch = new CountDownLatch(inactiveListeners());
		startListeners(true);
		try {
			log.info("Trying to make instance primary. Allowing for listeners to re-start");
			_instanceLatch.await(60, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			
		}
		return isAPrimaryInstance();
	}
	
	public static void _instanceLatchNotify(){
		if(_instanceLatch != null)
			_instanceLatch.countDown();
	}
	
	private static int inactiveListeners(){
		int count = 0;
		if(listeners != null && !listeners.isEmpty()){
			for(Listener listener : listeners){
				count += listener.isListening() ? 0 : 1;
					
			}
			
		}
		return count;
	}
	
	@SuppressWarnings("unused")
	private static int activeListeners(){
		int count = 0;
		if(listeners != null && !listeners.isEmpty()){
			for(Listener listener : listeners){
				count += listener.isListening() ? 1 : 0;
					
			}
			
		}
		return count;
		
	}
	
	public static boolean isAPrimaryInstance(){
		
		if(listeners != null && !listeners.isEmpty()){
			for(Listener listener : listeners){
				if(!listener.isListening()){
					
					return false;
				}
			}
			
			return true;
		}
		return false;
		
	}
		
	private static void startListeners(boolean retry){
		if(listeners != null && !listeners.isEmpty()){
			for(Listener listener : listeners){
				if(!listener.isListening())
					listener.startListening(retry);
			}
		}
	}
	
	private static String configFile = "";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
				
		startInstance(args);
		
		Runtime.getRuntime().addShutdownHook(new Thread("datacollector.shutdown.service"){
			
			@Override
			public void run(){stopInstance();}
			
		});
						
		setInstanceProperty();
		log.info("========== DataCollector instance: " + processId + " is running in " + (Config.isClusteredModeEnabled() ? "clustered " : "standalone ") + "mode ============");	
		
	}
	
	public static String getProcessId(){
		return processId;
		
	}
	
	public static void startInstance(String[] args){
		try {
			configFile = Config.getOption("c", args);
		} catch (Exception e) {
			
			e.printStackTrace();
			System.err.println("Exiting ...");
			System.exit(1);
		}
		if(configFile == null || "".equals(configFile.trim())){
			System.err.println("Configuration file needs to be specified: \n" + "com.egi.datacollector.Main -c /path/to/config/datacollector.properties \nExiting ...");
			System.exit(1);
		}
				
		try {
			Config.init(configFile);
		} catch (IOException e) {
			System.err.println("Could not load config properties! Exiting ...");
			e.printStackTrace();
			System.exit(1);
		}
				
		FileInputStream f = null;
		try {
			Properties log4j = new Properties();
			f = new FileInputStream(Config.getLog4jLocation());
			log4j.load(f);
			
			/*String logFile = ((String)log4j.get("log4j.appender.file.File")).replaceFirst("@", processId);
			log4j.put("log4j.appender.file.File", logFile);*/
			
			PropertyConfigurator.configure(log4j);
			log.info("Log4j configured ...");
			log.info("========== Starting DataCollector instance: " + processId + " ============");
		} catch (Throwable e1) {
			System.err.println("Cannot load log4j config!");
			e1.printStackTrace();
			System.exit(1);
		}
		finally{
			if(f != null){
				try {
					f.close();
				} catch (IOException e) {
					
				}
			}
		}
		
		
		log.info("Starting components ...");
		
		//Activate the cluster service first
		_instanceLatch = new CountDownLatch(1);
		//don't add it to listener list
		ClusterListener.instance().startListening(false);
		try {
			
			_instanceLatch.await(1, TimeUnit.MINUTES);
			
		} catch (InterruptedException e1) {
			
		}
		
		
		listeners.add(FtpListener.instance());
		listeners.add(SmppListener.instance());
		_instanceLatch = new CountDownLatch(listeners.size());
		startListeners(false);
		try {
			
			_instanceLatch.await(5, TimeUnit.MINUTES);
			
		} catch (InterruptedException e1) {
			
		}
		log.info("Components started ...");
	}
	
	public static void stopInstance(){

		log.warn("Shutdown signal detected");
		log.info("========== Stopping DataCollector instance: " + processId + " ============");
		_instanceLatch = new CountDownLatch(1);
		ClusterListener.instance().stopListening();
		try {
			_instanceLatch.await(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
		}
		if(listeners != null && !listeners.isEmpty()){
			
			_instanceLatch = new CountDownLatch(listeners.size());
			for(Listener listener : listeners){
				listener.stopListening();
			}
								
			try {
				
				_instanceLatch.await(120, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				
			}
		}
						
		ActorFramework.instance().stop();
		RedisClient.shutdown();
		
		try {
			
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			
		}
						
		log.info("========== Stopped DataCollector instance: " + processId + " ============");
	
	}
	
	private static void setInstanceProperty(){
		boolean primary = isAPrimaryInstance();
		System.setProperty("-Dcom.egi.etl.instance", primary ? "primary" : "secondary");
		
		if(primary){
			log.info("This is a primary instance");
			/*try {
				FileUtils.writeStringToFile(new File(Config.getPrimaryInstanceFile()), processId);
				log.info("Set primary: " + Main.getProcessId());
			} catch (IOException e) {
				log.warn("Unable to set primary - " + e.getMessage());
			}*/
		}
		else{
			log.info("This is a secondary instance");
			
		}
	}
	
	/**
	 * Changes the logger level
	 * @param logLevel
	 */
	public static String changeLoggingLevel(String logLevel){
		Level level = Level.toLevel(logLevel);
		LogManager.getRootLogger().setLevel(level);
				
		try {
			Method loggerMethod = log.getClass().getMethod(level.toString().toLowerCase(), Object.class);
			if(loggerMethod != null){
				loggerMethod.invoke(log, "* Logger level changed to: "+level+" *");
				
			}
		} catch (Throwable e) {
			
		}
		return LogManager.getRootLogger().getLevel().toString();
	}
	
	/**
	 * For starting a new instance on the same node
	 * @param touchFile
	 * @throws BootstrapException
	 */
	
	public static void startAnotherInstance(final File touchFile) throws BootstrapException {
		
		if(touchFile != null && touchFile.exists())
			touchFile.setLastModified(System.currentTimeMillis());
		
		String separator = System.getProperty("file.separator");
	    String classpath = System.getProperty("java.class.path");
	    String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
	    
	    List<String> var_args = new ArrayList<String>();
	    var_args.add(path);
	    var_args.add("-cp");
	    var_args.add(classpath);
	    
	    List<String> vmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
				
	    for(String arg : vmArgs)
	    	var_args.add(arg);
	    
	    var_args.add(Main.class.getCanonicalName());
	    var_args.add("-c");
	    var_args.add(configFile);
	    	    
	    ProcessBuilder processBuilder = new ProcessBuilder(var_args);
	    	    
		try {
			processBuilder.start();
			log.info("New jvm process invoked");
		} catch (IOException e) {
			log.error(e);
			throw new BootstrapException(e);
		}
		catch(Throwable e){
			log.error(e);
			throw new BootstrapException(e);
		}
		setInstanceProperty();
		
	}

}
