package com.egi.datacollector.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Config {

	private Config(){}
	
	public static String getDirectoryToWatch(){
		return getProperty("datacollector.listener.file.watchdir");
		
	}
	
	public static long getClusterLockFileTimeDelta(){
		return Long.parseLong(getProperty("datacollector.listener.cluster.lock.deltatime"));
	}
	
	public static String getPrimaryInstanceFile(){
		return getProperty("datacollector.instance.primary");
	}
	public static String getFileWatcherLockFile(){
		return getProperty("datacollector.listener.file.lock");
	}
	
	public static String getClusterLockFile(){
		return getProperty("datacollector.listener.cluster.lock");
	}
	
	private static int getOptionPos(String flag, String[] options) {
	    if (options == null)
	      return -1;
	    
	    for (int i = 0; i < options.length; i++) {
	      if ((options[i].length() > 0) && (options[i].charAt(0) == '-')) {
		// Check if it is a negative number
		try {
		  Double.valueOf(options[i]);
		} 
		catch (NumberFormatException e) {
		  // found?
		  if (options[i].equals("-" + flag))
		    return i;
		  // did we reach "--"?
		  if (options[i].charAt(1) == '-')
		    return -1;
		}
	      }
	    }
	    
	    return -1;
	 }
	
	/**
	   * Gets an option indicated by a flag "-String" from the given array
	   * of strings. Stops searching at the first marker "--". Replaces 
	   * flag and option with empty strings.
	   *
	   * @param flag the String indicating the option.
	   * @param options the array of strings containing all the options.
	   * @return the indicated option or an empty string
	   * @exception Exception if the option indicated by the flag can't be found
	   */
	public static String getOption(String flag, String[] options) 
		     {

		    String newString;
		    int i = getOptionPos(flag, options);

		    if (i > -1) {
		      if (options[i].equals("-" + flag)) {
			if (i + 1 == options.length) {
				return "";
			}
			options[i] = "";
			newString = new String(options[i + 1]);
			options[i + 1] = "";
			return newString;
		      }
		      if (options[i].charAt(1) == '-') {
			return "";
		      }
		    }
		    
		    return "";
	}

	private static Properties props = null;
	public static void init(String configFile) throws IOException{
		props = new Properties();
		
		try {
			props.load(new FileReader(configFile));
			System.out.println("Config loaded ..");
		} catch (IOException e) {
			throw e;
		}
	}
	
	public static String getProperty(String key){
		return getProperty(key, "");
	}
	public static String getProperty(String key, String defaultValue){
		String value = props.getProperty(key, defaultValue);
		return Utilities.isNullOrBlank(value) ? defaultValue : value;
	}
	public static Properties getConfiguration(){
		return props;
		
	}

	public static String getLog4jLocation() {
		
		return getProperty("datacollector.log4j.config.path");
	}
	public static String getHazelcastConfig() {
		
		return getProperty("datacollector.hazelcast.config.path");
	}

	public static int getSmppMaxConnSize(){
		try {
			return Integer.parseInt(getProperty("datacollector.listener.smpp.sessionthreads"));
		} catch (NumberFormatException e) {
			
		}
		return 1;
	}
	public static int getSMPPListenPort() {
		
		try {
			return Integer.parseInt(getProperty("datacollector.listener.smpp.port"));
		} catch (NumberFormatException e) {
			return 25;
		}
	}
	
	public static String getSmppServerHost() {
		
		return getProperty("datacollector.listener.smpp.host", "localhost");
	}

	public static String getRedisServerHost() {
		
		return getProperty("datacollector.loader.redis.host", "localhost");
	}

	public static int getRedisServerPort() {
		try {
			return Integer.parseInt(getProperty("datacollector.loader.redis.port"));
		} catch (NumberFormatException e) {
			
		}
		return 6379;
	}

	/*public static int getRedisSocketSoTimeOutSecs(){
		//
		try {
			return Integer.parseInt(getProperty("datacollector.loader.redis.socket.so_timeout"));
		} catch (NumberFormatException e) {
			
		}
		return 5;
	}*/
	public static int getRedisPoolExpirySecs() {
		try {
			return Integer.parseInt(getProperty("datacollector.loader.redis.pool.expiry_task_run.sec"));
		} catch (NumberFormatException e) {
			
		}
		return 20;
	}

	public static int getRedisPoolSize() {
		try {
			return Integer.parseInt(getProperty("datacollector.loader.redis.pool.size"));
		} catch (NumberFormatException e) {
			
		}
		return 10;
	}

	public static int getRedisPoolWaitSecs() {
		try {
			return Integer.parseInt(getProperty("datacollector.loader.redis.pool.wait.sec"));
		} catch (NumberFormatException e) {
			
		}
		return 2;
	}

	public static String getFtpServerHost(){
		return getProperty("datacollector.listener.ftp.host");
	}
	public static String getFtpServerUser(){
		return getProperty("datacollector.listener.ftp.user");
	}
	public static String getFtpServerPassword(){
		return getProperty("datacollector.listener.ftp.password");
	}
	public static String getFtpLocalDir(){
		return getProperty("datacollector.listener.ftp.localdir");
	}
	public static String getFtpRemoteDir(){
		return getProperty("datacollector.listener.ftp.remotedir");
	}
	public static boolean isFtpDownloadOnInit(){
		return "true".equals(getProperty("datacollector.listener.ftp.init.download"));
	}
	public static int getFtpServerPort() {
		try {
			return Integer.parseInt(getProperty("datacollector.listener.ftp.port"));
			
		} catch (NumberFormatException e) {
			
		}
		return -1;
	}
	public static long getFtpPollInterval() {
		try {
			long hrs = Long.parseLong(getProperty("datacollector.listener.ftp.poll.hours"));
			return hrs * 60 * 60 * 1000;
		} catch (NumberFormatException e) {
			
		}
		return 24*60*60;
	}
	
	public static boolean isFtpRemoveRemoteOnDownload(){
		return "true".equalsIgnoreCase(getProperty("datacollector.listener.ftp.remotefile.delete"));
				
	}

	public static int getFtpDownloadThreads() {
		try {
			return Integer.parseInt(getProperty("datacollector.listener.ftp.download.threads"));
			
		} catch (NumberFormatException e) {
			
		}
		return Runtime.getRuntime().availableProcessors();
	}

	public static boolean useMemoryMappedIO() {
		return "true".equalsIgnoreCase(getProperty("datacollector.processor.file.use.mem_map_io"));
	}

	public static long getRedisTimeToIdle() {
		try {
			return Long.parseLong(getProperty("datacollector.loader.redis.pool.time_to_idle.ms"));
			
		} catch (NumberFormatException e) {
			
		}
		return 0;
	}

	public static long getRedisTimeToLive() {
		try {
			return Long.parseLong(getProperty("datacollector.loader.redis.pool.time_to_live.ms"));
			
		} catch (NumberFormatException e) {
			
		}
		return 0;
	}

	public static int getSmppRetryCount() {
		try {
			return Integer.parseInt(getProperty("datacollector.listener.smpp.retry"));
			
		} catch (NumberFormatException e) {
			
		}
		return 0;
	}
	
	public static boolean isClusteredModeEnabled(){
		return "true".equals(getProperty("datacollector.hazelcast.mode.cluster"));
	}

	public static long getSmppRetryWait() {
		try {
			return Long.parseLong(getProperty("datacollector.listener.smpp.retry.wait_sec")) * 1000;
			
		} catch (NumberFormatException e) {
			
		}
		return 0;
	}

	public static String sshPassword() {
		return getProperty("datacollector.ssh.password");
	}

	public static String sshUser() {
		return getProperty("datacollector.ssh.user");
	}
	
	public static String sshCmdIsInstanceRunning(){
		return getProperty("datacollector.ssh.cmd.isrunning");
	}

	public static String sshHost() {
		// TODO Auto-generated method stub
		return null;
	}

	public static boolean useMapReduceFunction() {
		return "true".equals(getProperty("datacollector.processor.file.use.mapreduce"));
	}

	public static int getForkJoinParallelism() {
		// TODO Auto-generated method stub
		return 8;
	}

	public static String sshCmdStartInstance() {
		return getProperty("datacollector.ssh.cmd.start");
	}

	public static int getNoOfProcessorActors() {
		try {
			return Integer.parseInt(getProperty("datacollector.processor.actors"));
		} catch (NumberFormatException e) {
			
		}
		return 0;
	}
}
