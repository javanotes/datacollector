package com.egi.datacollector.processor.file;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.egi.datacollector.listener.cluster.ClusterListener;
import com.egi.datacollector.processor.Data;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.util.Config;
import com.egi.datacollector.util.Utilities;
import com.egi.datacollector.util.actors.ActorFramework;
import com.egi.datacollector.util.exception.ProcessorException;

public class FileProcessor extends Processor {
	
	private static final Logger log = Logger.getLogger(FileProcessor.class);

	private final StringBuilder recordBuffer = new StringBuilder();
	static int BLOCK_SIZE = 8192;
	
	private void clear(){
		recordBuffer.delete(0, recordBuffer.length());
		recordLength = 0;
	}
	
	private int recordLength = 0;
	
	/**
	 * TODO
	 * @param block
	 */
	private void processMapReduce(byte[] block){
		
		for(byte b : block) {
			char nextChar = (char)b;
			switch (nextChar) {
			case '\r':
				
				ClusterListener.instance().addToMapReduceJobsMap(new RecordData(recordBuffer.toString()));
				recordBuffer.delete(0, recordLength);
				recordLength = 0;
				break;
			case '\n':
				ClusterListener.instance().addToMapReduceJobsMap(new RecordData(recordBuffer.toString()));
				recordBuffer.delete(0, recordLength);
				recordLength = 0;
				break;
			default:
				recordBuffer.append(nextChar);
				recordLength++;
				break;
			}
		}
	}
	
	private void processParallel(byte[] block){
		
		for(byte b : block) {
			char nextChar = (char)b;
			switch (nextChar) {
			case '\r':
				
				ActorFramework.instance().submitFileRecordToProcess(new RecordData(recordBuffer.toString()));
				recordBuffer.delete(0, recordLength);
				recordLength = 0;
				break;
			case '\n':
				ActorFramework.instance().submitFileRecordToProcess(new RecordData(recordBuffer.toString()));
				recordBuffer.delete(0, recordLength);
				recordLength = 0;
				break;
			default:
				recordBuffer.append(nextChar);
				recordLength++;
				break;
			}
		}
	}
	
	
	private void usingMappedIO(String fileName) throws ProcessorException{
		/*
		 * TODO consider implementing the reading and writing both using multi-threaded
		 */
		if(Utilities.isNullOrBlank(fileName)){
			throw new ProcessorException("File name is null");
		}
		log.info("Starting transforming file: " + fileName);
		FileChannel channel = null;
		MappedByteBuffer byteBuffer = null;
		long start;
		
			
			start = System.currentTimeMillis();
			try {
				channel = new FileInputStream(fileName).getChannel();
				byteBuffer = channel.map(MapMode.READ_ONLY, 0L, channel.size());
			} catch (IOException e) {
				log.error(e.getMessage(), e);
				throw new ProcessorException(e);
			}
			
			if (byteBuffer != null) {
				
				final BlockingQueue<byte[]> stream = new LinkedBlockingQueue<>();
				Thread _localThread = new Thread("datacollector.file.reader") {

					public void run() {
						while (true) {
							byte[] nextDataBlock = null;
							try {
								nextDataBlock = stream.poll(1, TimeUnit.SECONDS);
							} catch (InterruptedException e) {

							}
							if (nextDataBlock == null) {
								break;
							} else {
								if (Config.useMapReduceFunction())
									processMapReduce(nextDataBlock);
								else
									processParallel(nextDataBlock);
							}
						}
					}
				};
				_localThread.setDaemon(true);
				_localThread.start();
				
				byte[] nextBlock = null;
				
				while (byteBuffer.hasRemaining()) {
					if (byteBuffer.remaining() < BLOCK_SIZE)
						nextBlock = new byte[byteBuffer.remaining()];
					else
						nextBlock = new byte[BLOCK_SIZE];
					byteBuffer.get(nextBlock);

					try {
						stream.put(nextBlock);
					} catch (InterruptedException e) {
						log.warn(e.getMessage());
					}

				}
				
				if (channel != null) {
					try {
						channel.close();
					} catch (IOException e) {
						log.warn(e.getMessage());
					}
				}
				
				byteBuffer.clear();
				byteBuffer = null;
				System.gc();
				
				try {
					_localThread.join();
				} catch (InterruptedException e) {
					log.warn(e.getMessage());
				}
				
				log.debug("Time taken: " + (System.currentTimeMillis() - start));
			}
		
		
	}
	
	private void usingBuffReader(String fileName) throws ProcessorException{
		
		if(Utilities.isNullOrBlank(fileName)){
			throw new ProcessorException("File name is null");
		}
		BufferedReader buffer = null;
		
		log.info("Starting transforming file: " + fileName);
		try {
			long start = System.currentTimeMillis();
			
			buffer = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			RecordData record = null;
			
			String nextLine = "";
			while((nextLine = buffer.readLine()) != null){
				record = new RecordData(nextLine);
				if(Config.useMapReduceFunction()){
					//do map reduce
				}
				else{
					ActorFramework.instance().submitFileRecordToProcess(record);
				}
				
			}
			log.debug("Time taken: " + (System.currentTimeMillis() - start));
		} catch (FileNotFoundException e) {
			log.error(e.getMessage(), e);
			throw new ProcessorException(e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw new ProcessorException(e);
		}
		finally{
			if(buffer != null){
				try {
					buffer.close();
				} catch (IOException e) {
					log.warn(e.getMessage());
				}
				
			}
		}
		
		
	}

	@Override
	public boolean process(Data job) throws ProcessorException {
		
		if (job instanceof FileData) {
			FileData fileJob = (FileData) job;
			if (fileJob.isMemMapIO()) {
				for(String file : fileJob.getFiles()){
					clear();
					usingMappedIO(file);
				}
			} else {
				for(String file : fileJob.getFiles()){
					//if(file.contains("HelloWorld123.txt"))
					usingBuffReader(file);
				}
				
			}
		}
		else{
			throw new ProcessorException("Invalid job type: "+job.getClass().getName());
		}
		return true;
	}

}
