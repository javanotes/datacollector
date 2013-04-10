package com.egi.datacollector.listener.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.egi.datacollector.listener.Listener;
import com.egi.datacollector.listener.ListenerState.State;
import com.egi.datacollector.server.Main;
import com.egi.datacollector.util.Config;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class HttpListener extends Listener implements Runnable{

	private static HttpListener _singleton = null;
	private HttpServer server = null;
	
	private ExecutorService threadPool = null;
	
	private static final Logger log = Logger.getLogger(HttpListener.class);
	
	private class RequestHandler implements HttpHandler{

		@Override
		public void handle(HttpExchange http) throws IOException {
			log.info("New http request: " + http.getRequestURI());
			
			Headers reqHeaders = http.getRequestHeaders();
		    Set<Map.Entry<String, List<String>>> entries = reqHeaders.entrySet();
		    StringBuffer headers = new StringBuffer();
		    for (Map.Entry<String, List<String>> entry : entries)
		      headers.append(entry.toString() + "\n");
		    
		    Headers respHeaders = http.getResponseHeaders();
		    respHeaders.set("Content-Type", "text/xml");
			http.sendResponseHeaders(200, 0);
			
			OutputStream out = http.getResponseBody();
			
			String response = Main.getStatusXml();
			out.write(response.getBytes());
			out.close();
			
		}
		
	}
	
		
	private HttpListener(){
		
	}
	
	public static HttpListener instance(){
		
		if(_singleton == null){
			synchronized (HttpListener.class) {
				if(_singleton == null)
					_singleton = new HttpListener();
			}
			
		}
		return _singleton;
	}

	@Override
	public void run() {
		init();
		
	}

	private void init(){
		try {
			server = HttpServer.create();
			server.bind(new InetSocketAddress(Config.getHTTPListenPort()), Config.getHTTPMaxConn());
			server.createContext("/datacollector", new RequestHandler());
			
			threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
				private int n=0;
				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "datacollector.http.handler-"+n++);
				}
			});
			server.setExecutor(threadPool);
			server.start();
			log.info("Started HTTP transport on port " + Config.getHTTPListenPort());
			state.set(State.Running);
		} catch (IOException e) {
			log.error("Unable to start HTTP transport", e);
			state.set(State.Error);
		}
		finally{
			Main._instanceLatchNotify();
		}
	}
	@Override
	public boolean isListening() {
		
		return state.get() == State.Running;
	}

	@Override
	public void startListening(boolean retry) {
		Thread t = new Thread(this, "datacollector.http.listener");
		t.start();
		
	}

	@Override
	public void stopListening() {
		log.info("Stopping HTTP transport");
		if(server != null){
			server.stop(0);
			
		}
		if(threadPool != null){
			threadPool.shutdown();
			try {
				threadPool.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				
			}
		}
		log.info("Stopped HTTP transport");
		Main._instanceLatchNotify();
	}

	@Override
	public void checkState() {
		// TODO Auto-generated method stub
		
	}

}
