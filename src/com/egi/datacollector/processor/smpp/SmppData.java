package com.egi.datacollector.processor.smpp;

import com.egi.datacollector.processor.Data;

public class SmppData implements Data {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7070308608498855809L;

	private byte [] bytes;
	
	private final String source;
	public String getSource() {
		return source;
	}

	public String getDestination() {
		return destination;
	}

	public String getMessage() {
		return message;
	}

	private final String destination;
	private final String message;
	private String smscGenId;
	
	public String getSmscGenId() {
		return smscGenId;
	}

	public void setSmscGenId(String smscGenId) {
		this.smscGenId = smscGenId;
	}

	public SmppData(String sourceAddress, 
					String destnAddress, 
					String messageContent
					){
		
		source = sourceAddress;
		destination = destnAddress;
		message = messageContent;
		
	}

	@Override
	public String type() {
		return "smpp";
	}

	public byte [] getBytes() {
		return bytes;
	}

	public String getSmscId() {
		return smscGenId;
	}
		

}
