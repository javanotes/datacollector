package com.egi.datacollector.processor.smpp;

import com.egi.datacollector.processor.Data;
import com.logica.smscsim.ShortMessageValue;

public class SmppData implements Data {
	
	private byte [] bytes;
	
	private final ShortMessageValue messageBody;
	
	public SmppData(ShortMessageValue messageBody){
		
		this.messageBody = messageBody;
	}

	@Override
	public String type() {
		return "smpp";
	}

	public byte [] getBytes() {
		return bytes;
	}

	public ShortMessageValue getMessageBody() {
		return messageBody;
	}

	

}
