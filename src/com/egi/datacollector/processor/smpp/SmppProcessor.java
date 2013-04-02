package com.egi.datacollector.processor.smpp;

import org.apache.log4j.Logger;

import com.egi.datacollector.processor.Data;
import com.egi.datacollector.processor.Processor;
import com.egi.datacollector.util.exception.ProcessorException;
import com.logica.smpp.pdu.PDUException;

/**
 * Service class for processing a smpp request
 * @author esutdal
 *
 */
public class SmppProcessor extends Processor {
	private static final Logger log = Logger.getLogger(SmppProcessor.class);
	
		
	/**
	 * Note:
	 * This class will be invoked in a multi-threaded manner. So avoid any instance variable.
	 * Use it as a service class as much as possible
	 */
	/*
	 * (non-Javadoc)
	 * @see com.egi.datacollector.processor.Processor#process(com.egi.datacollector.processor.Data)
	 */

	@Override
	public boolean process(Data job) throws ProcessorException {
		
		if(job instanceof SmppData){
			SmppData smppPdu = (SmppData) job;
			
			try {
				
				if(smppPdu.getMessage() != null){
					
					//now process it to database
					log.info("Short message text: " + smppPdu.getMessage());
					return true;
				}
				else{
					throw new PDUException("SmppProcessor: Does not seem to be a valid SUBMIT_SM smpp pdu");
				}
				
			} catch (PDUException e) {
				log.error("Invalid smpp PDU", e);
				throw new ProcessorException(e);
			}
		}
		return false;
	}
	
}
