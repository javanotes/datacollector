/*
 * Copyright (c) 1996-2001
 * Logica Mobile Networks Limited
 * All rights reserved.
 *
 * This software is distributed under Logica Open Source License Version 1.0
 * ("Licence Agreement"). You shall use it and distribute only in accordance
 * with the terms of the License Agreement.
 *
 */
package com.logica.smscsim;

import java.io.Serializable;

import com.logica.smpp.pdu.SubmitSM;

/**
 * Class for storing a subset of attributes of messages to a message store.

 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.0, 21 Jun 2001
 */
public class ShortMessageValue implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7743625835696149874L;
	private String messageId;
    String systemId;
    public String getSystemId() {
		return systemId;
	}

	public void setSystemId(String systemId) {
		this.systemId = systemId;
	}

	public String getServiceType() {
		return serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}

	public String getSourceAddr() {
		return sourceAddr;
	}

	public void setSourceAddr(String sourceAddr) {
		this.sourceAddr = sourceAddr;
	}

	public String getDestinationAddr() {
		return destinationAddr;
	}

	public void setDestinationAddr(String destinationAddr) {
		this.destinationAddr = destinationAddr;
	}

	public String getShortMessage() {
		return shortMessage;
	}

	public void setShortMessage(String shortMessage) {
		this.shortMessage = shortMessage;
	}

	String serviceType;
    String sourceAddr;
    String destinationAddr;
    String shortMessage;

    /**
     * Constructor for building the object from <code>SubmitSM</code>
     * PDU.
     *
     * @param systemId system id of the client
     * @param submit the PDU send from the client
     */
    public ShortMessageValue(String systemId, SubmitSM submit)
    {
        this.systemId = systemId;
        serviceType = submit.getServiceType();
        sourceAddr = submit.getSourceAddr().getAddress();
        destinationAddr = submit.getDestAddr().getAddress();
        shortMessage = submit.getShortMessage();
    }

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}
}
