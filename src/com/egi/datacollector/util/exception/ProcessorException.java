package com.egi.datacollector.util.exception;

public class ProcessorException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7132755058383550386L;
	
	public ProcessorException()
    {
		super();
    }

    public ProcessorException(String message)
    {
        super(message);
    }

    public ProcessorException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ProcessorException(Throwable cause)
    {
        super(cause);
    }

}
