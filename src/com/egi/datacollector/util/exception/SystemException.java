package com.egi.datacollector.util.exception;

public class SystemException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5302061994602384584L;
	
	public SystemException()
    {
		super();
    }

    public SystemException(String message)
    {
        super(message);
    }

    public SystemException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SystemException(Throwable cause)
    {
        super(cause);
    }

}
