package com.egi.datacollector.util.exception;

public class GeneralException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5302061994602384584L;
	
	public GeneralException()
    {
		super();
    }

    public GeneralException(String message)
    {
        super(message);
    }

    public GeneralException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public GeneralException(Throwable cause)
    {
        super(cause);
    }

}
