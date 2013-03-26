package com.egi.datacollector.util.exception;

public class TransformerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5632068174343244496L;
	
	public TransformerException()
    {
		super();
    }

    public TransformerException(String message)
    {
        super(message);
    }

    public TransformerException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public TransformerException(Throwable cause)
    {
        super(cause);
    }

}
