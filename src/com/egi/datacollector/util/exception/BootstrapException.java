package com.egi.datacollector.util.exception;

public class BootstrapException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3144620326596257481L;
	
	public BootstrapException()
    {
		super();
    }

    public BootstrapException(String message)
    {
        super(message);
    }

    public BootstrapException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public BootstrapException(Throwable cause)
    {
        super(cause);
    }

}
