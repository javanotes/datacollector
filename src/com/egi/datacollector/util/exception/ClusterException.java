package com.egi.datacollector.util.exception;

public class ClusterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1734465670745943228L;
	
	public ClusterException()
    {
		super();
    }

    public ClusterException(String message)
    {
        super(message);
    }

    public ClusterException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ClusterException(Throwable cause)
    {
        super(cause);
    }

}
