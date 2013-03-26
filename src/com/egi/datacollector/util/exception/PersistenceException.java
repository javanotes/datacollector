package com.egi.datacollector.util.exception;

public class PersistenceException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6280317916916951296L;
	
	public PersistenceException()
    {
		super();
    }

    public PersistenceException(String message)
    {
        super(message);
    }

    public PersistenceException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public PersistenceException(Throwable cause)
    {
        super(cause);
    }

}
