package com.github.quintans.ezSQL.exceptions;

public class PersistenceException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public PersistenceException(String msg) {
		super(msg);		
	}

	public PersistenceException(String msg, Throwable t) {
		super(msg, t);
	}

	public PersistenceException(Throwable t) {
		super(t);		
	}

}
