package com.github.quintans.jdbc.exceptions;

public class PersistenceIntegrityConstraintException extends PersistenceException {
    private static final long serialVersionUID = 1L;

    public PersistenceIntegrityConstraintException(String msg, Throwable t) {
        super(msg, t);
    }

}
