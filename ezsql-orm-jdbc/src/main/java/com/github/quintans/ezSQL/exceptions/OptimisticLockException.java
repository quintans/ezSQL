package com.github.quintans.ezSQL.exceptions;

public class OptimisticLockException extends RuntimeException {
  public final static String OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed.";

  private static final long serialVersionUID = 1L;

  public OptimisticLockException() {
    super(OPTIMISTIC_LOCK_MSG);
  }

  public OptimisticLockException(String msg) {
    super(msg);
  }

  public OptimisticLockException(String msg, Throwable t) {
    super(msg, t);
  }

  public OptimisticLockException(Throwable t) {
    super(t);
  }
}
