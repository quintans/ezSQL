package com.github.quintans.ezSQL.exception;

public class OrmException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public OrmException(String msg, Object... parameters) {
    super(format(msg, parameters));
  }

  public OrmException(Throwable t, String msg, Object... parameters) {
    super(format(msg, parameters), t);
  }

  public OrmException(Throwable t) {
    super(t);
  }

  private static String format(String template, Object... parameters) {
    if (parameters == null) {
      return template;
    }
    return String.format(template, parameters);
  }
}
