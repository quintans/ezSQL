package com.github.quintans.ezSQL.sp;

/**
 * Definition of a Database Stored Procedure
 *
 * @author paulo.quintans
 */
public interface SqlProcedureDSL {

  String getName();

  int getParametersSize();

  boolean isFunction();
}
